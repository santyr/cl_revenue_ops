"""
Database module for cl-revenue-ops

Handles SQLite persistence for:
- Channel flow states and history
- PID controller state (integral terms)
- Fee change history
- Rebalance history
"""

import sqlite3
import os
import time
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path


class Database:
    """
    SQLite database manager for the Revenue Operations plugin.
    
    Provides persistence for:
    - Channel states (source/sink/balanced classification)
    - Flow metrics history
    - PID controller state
    - Fee change audit log
    - Rebalance history
    """
    
    def __init__(self, db_path: str, plugin):
        """
        Initialize the database connection.
        
        Args:
            db_path: Path to SQLite database file
            plugin: Reference to the pyln Plugin for logging
        """
        self.db_path = os.path.expanduser(db_path)
        self.plugin = plugin
        self._conn: Optional[sqlite3.Connection] = None
        
    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            # Ensure directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            self._conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None  # Autocommit mode
            )
            self._conn.row_factory = sqlite3.Row
        return self._conn
    
    def initialize(self):
        """Create database tables if they don't exist."""
        conn = self._get_connection()
        
        # Channel states table - stores current classification
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_states (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                state TEXT NOT NULL,  -- 'source', 'sink', 'balanced'
                flow_ratio REAL NOT NULL,
                sats_in INTEGER NOT NULL,
                sats_out INTEGER NOT NULL,
                capacity INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        
        # Flow history table - tracks flow over time
        conn.execute("""
            CREATE TABLE IF NOT EXISTS flow_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                sats_in INTEGER NOT NULL,
                sats_out INTEGER NOT NULL,
                flow_ratio REAL NOT NULL,
                state TEXT NOT NULL
            )
        """)
        
        # PID state table - stores controller state per channel
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pid_state (
                channel_id TEXT PRIMARY KEY,
                integral REAL NOT NULL DEFAULT 0,
                last_error REAL NOT NULL DEFAULT 0,
                last_fee_ppm INTEGER NOT NULL DEFAULT 0,
                last_update INTEGER NOT NULL
            )
        """)
        
        # Fee changes audit log
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fee_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                old_fee_ppm INTEGER NOT NULL,
                new_fee_ppm INTEGER NOT NULL,
                reason TEXT,
                manual INTEGER NOT NULL DEFAULT 0,
                timestamp INTEGER NOT NULL
            )
        """)
        
        # Rebalance history
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_channel TEXT NOT NULL,
                to_channel TEXT NOT NULL,
                amount_sats INTEGER NOT NULL,
                max_fee_sats INTEGER NOT NULL,
                actual_fee_sats INTEGER,
                expected_profit_sats INTEGER NOT NULL,
                actual_profit_sats INTEGER,
                status TEXT NOT NULL,  -- 'pending', 'success', 'failed'
                error_message TEXT,
                timestamp INTEGER NOT NULL
            )
        """)
        
        # Real-time forwards tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS forwards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                in_channel TEXT NOT NULL,
                out_channel TEXT NOT NULL,
                in_msat INTEGER NOT NULL,
                out_msat INTEGER NOT NULL,
                fee_msat INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )
        """)
        
        # Clboss unmanage tracking
        conn.execute("""
            CREATE TABLE IF NOT EXISTS clboss_unmanaged (
                peer_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                unmanaged_at INTEGER NOT NULL,
                PRIMARY KEY (peer_id, tag)
            )
        """)
        
        # Create indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_flow_history_channel ON flow_history(channel_id, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fee_changes_channel ON fee_changes(channel_id, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forwards_time ON forwards(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forwards_channels ON forwards(in_channel, out_channel)")
        
        self.plugin.log("Database initialized successfully")
    
    # =========================================================================
    # Channel State Methods
    # =========================================================================
    
    def update_channel_state(self, channel_id: str, peer_id: str, state: str,
                             flow_ratio: float, sats_in: int, sats_out: int, 
                             capacity: int):
        """Update the current state of a channel."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO channel_states 
            (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (channel_id, peer_id, state, flow_ratio, sats_in, sats_out, capacity, now))
        
        # Also record in history
        conn.execute("""
            INSERT INTO flow_history 
            (channel_id, timestamp, sats_in, sats_out, flow_ratio, state)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (channel_id, now, sats_in, sats_out, flow_ratio, state))
    
    def get_channel_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get the current state of a channel."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM channel_states WHERE channel_id = ?", 
            (channel_id,)
        ).fetchone()
        
        if row:
            return dict(row)
        return None
    
    def get_all_channel_states(self) -> List[Dict[str, Any]]:
        """Get states of all tracked channels."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM channel_states ORDER BY state, flow_ratio DESC").fetchall()
        return [dict(row) for row in rows]
    
    def get_channels_by_state(self, state: str) -> List[Dict[str, Any]]:
        """Get all channels with a specific state."""
        conn = self._get_connection()
        rows = conn.execute(
            "SELECT * FROM channel_states WHERE state = ?",
            (state,)
        ).fetchall()
        return [dict(row) for row in rows]
    
    # =========================================================================
    # PID State Methods
    # =========================================================================
    
    def get_pid_state(self, channel_id: str) -> Dict[str, Any]:
        """Get PID controller state for a channel."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM pid_state WHERE channel_id = ?",
            (channel_id,)
        ).fetchone()
        
        if row:
            return dict(row)
        
        # Return default state if not found
        return {
            'channel_id': channel_id,
            'integral': 0.0,
            'last_error': 0.0,
            'last_fee_ppm': 0,
            'last_update': 0
        }
    
    def update_pid_state(self, channel_id: str, integral: float, last_error: float, 
                         last_fee_ppm: int):
        """Update PID controller state for a channel."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO pid_state 
            (channel_id, integral, last_error, last_fee_ppm, last_update)
            VALUES (?, ?, ?, ?, ?)
        """, (channel_id, integral, last_error, last_fee_ppm, now))
    
    # =========================================================================
    # Fee Change Methods
    # =========================================================================
    
    def record_fee_change(self, channel_id: str, peer_id: str, old_fee_ppm: int,
                          new_fee_ppm: int, reason: str, manual: bool = False):
        """Record a fee change for audit purposes."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT INTO fee_changes 
            (channel_id, peer_id, old_fee_ppm, new_fee_ppm, reason, manual, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (channel_id, peer_id, old_fee_ppm, new_fee_ppm, reason, 1 if manual else 0, now))
    
    def get_recent_fee_changes(self, limit: int = 10, channel_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get recent fee changes, optionally filtered by channel."""
        conn = self._get_connection()
        
        if channel_id:
            rows = conn.execute("""
                SELECT * FROM fee_changes 
                WHERE channel_id = ?
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (channel_id, limit)).fetchall()
        else:
            rows = conn.execute("""
                SELECT * FROM fee_changes 
                ORDER BY timestamp DESC 
                LIMIT ?
            """, (limit,)).fetchall()
        
        return [dict(row) for row in rows]
    
    # =========================================================================
    # Rebalance History Methods
    # =========================================================================
    
    def record_rebalance(self, from_channel: str, to_channel: str, amount_sats: int,
                         max_fee_sats: int, expected_profit_sats: int,
                         status: str = 'pending') -> int:
        """Record a rebalance attempt and return its ID."""
        conn = self._get_connection()
        now = int(time.time())
        
        cursor = conn.execute("""
            INSERT INTO rebalance_history 
            (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats,
             status, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (from_channel, to_channel, amount_sats, max_fee_sats, expected_profit_sats,
              status, now))
        
        return cursor.lastrowid
    
    def update_rebalance_result(self, rebalance_id: int, status: str,
                                actual_fee_sats: Optional[int] = None,
                                actual_profit_sats: Optional[int] = None,
                                error_message: Optional[str] = None):
        """Update a rebalance record with the result."""
        conn = self._get_connection()
        
        conn.execute("""
            UPDATE rebalance_history 
            SET status = ?, actual_fee_sats = ?, actual_profit_sats = ?, error_message = ?
            WHERE id = ?
        """, (status, actual_fee_sats, actual_profit_sats, error_message, rebalance_id))
    
    def get_recent_rebalances(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent rebalance attempts."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM rebalance_history 
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(row) for row in rows]
    
    def get_last_rebalance_time(self, channel_id: str) -> Optional[int]:
        """
        Get the timestamp of the last successful rebalance for a channel.
        
        Used to enforce rebalance cooldown periods to prevent thrashing.
        
        Args:
            channel_id: The destination channel of the rebalance
            
        Returns:
            Unix timestamp of last successful rebalance, or None if never
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT MAX(timestamp) as last_time
            FROM rebalance_history 
            WHERE to_channel = ? AND status = 'success'
        """, (channel_id,)).fetchone()
        
        if row and row['last_time']:
            return row['last_time']
        return None
    
    # =========================================================================
    # Forward Tracking Methods
    # =========================================================================
    
    def record_forward(self, in_channel: str, out_channel: str, 
                       in_msat: int, out_msat: int, fee_msat: int):
        """Record a completed forward for real-time tracking."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT INTO forwards 
            (in_channel, out_channel, in_msat, out_msat, fee_msat, timestamp)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (in_channel, out_channel, in_msat, out_msat, fee_msat, now))
    
    def get_channel_forwards(self, channel_id: str, since_timestamp: int) -> Dict[str, int]:
        """Get aggregate forward stats for a channel since a timestamp."""
        conn = self._get_connection()
        
        # Get inbound flow (channel received HTLCs)
        row_in = conn.execute("""
            SELECT COALESCE(SUM(in_msat), 0) as total_in_msat
            FROM forwards
            WHERE in_channel = ? AND timestamp >= ?
        """, (channel_id, since_timestamp)).fetchone()
        
        # Get outbound flow (channel sent HTLCs)
        row_out = conn.execute("""
            SELECT COALESCE(SUM(out_msat), 0) as total_out_msat
            FROM forwards
            WHERE out_channel = ? AND timestamp >= ?
        """, (channel_id, since_timestamp)).fetchone()
        
        return {
            'in_msat': row_in['total_in_msat'] if row_in else 0,
            'out_msat': row_out['total_out_msat'] if row_out else 0
        }
    
    def get_daily_volume(self, days: int = 7) -> int:
        """Get total routing volume over the past N days."""
        conn = self._get_connection()
        since = int(time.time()) - (days * 86400)
        
        row = conn.execute("""
            SELECT COALESCE(SUM(out_msat), 0) as total_volume_msat
            FROM forwards
            WHERE timestamp >= ?
        """, (since,)).fetchone()
        
        return row['total_volume_msat'] // 1000 if row else 0
    
    # =========================================================================
    # Clboss Unmanage Tracking
    # =========================================================================
    
    def record_unmanage(self, peer_id: str, tag: str):
        """Record that we unmanaged a peer/tag from clboss."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO clboss_unmanaged 
            (peer_id, tag, unmanaged_at)
            VALUES (?, ?, ?)
        """, (peer_id, tag, now))
    
    def remove_unmanage(self, peer_id: str, tag: Optional[str] = None):
        """Remove unmanage record (when remanaging)."""
        conn = self._get_connection()
        
        if tag:
            conn.execute(
                "DELETE FROM clboss_unmanaged WHERE peer_id = ? AND tag = ?",
                (peer_id, tag)
            )
        else:
            conn.execute(
                "DELETE FROM clboss_unmanaged WHERE peer_id = ?",
                (peer_id,)
            )
    
    def is_unmanaged(self, peer_id: str, tag: str) -> bool:
        """Check if a peer/tag is currently unmanaged."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT 1 FROM clboss_unmanaged WHERE peer_id = ? AND tag = ?",
            (peer_id, tag)
        ).fetchone()
        return row is not None
    
    def get_all_unmanaged(self) -> List[Dict[str, Any]]:
        """Get all unmanaged peer/tag combinations."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM clboss_unmanaged").fetchall()
        return [dict(row) for row in rows]
    
    # =========================================================================
    # Cleanup Methods
    # =========================================================================
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Remove old data to prevent database bloat."""
        conn = self._get_connection()
        cutoff = int(time.time()) - (days_to_keep * 86400)
        
        conn.execute("DELETE FROM flow_history WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM forwards WHERE timestamp < ?", (cutoff,))
        
        self.plugin.log(f"Cleaned up data older than {days_to_keep} days")
    
    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
