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
        # LEGACY: Kept for backward compatibility, but Hill Climbing uses fee_strategy_state
        conn.execute("""
            CREATE TABLE IF NOT EXISTS pid_state (
                channel_id TEXT PRIMARY KEY,
                integral REAL NOT NULL DEFAULT 0,
                last_error REAL NOT NULL DEFAULT 0,
                last_fee_ppm INTEGER NOT NULL DEFAULT 0,
                last_update INTEGER NOT NULL
            )
        """)
        
        # NEW: Fee Strategy State table for Hill Climbing controller
        # Stores state for the revenue-maximizing Perturb & Observe algorithm
        # UPDATED: Uses last_revenue_rate (REAL) for rate-based feedback instead of
        # last_revenue_sats to measure revenue per hour since last fee change.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fee_strategy_state (
                channel_id TEXT PRIMARY KEY,
                last_revenue_rate REAL NOT NULL DEFAULT 0.0,
                last_fee_ppm INTEGER NOT NULL DEFAULT 0,
                trend_direction INTEGER NOT NULL DEFAULT 1,  -- 1 = increase, -1 = decrease
                step_ppm INTEGER NOT NULL DEFAULT 50,  -- Current step size (for dampening)
                consecutive_same_direction INTEGER NOT NULL DEFAULT 0,
                last_update INTEGER NOT NULL DEFAULT 0
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
        
        # Channel open costs tracking (for profitability analysis)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_costs (
                channel_id TEXT PRIMARY KEY,
                peer_id TEXT NOT NULL,
                open_cost_sats INTEGER NOT NULL DEFAULT 0,
                capacity_sats INTEGER NOT NULL,
                opened_at INTEGER NOT NULL
            )
        """)
        
        # Rebalance costs tracking (cumulative per channel)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rebalance_costs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                peer_id TEXT NOT NULL,
                cost_sats INTEGER NOT NULL,
                amount_sats INTEGER NOT NULL,
                timestamp INTEGER NOT NULL
            )
        """)
        
        # Channel failure tracking for adaptive backoff (persisted across restarts)
        # This prevents "retry storms" after plugin restart
        conn.execute("""
            CREATE TABLE IF NOT EXISTS channel_failures (
                channel_id TEXT PRIMARY KEY,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_failure_time INTEGER NOT NULL DEFAULT 0
            )
        """)
        
        # Create indexes for common queries
        conn.execute("CREATE INDEX IF NOT EXISTS idx_flow_history_channel ON flow_history(channel_id, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fee_changes_channel ON fee_changes(channel_id, timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forwards_time ON forwards(timestamp)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_forwards_channels ON forwards(in_channel, out_channel)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_rebalance_costs_channel ON rebalance_costs(channel_id)")
        
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
    # PID State Methods (LEGACY - kept for backward compatibility)
    # =========================================================================
    
    def get_pid_state(self, channel_id: str) -> Dict[str, Any]:
        """Get PID controller state for a channel (LEGACY)."""
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
        """Update PID controller state for a channel (LEGACY)."""
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO pid_state 
            (channel_id, integral, last_error, last_fee_ppm, last_update)
            VALUES (?, ?, ?, ?, ?)
        """, (channel_id, integral, last_error, last_fee_ppm, now))
    
    # =========================================================================
    # Fee Strategy State Methods (NEW - Hill Climbing Controller)
    # =========================================================================
    
    def get_fee_strategy_state(self, channel_id: str) -> Dict[str, Any]:
        """
        Get Hill Climbing fee strategy state for a channel.
        
        Used by the revenue-maximizing Perturb & Observe algorithm.
        
        Args:
            channel_id: Channel to get state for
            
        Returns:
            Dict with last_revenue_rate, last_fee_ppm, trend_direction, step_ppm, etc.
        """
        conn = self._get_connection()
        row = conn.execute(
            "SELECT * FROM fee_strategy_state WHERE channel_id = ?",
            (channel_id,)
        ).fetchone()
        
        if row:
            result = dict(row)
            # Handle migration: old schema had last_revenue_sats (int)
            # New schema uses last_revenue_rate (float)
            if 'last_revenue_sats' in result and 'last_revenue_rate' not in result:
                result['last_revenue_rate'] = float(result.get('last_revenue_sats', 0))
            # Ensure step_ppm is present (may be missing from old schema)
            if 'step_ppm' not in result:
                result['step_ppm'] = 50  # Default step size
            return result
        
        # Return default state if not found
        return {
            'channel_id': channel_id,
            'last_revenue_rate': 0.0,  # Revenue rate in sats/hour
            'last_fee_ppm': 0,
            'trend_direction': 1,  # Default: try increasing fee
            'step_ppm': 50,  # Default step size for dampening
            'consecutive_same_direction': 0,
            'last_update': 0
        }
    
    def update_fee_strategy_state(self, channel_id: str, last_revenue_rate: float,
                                   last_fee_ppm: int, trend_direction: int,
                                   step_ppm: int = 50,
                                   consecutive_same_direction: int = 0):
        """
        Update Hill Climbing fee strategy state for a channel.
        
        Called after each fee adjustment iteration to record the state
        for the next observation period.
        
        Args:
            channel_id: Channel to update
            last_revenue_rate: Revenue rate in sats/hour observed since last change
            last_fee_ppm: Fee that was in effect
            trend_direction: Direction we were moving (1 = up, -1 = down)
            step_ppm: Current step size (for wiggle dampening)
            consecutive_same_direction: How many times we've moved same way
        """
        conn = self._get_connection()
        now = int(time.time())
        
        conn.execute("""
            INSERT OR REPLACE INTO fee_strategy_state 
            (channel_id, last_revenue_rate, last_fee_ppm, trend_direction,
             step_ppm, consecutive_same_direction, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (channel_id, last_revenue_rate, last_fee_ppm, trend_direction,
              step_ppm, consecutive_same_direction, now))
    
    def get_all_fee_strategy_states(self) -> List[Dict[str, Any]]:
        """Get fee strategy state for all channels."""
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM fee_strategy_state").fetchall()
        return [dict(row) for row in rows]
    
    def reset_fee_strategy_state(self, channel_id: str):
        """
        Reset the fee strategy state for a channel.
        
        Use when manually intervening or if the controller is behaving erratically.
        """
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM fee_strategy_state WHERE channel_id = ?",
            (channel_id,)
        )
        self.plugin.log(f"Reset fee strategy state for {channel_id}")
    
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
    
    def get_total_rebalance_fees(self, since_timestamp: int) -> int:
        """
        Get the total rebalancing fees spent since a given timestamp.
        
        Used for Global Capital Controls to enforce daily budget limits.
        Sums actual_fee_sats from successful rebalances since the timestamp.
        
        Args:
            since_timestamp: Unix timestamp to start summing from
            
        Returns:
            Total fees spent in sats (0 if none)
        """
        conn = self._get_connection()
        row = conn.execute("""
            SELECT COALESCE(SUM(actual_fee_sats), 0) as total_fees
            FROM rebalance_history 
            WHERE timestamp >= ? AND status = 'success' AND actual_fee_sats IS NOT NULL
        """, (since_timestamp,)).fetchone()
        
        return row['total_fees'] if row else 0
    
    def get_rebalance_history_by_peer(self, peer_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Get rebalance history for channels belonging to a specific peer.
        
        Joins rebalance_history with channel_states to find channels
        for the given peer, then returns rebalances to those channels.
        
        Args:
            peer_id: The peer node ID
            limit: Maximum records to return
            
        Returns:
            List of rebalance records with fee and amount info
        """
        conn = self._get_connection()
        
        # First get all channels for this peer
        peer_channels = conn.execute("""
            SELECT channel_id FROM channel_states WHERE peer_id = ?
        """, (peer_id,)).fetchall()
        
        if not peer_channels:
            return []
        
        channel_ids = [row['channel_id'] for row in peer_channels]
        placeholders = ','.join('?' * len(channel_ids))
        
        # Get rebalances to these channels
        rows = conn.execute(f"""
            SELECT 
                to_channel,
                amount_sats,
                actual_fee_sats as fee_paid_msat,
                amount_sats * 1000 as amount_msat,
                status,
                timestamp
            FROM rebalance_history 
            WHERE to_channel IN ({placeholders})
            ORDER BY timestamp DESC 
            LIMIT ?
        """, (*channel_ids, limit)).fetchall()
        
        return [dict(row) for row in rows]
    
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
    
    def get_volume_since(self, channel_id: str, timestamp: int) -> int:
        """
        Get total outbound volume for a channel since a specific timestamp.
        
        This is used by the Fee Controller to measure volume specifically
        since the last fee change, rather than using a 7-day average which
        dilutes the signal of recent fee changes.
        
        Args:
            channel_id: Channel to get volume for
            timestamp: Unix timestamp to start counting from
            
        Returns:
            Total outbound volume in satoshis since the timestamp
        """
        conn = self._get_connection()
        
        row = conn.execute("""
            SELECT COALESCE(SUM(out_msat), 0) as total_out_msat
            FROM forwards
            WHERE out_channel = ? AND timestamp > ?
        """, (channel_id, timestamp)).fetchone()
        
        # Convert msat to sats
        return (row['total_out_msat'] // 1000) if row else 0
    
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
    # Profitability Tracking Methods
    # =========================================================================
    
    def record_channel_open_cost(self, channel_id: str, peer_id: str,
                                  open_cost_sats: int, capacity_sats: int,
                                  timestamp: Optional[int] = None):
        """Record the cost to open a channel."""
        conn = self._get_connection()
        conn.execute("""
            INSERT OR REPLACE INTO channel_costs 
            (channel_id, peer_id, open_cost_sats, capacity_sats, opened_at)
            VALUES (?, ?, ?, ?, ?)
        """, (channel_id, peer_id, open_cost_sats, capacity_sats, 
              timestamp or int(time.time())))
        conn.commit()
    
    def get_channel_open_cost(self, channel_id: str) -> Optional[int]:
        """Get the recorded open cost for a channel."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT open_cost_sats FROM channel_costs WHERE channel_id = ?",
            (channel_id,)
        ).fetchone()
        return row["open_cost_sats"] if row else None
    
    def record_rebalance_cost(self, channel_id: str, peer_id: str,
                              cost_sats: int, amount_sats: int,
                              timestamp: Optional[int] = None):
        """Record a rebalance cost for a channel."""
        conn = self._get_connection()
        conn.execute("""
            INSERT INTO rebalance_costs 
            (channel_id, peer_id, cost_sats, amount_sats, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (channel_id, peer_id, cost_sats, amount_sats,
              timestamp or int(time.time())))
        conn.commit()
    
    def get_channel_rebalance_costs(self, channel_id: str) -> int:
        """Get total rebalance costs for a channel."""
        conn = self._get_connection()
        row = conn.execute(
            "SELECT COALESCE(SUM(cost_sats), 0) as total FROM rebalance_costs WHERE channel_id = ?",
            (channel_id,)
        ).fetchone()
        return row["total"] if row else 0
    
    def get_channel_cost_history(self, channel_id: str) -> List[Dict[str, Any]]:
        """Get detailed cost history for a channel."""
        conn = self._get_connection()
        rows = conn.execute("""
            SELECT * FROM rebalance_costs 
            WHERE channel_id = ? 
            ORDER BY timestamp DESC
        """, (channel_id,)).fetchall()
        return [dict(row) for row in rows]
    
    def get_all_channel_costs(self) -> Dict[str, Dict[str, int]]:
        """Get summary of costs for all channels."""
        conn = self._get_connection()
        
        result = {}
        
        # Get open costs
        open_rows = conn.execute("SELECT * FROM channel_costs").fetchall()
        for row in open_rows:
            channel_id = row["channel_id"]
            result[channel_id] = {
                "open_cost_sats": row["open_cost_sats"],
                "rebalance_cost_sats": 0,
                "total_cost_sats": row["open_cost_sats"]
            }
        
        # Get rebalance costs
        rebalance_rows = conn.execute("""
            SELECT channel_id, SUM(cost_sats) as total 
            FROM rebalance_costs 
            GROUP BY channel_id
        """).fetchall()
        
        for row in rebalance_rows:
            channel_id = row["channel_id"]
            if channel_id not in result:
                result[channel_id] = {
                    "open_cost_sats": 0,
                    "rebalance_cost_sats": 0,
                    "total_cost_sats": 0
                }
            result[channel_id]["rebalance_cost_sats"] = row["total"]
            result[channel_id]["total_cost_sats"] = (
                result[channel_id]["open_cost_sats"] + row["total"]
            )
        
        return result
    
    # =========================================================================
    # Channel Failure Tracking Methods (Persistent Backoff)
    # =========================================================================
    
    def get_failure_count(self, channel_id: str) -> Tuple[int, int]:
        """
        Get the failure count and last failure time for a channel.
        
        Used by the rebalancer for adaptive backoff logic.
        
        Args:
            channel_id: Channel to query
            
        Returns:
            Tuple of (failure_count, last_failure_time)
        """
        conn = self._get_connection()
        row = conn.execute(
            "SELECT failure_count, last_failure_time FROM channel_failures WHERE channel_id = ?",
            (channel_id,)
        ).fetchone()
        
        if row:
            return (row["failure_count"], row["last_failure_time"])
        return (0, 0)
    
    def increment_failure_count(self, channel_id: str) -> int:
        """
        Increment the failure count for a channel and update last failure time.
        
        Called when a rebalance attempt fails.
        
        Args:
            channel_id: Channel that failed
            
        Returns:
            New failure count
        """
        conn = self._get_connection()
        now = int(time.time())
        
        # Get current count
        current_count, _ = self.get_failure_count(channel_id)
        new_count = current_count + 1
        
        conn.execute("""
            INSERT OR REPLACE INTO channel_failures 
            (channel_id, failure_count, last_failure_time)
            VALUES (?, ?, ?)
        """, (channel_id, new_count, now))
        
        return new_count
    
    def reset_failure_count(self, channel_id: str) -> None:
        """
        Reset the failure count for a channel (e.g., after successful rebalance).
        
        Args:
            channel_id: Channel to reset
        """
        conn = self._get_connection()
        conn.execute(
            "DELETE FROM channel_failures WHERE channel_id = ?",
            (channel_id,)
        )
    
    def get_all_failure_counts(self) -> Dict[str, Tuple[int, int]]:
        """
        Get failure counts for all channels with recorded failures.
        
        Returns:
            Dict mapping channel_id to (failure_count, last_failure_time)
        """
        conn = self._get_connection()
        rows = conn.execute("SELECT * FROM channel_failures").fetchall()
        return {row["channel_id"]: (row["failure_count"], row["last_failure_time"]) for row in rows}
    
    # =========================================================================
    # Cleanup Methods
    # =========================================================================
    
    def cleanup_old_data(self, days_to_keep: int = 8):
        """
        Remove old data to prevent database bloat.
        
        AGGRESSIVE PRUNING (Day 2 Task 3):
        The forwards table grows very fast on high-traffic nodes. We only need
        data for the flow_window_days (default 7 days), so we default to keeping
        8 days (7 + 1 buffer) instead of the previous 30 days.
        
        The caller (cl-revenue-ops.py) should pass max(8, config.flow_window_days + 1)
        to ensure we keep enough data for flow analysis while preventing bloat.
        
        Args:
            days_to_keep: Number of days of data to retain (default 8)
        """
        conn = self._get_connection()
        cutoff = int(time.time()) - (days_to_keep * 86400)
        
        # Count rows before deletion for logging
        flow_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM flow_history WHERE timestamp < ?", (cutoff,)
        ).fetchone()["cnt"]
        forwards_count = conn.execute(
            "SELECT COUNT(*) as cnt FROM forwards WHERE timestamp < ?", (cutoff,)
        ).fetchone()["cnt"]
        
        conn.execute("DELETE FROM flow_history WHERE timestamp < ?", (cutoff,))
        conn.execute("DELETE FROM forwards WHERE timestamp < ?", (cutoff,))
        
        if flow_count > 0 or forwards_count > 0:
            self.plugin.log(
                f"Cleaned up data older than {days_to_keep} days: "
                f"{flow_count} flow_history rows, {forwards_count} forwards rows"
            )
    
    def close(self):
        """Close the database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
