#!/usr/bin/env python3
"""
cl-revenue-ops: A Revenue Operations Plugin for Core Lightning

This plugin acts as a "Revenue Operations" layer that sits on top of the clboss 
automated manager. While clboss handles channel creation and node reliability,
this plugin overrides clboss for fee setting and rebalancing decisions to 
maximize profitability based on economic principles rather than heuristics.

MANAGER-OVERRIDE PATTERN:
-------------------------
Before changing any channel state, this plugin checks if the peer is managed 
by clboss. If it is, we issue the `clboss-unmanage` command for that specific 
peer and tag (e.g., lnfee) to prevent clboss from reverting our changes.

This allows us to:
1. Let clboss handle what it's good at (channel creation, peer selection)
2. Take over the economic decisions (fee setting, rebalancing) where we can
   apply more sophisticated algorithms

Dependencies:
- pyln-client: Core Lightning plugin framework
- bookkeeper plugin (built-in): Historical routing data
- External rebalancer (circular/sling): Executes rebalance payments

Author: Lightning Goats Team
License: MIT
"""

import os
import sys
import time
import json
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import defaultdict
from pathlib import Path

from pyln.client import Plugin, RpcError

# Import our modules
from modules.flow_analysis import FlowAnalyzer, ChannelState
from modules.fee_controller import PIDFeeController
from modules.rebalancer import EVRebalancer
from modules.clboss_manager import ClbossManager
from modules.config import Config
from modules.database import Database

# Initialize the plugin
plugin = Plugin()

# Global instances (initialized in init)
flow_analyzer: Optional[FlowAnalyzer] = None
fee_controller: Optional[PIDFeeController] = None
rebalancer: Optional[EVRebalancer] = None
clboss_manager: Optional[ClbossManager] = None
database: Optional[Database] = None
config: Optional[Config] = None


# =============================================================================
# PLUGIN OPTIONS
# =============================================================================

plugin.add_option(
    name='revenue-ops-db-path',
    default='~/.lightning/revenue_ops.db',
    description='Path to the SQLite database for storing state'
)

plugin.add_option(
    name='revenue-ops-flow-interval',
    default='3600',
    description='Interval in seconds for flow analysis (default: 1 hour)'
)

plugin.add_option(
    name='revenue-ops-fee-interval',
    default='1800',
    description='Interval in seconds for fee adjustments (default: 30 min)'
)

plugin.add_option(
    name='revenue-ops-rebalance-interval',
    default='900',
    description='Interval in seconds for rebalance checks (default: 15 min)'
)

plugin.add_option(
    name='revenue-ops-target-flow',
    default='100000',
    description='Target daily flow in sats per channel (default: 100,000)'
)

plugin.add_option(
    name='revenue-ops-min-fee-ppm',
    default='10',
    description='Minimum fee floor in PPM (default: 10)'
)

plugin.add_option(
    name='revenue-ops-max-fee-ppm',
    default='5000',
    description='Maximum fee ceiling in PPM (default: 5000)'
)

plugin.add_option(
    name='revenue-ops-rebalance-min-profit',
    default='10',
    description='Minimum profit in sats to trigger rebalance (default: 10)'
)

plugin.add_option(
    name='revenue-ops-pid-kp',
    default='0.5',
    description='PID Proportional gain (default: 0.5)'
)

plugin.add_option(
    name='revenue-ops-pid-ki',
    default='0.1',
    description='PID Integral gain (default: 0.1)'
)

plugin.add_option(
    name='revenue-ops-pid-kd',
    default='0.05',
    description='PID Derivative gain (default: 0.05)'
)

plugin.add_option(
    name='revenue-ops-flow-window-days',
    default='7',
    description='Number of days to analyze for flow calculation (default: 7)'
)

plugin.add_option(
    name='revenue-ops-clboss-enabled',
    default='true',
    description='Whether to interact with clboss for unmanage commands (default: true)'
)

plugin.add_option(
    name='revenue-ops-rebalancer',
    default='circular',
    description='Which rebalancer plugin to use: circular or sling (default: circular)'
)

plugin.add_option(
    name='revenue-ops-dry-run',
    default='false',
    description='If true, log actions but do not execute (default: false)'
)


# =============================================================================
# INITIALIZATION
# =============================================================================

@plugin.init()
def init(options: Dict[str, Any], configuration: Dict[str, Any], plugin: Plugin, **kwargs):
    """
    Initialize the Revenue Operations plugin.
    
    This is called once when the plugin starts. We:
    1. Parse and validate options
    2. Initialize the database
    3. Create instances of our analysis modules
    4. Set up timers for periodic execution
    """
    global flow_analyzer, fee_controller, rebalancer, clboss_manager, database, config
    
    plugin.log("Initializing cl-revenue-ops plugin...")
    
    # Build configuration from options
    config = Config(
        db_path=os.path.expanduser(options['revenue-ops-db-path']),
        flow_interval=int(options['revenue-ops-flow-interval']),
        fee_interval=int(options['revenue-ops-fee-interval']),
        rebalance_interval=int(options['revenue-ops-rebalance-interval']),
        target_flow=int(options['revenue-ops-target-flow']),
        min_fee_ppm=int(options['revenue-ops-min-fee-ppm']),
        max_fee_ppm=int(options['revenue-ops-max-fee-ppm']),
        rebalance_min_profit=int(options['revenue-ops-rebalance-min-profit']),
        pid_kp=float(options['revenue-ops-pid-kp']),
        pid_ki=float(options['revenue-ops-pid-ki']),
        pid_kd=float(options['revenue-ops-pid-kd']),
        flow_window_days=int(options['revenue-ops-flow-window-days']),
        clboss_enabled=options['revenue-ops-clboss-enabled'].lower() == 'true',
        rebalancer_plugin=options['revenue-ops-rebalancer'],
        dry_run=options['revenue-ops-dry-run'].lower() == 'true'
    )
    
    plugin.log(f"Configuration loaded: target_flow={config.target_flow}, "
               f"fee_range=[{config.min_fee_ppm}, {config.max_fee_ppm}], "
               f"dry_run={config.dry_run}")
    
    # Initialize database
    database = Database(config.db_path, plugin)
    database.initialize()
    
    # Initialize clboss manager (handles unmanage commands)
    clboss_manager = ClbossManager(plugin, config)
    
    # Initialize analysis modules
    flow_analyzer = FlowAnalyzer(plugin, config, database)
    fee_controller = PIDFeeController(plugin, config, database, clboss_manager)
    rebalancer = EVRebalancer(plugin, config, database, clboss_manager)
    
    # Set up periodic background tasks using threading
    # Note: plugin.log() is safe to call from threads in pyln-client
    # We use daemon threads so they don't block shutdown
    
    def flow_analysis_loop():
        """Background loop for flow analysis."""
        # Initial delay to let lightningd fully start
        time.sleep(10)
        while True:
            try:
                plugin.log("Running scheduled flow analysis...")
                run_flow_analysis()
                
                # Run cleanup on each iteration (it's a fast DELETE query)
                # Keeps history tables from growing unbounded over months
                if database:
                    database.cleanup_old_data(days_to_keep=30)
                    
            except Exception as e:
                plugin.log(f"Error in flow analysis: {e}", level='error')
            time.sleep(config.flow_interval)
    
    def fee_adjustment_loop():
        """Background loop for fee adjustment."""
        # Initial delay to let flow analysis run first
        time.sleep(60)
        while True:
            try:
                plugin.log("Running scheduled fee adjustment...")
                run_fee_adjustment()
            except Exception as e:
                plugin.log(f"Error in fee adjustment: {e}", level='error')
            time.sleep(config.fee_interval)
    
    def rebalance_check_loop():
        """Background loop for rebalance checks."""
        # Initial delay to let other analyses run first
        time.sleep(120)
        while True:
            try:
                plugin.log("Running scheduled rebalance check...")
                run_rebalance_check()
            except Exception as e:
                plugin.log(f"Error in rebalance check: {e}", level='error')
            time.sleep(config.rebalance_interval)
    
    # Start background threads (daemon=True so they don't block shutdown)
    threading.Thread(target=flow_analysis_loop, daemon=True, name="flow-analysis").start()
    threading.Thread(target=fee_adjustment_loop, daemon=True, name="fee-adjustment").start()
    threading.Thread(target=rebalance_check_loop, daemon=True, name="rebalance-check").start()
    
    plugin.log("cl-revenue-ops plugin initialized successfully!")
    return None


# =============================================================================
# CORE LOGIC FUNCTIONS
# =============================================================================

def run_flow_analysis():
    """
    Module 1: Flow Analysis & Sink/Source Detection
    
    Query bookkeeper to calculate the "Net Flow" of every channel over 
    the last N days. Calculate FlowRatio and mark channels as Source/Sink/Balanced.
    """
    if flow_analyzer is None:
        plugin.log("Flow analyzer not initialized", level='error')
        return
    
    try:
        results = flow_analyzer.analyze_all_channels()
        plugin.log(f"Flow analysis complete: {len(results)} channels analyzed")
        
        # Log summary
        sources = sum(1 for r in results.values() if r.state == ChannelState.SOURCE)
        sinks = sum(1 for r in results.values() if r.state == ChannelState.SINK)
        balanced = sum(1 for r in results.values() if r.state == ChannelState.BALANCED)
        plugin.log(f"Channel states: {sources} sources, {sinks} sinks, {balanced} balanced")
        
    except Exception as e:
        plugin.log(f"Flow analysis failed: {e}", level='error')
        raise


def run_fee_adjustment():
    """
    Module 2: PID Fee Controller (Dynamic Pricing)
    
    Adjust channel fees based on the Flow Analysis using a PID controller.
    Before setting fees, unmanage from clboss to prevent conflicts.
    """
    if fee_controller is None:
        plugin.log("Fee controller not initialized", level='error')
        return
    
    try:
        adjustments = fee_controller.adjust_all_fees()
        plugin.log(f"Fee adjustment complete: {len(adjustments)} channels adjusted")
        
    except Exception as e:
        plugin.log(f"Fee adjustment failed: {e}", level='error')
        raise


def run_rebalance_check():
    """
    Module 3: EV-Based Rebalancing (Profit-Aware)
    
    Identify rebalance candidates based on expected value calculation.
    Only trigger rebalances when the EV is positive and significant.
    """
    if rebalancer is None:
        plugin.log("Rebalancer not initialized", level='error')
        return
    
    try:
        candidates = rebalancer.find_rebalance_candidates()
        plugin.log(f"Rebalance check complete: {len(candidates)} profitable candidates found")
        
        for candidate in candidates:
            rebalancer.execute_rebalance(candidate)
            
    except Exception as e:
        plugin.log(f"Rebalance check failed: {e}", level='error')
        raise


# =============================================================================
# RPC METHODS - Exposed to lightning-cli
# =============================================================================

@plugin.method("revenue-status")
def revenue_status(plugin: Plugin) -> Dict[str, Any]:
    """
    Get the current status of the revenue operations plugin.
    
    Usage: lightning-cli revenue-status
    """
    if database is None:
        return {"error": "Plugin not fully initialized"}
    
    channel_states = database.get_all_channel_states()
    fee_history = database.get_recent_fee_changes(limit=10)
    rebalance_history = database.get_recent_rebalances(limit=10)
    
    return {
        "status": "running",
        "config": {
            "target_flow_sats": config.target_flow,
            "fee_range_ppm": [config.min_fee_ppm, config.max_fee_ppm],
            "rebalance_min_profit_sats": config.rebalance_min_profit,
            "dry_run": config.dry_run
        },
        "channel_states": channel_states,
        "recent_fee_changes": fee_history,
        "recent_rebalances": rebalance_history
    }


@plugin.method("revenue-analyze")
def revenue_analyze(plugin: Plugin, channel_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Run flow analysis on demand (optionally for a specific channel).
    
    Usage: lightning-cli revenue-analyze [channel_id]
    """
    if flow_analyzer is None:
        return {"error": "Plugin not fully initialized"}
    
    if channel_id:
        result = flow_analyzer.analyze_channel(channel_id)
        return {"channel": channel_id, "analysis": result.__dict__ if result else None}
    else:
        run_flow_analysis()
        return {"status": "Flow analysis triggered"}


@plugin.method("revenue-set-fee")
def revenue_set_fee(plugin: Plugin, channel_id: str, fee_ppm: int) -> Dict[str, Any]:
    """
    Manually set fee for a channel (with clboss unmanage).
    
    Usage: lightning-cli revenue-set-fee channel_id fee_ppm
    """
    if fee_controller is None:
        return {"error": "Plugin not fully initialized"}
    
    try:
        result = fee_controller.set_channel_fee(channel_id, fee_ppm, manual=True)
        return {"status": "success", "channel": channel_id, "new_fee_ppm": fee_ppm, **result}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@plugin.method("revenue-rebalance")
def revenue_rebalance(plugin: Plugin, 
                      from_channel: str, 
                      to_channel: str, 
                      amount_sats: int,
                      max_fee_sats: Optional[int] = None) -> Dict[str, Any]:
    """
    Manually trigger a rebalance with profit constraints.
    
    Usage: lightning-cli revenue-rebalance from_channel to_channel amount_sats [max_fee_sats]
    """
    if rebalancer is None:
        return {"error": "Plugin not fully initialized"}
    
    try:
        result = rebalancer.manual_rebalance(from_channel, to_channel, amount_sats, max_fee_sats)
        return {"status": "success", **result}
    except Exception as e:
        return {"status": "error", "error": str(e)}


@plugin.method("revenue-clboss-status")
def revenue_clboss_status(plugin: Plugin) -> Dict[str, Any]:
    """
    Check which channels are currently unmanaged from clboss.
    
    Usage: lightning-cli revenue-clboss-status
    """
    if clboss_manager is None:
        return {"error": "Plugin not fully initialized"}
    
    return clboss_manager.get_unmanaged_status()


@plugin.method("revenue-remanage")
def revenue_remanage(plugin: Plugin, peer_id: str, tag: Optional[str] = None) -> Dict[str, Any]:
    """
    Re-enable clboss management for a peer (release our override).
    
    Usage: lightning-cli revenue-remanage peer_id [tag]
    """
    if clboss_manager is None:
        return {"error": "Plugin not fully initialized"}
    
    try:
        result = clboss_manager.remanage(peer_id, tag)
        return {"status": "success", "peer_id": peer_id, **result}
    except Exception as e:
        return {"status": "error", "error": str(e)}


# =============================================================================
# HOOKS - React to Lightning events
# =============================================================================

@plugin.hook("htlc_accepted")
def on_htlc_accepted(onion: Dict, htlc: Dict, plugin: Plugin, **kwargs) -> Dict[str, str]:
    """
    Hook called when an HTLC is accepted.
    
    We can use this to track live routing activity and update our flow metrics
    in real-time rather than waiting for periodic analysis.
    
    For now, we just let it pass through - the periodic analysis from bookkeeper
    is sufficient for initial implementation.
    """
    # Just continue - we don't want to interfere with routing
    return {"result": "continue"}


@plugin.subscribe("forward_event")
def on_forward_event(forward_event: Dict, plugin: Plugin, **kwargs):
    """
    Notification when a forward completes (success or failure).
    
    We can use this for real-time flow tracking to complement our periodic
    bookkeeper analysis.
    """
    if database is None:
        return
    
    status = forward_event.get("status")
    if status == "settled":
        # Record successful forward for real-time metrics
        in_channel = forward_event.get("in_channel")
        out_channel = forward_event.get("out_channel")
        in_msat = forward_event.get("in_msat", 0)
        out_msat = forward_event.get("out_msat", 0)
        fee_msat = forward_event.get("fee_msat", 0)
        
        database.record_forward(in_channel, out_channel, in_msat, out_msat, fee_msat)


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    plugin.run()
