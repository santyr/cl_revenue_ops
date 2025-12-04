"""
Flow Analysis module for cl-revenue-ops

MODULE 1: Flow Analysis & Sink/Source Detection

This module analyzes routing flow through each channel to classify them as:
- SOURCE: Channels that are draining (sats flowing out)
- SINK: Channels that are filling up (sats flowing in)
- BALANCED: Channels with roughly equal in/out flow

The classification drives fee and rebalancing decisions:
- Sources need higher fees (scarce outbound liquidity)
- Sinks need lower fees (encourage outflow)
- Balanced channels are at target state

Data Sources:
1. bookkeeper plugin (preferred) - provides accounting-grade data
2. listforwards RPC - fallback if bookkeeper unavailable
"""

import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta

from pyln.client import Plugin, RpcError


class ChannelState(Enum):
    """
    Classification of channel flow state.
    
    SOURCE: Net outflow - channel is draining
    SINK: Net inflow - channel is filling
    BALANCED: Roughly equal flow - ideal state
    UNKNOWN: Not enough data to classify
    """
    SOURCE = "source"
    SINK = "sink"
    BALANCED = "balanced"
    UNKNOWN = "unknown"


@dataclass
class FlowMetrics:
    """
    Flow metrics for a single channel.
    
    Attributes:
        channel_id: Short channel ID
        peer_id: Node ID of the peer
        sats_in: Total sats routed into this channel (from peer)
        sats_out: Total sats routed out of this channel (to peer)
        capacity: Channel capacity in sats
        flow_ratio: (sats_out - sats_in) / capacity
        state: Classified state (SOURCE/SINK/BALANCED)
        daily_volume: Average daily routing volume
        analysis_window_days: Number of days analyzed
    """
    channel_id: str
    peer_id: str
    sats_in: int
    sats_out: int
    capacity: int
    flow_ratio: float
    state: ChannelState
    daily_volume: int
    analysis_window_days: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "sats_in": self.sats_in,
            "sats_out": self.sats_out,
            "capacity": self.capacity,
            "flow_ratio": round(self.flow_ratio, 4),
            "state": self.state.value,
            "daily_volume": self.daily_volume,
            "analysis_window_days": self.analysis_window_days
        }


class FlowAnalyzer:
    """
    Analyzes routing flow to classify channels as Source/Sink/Balanced.
    
    Flow Analysis Logic:
    1. Query bookkeeper or listforwards for historical routing data
    2. Calculate net flow for each channel over the analysis window
    3. Compute FlowRatio = (SatsOut - SatsIn) / Capacity
    4. Classify based on thresholds:
       - FlowRatio > 0.5: SOURCE (draining)
       - FlowRatio < -0.5: SINK (filling)
       - Otherwise: BALANCED
    """
    
    def __init__(self, plugin: Plugin, config, database):
        """
        Initialize the flow analyzer.
        
        Args:
            plugin: Reference to the pyln Plugin
            config: Configuration object
            database: Database instance for persistence
        """
        self.plugin = plugin
        self.config = config
        self.database = database
        self._bookkeeper_available: Optional[bool] = None
    
    def is_bookkeeper_available(self) -> bool:
        """
        Check if the bookkeeper plugin is available.
        
        Returns:
            True if bookkeeper commands are available
        """
        if self._bookkeeper_available is not None:
            return self._bookkeeper_available
        
        try:
            # Try a bookkeeper command
            self.plugin.rpc.call("bkpr-listbalances")
            self._bookkeeper_available = True
            self.plugin.log("bookkeeper plugin detected and available")
            return True
        except RpcError as e:
            if "Unknown command" in str(e):
                self._bookkeeper_available = False
                self.plugin.log("bookkeeper not available, using listforwards fallback")
                return False
            # Other errors might be temporary
            return True
        except Exception as e:
            self.plugin.log(f"Error checking bookkeeper: {e}", level='warning')
            self._bookkeeper_available = False
            return False
    
    def analyze_all_channels(self) -> Dict[str, FlowMetrics]:
        """
        Analyze flow for all channels.
        
        This is the main entry point, called periodically by the timer.
        
        Returns:
            Dict mapping channel_id to FlowMetrics
        """
        results = {}
        
        # Get list of all channels
        channels = self._get_channels()
        
        if not channels:
            self.plugin.log("No channels found to analyze")
            return results
        
        self.plugin.log(f"Analyzing flow for {len(channels)} channels")
        
        # Get flow data (from bookkeeper or listforwards)
        if self.is_bookkeeper_available():
            flow_data = self._get_flow_from_bookkeeper()
        else:
            flow_data = self._get_flow_from_listforwards()
        
        # Analyze each channel
        for channel in channels:
            channel_id = channel.get("short_channel_id") or channel.get("channel_id")
            if not channel_id:
                continue
            
            peer_id = channel.get("peer_id", "")
            capacity = channel.get("capacity_msat", 0) // 1000  # Convert to sats
            if capacity == 0:
                capacity = channel.get("capacity", 0)
            
            # Get flow data for this channel
            channel_flow = flow_data.get(channel_id, {"in": 0, "out": 0})
            sats_in = channel_flow.get("in", 0)
            sats_out = channel_flow.get("out", 0)
            
            # Calculate metrics
            metrics = self._calculate_metrics(
                channel_id=channel_id,
                peer_id=peer_id,
                sats_in=sats_in,
                sats_out=sats_out,
                capacity=capacity
            )
            
            results[channel_id] = metrics
            
            # Store in database
            self.database.update_channel_state(
                channel_id=channel_id,
                peer_id=peer_id,
                state=metrics.state.value,
                flow_ratio=metrics.flow_ratio,
                sats_in=sats_in,
                sats_out=sats_out,
                capacity=capacity
            )
        
        return results
    
    def analyze_channel(self, channel_id: str) -> Optional[FlowMetrics]:
        """
        Analyze flow for a specific channel.
        
        Args:
            channel_id: The channel to analyze
            
        Returns:
            FlowMetrics for the channel, or None if not found
        """
        # Get channel info
        channel = self._get_channel(channel_id)
        if not channel:
            return None
        
        peer_id = channel.get("peer_id", "")
        capacity = channel.get("capacity_msat", 0) // 1000
        if capacity == 0:
            capacity = channel.get("capacity", 0)
        
        # Get flow data
        if self.is_bookkeeper_available():
            flow_data = self._get_flow_from_bookkeeper(channel_id)
        else:
            flow_data = self._get_flow_from_listforwards(channel_id)
        
        channel_flow = flow_data.get(channel_id, {"in": 0, "out": 0})
        
        return self._calculate_metrics(
            channel_id=channel_id,
            peer_id=peer_id,
            sats_in=channel_flow.get("in", 0),
            sats_out=channel_flow.get("out", 0),
            capacity=capacity
        )
    
    def _calculate_metrics(self, channel_id: str, peer_id: str,
                          sats_in: int, sats_out: int, capacity: int) -> FlowMetrics:
        """
        Calculate flow metrics and classify a channel.
        
        The FlowRatio formula:
        FlowRatio = (SatsOut - SatsIn) / Capacity
        
        Interpretation:
        - Positive ratio: More sats going out than coming in (SOURCE)
        - Negative ratio: More sats coming in than going out (SINK)
        - Near zero: Balanced flow
        
        Args:
            channel_id: Channel identifier
            peer_id: Peer node ID
            sats_in: Total sats routed in
            sats_out: Total sats routed out
            capacity: Channel capacity
            
        Returns:
            FlowMetrics with classification
        """
        # Calculate flow ratio
        if capacity > 0:
            flow_ratio = (sats_out - sats_in) / capacity
        else:
            flow_ratio = 0.0
        
        # Classify based on thresholds
        if sats_in == 0 and sats_out == 0:
            state = ChannelState.UNKNOWN
        elif flow_ratio > self.config.source_threshold:
            state = ChannelState.SOURCE
        elif flow_ratio < self.config.sink_threshold:
            state = ChannelState.SINK
        else:
            state = ChannelState.BALANCED
        
        # Calculate daily volume
        total_volume = sats_in + sats_out
        daily_volume = total_volume // max(self.config.flow_window_days, 1)
        
        return FlowMetrics(
            channel_id=channel_id,
            peer_id=peer_id,
            sats_in=sats_in,
            sats_out=sats_out,
            capacity=capacity,
            flow_ratio=flow_ratio,
            state=state,
            daily_volume=daily_volume,
            analysis_window_days=self.config.flow_window_days
        )
    
    def _get_channels(self) -> List[Dict[str, Any]]:
        """Get list of all channels from lightningd."""
        try:
            result = self.plugin.rpc.listpeerchannels()
            channels = []
            
            # listpeerchannels returns channels grouped by peer
            for channel_info in result.get("channels", []):
                if channel_info.get("state") == "CHANNELD_NORMAL":
                    channels.append(channel_info)
            
            return channels
        except RpcError as e:
            self.plugin.log(f"Error getting channels: {e}", level='error')
            return []
    
    def _get_channel(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get info for a specific channel."""
        channels = self._get_channels()
        for channel in channels:
            scid = channel.get("short_channel_id") or channel.get("channel_id")
            if scid == channel_id:
                return channel
        return None
    
    def _get_flow_from_bookkeeper(self, channel_id: Optional[str] = None) -> Dict[str, Dict[str, int]]:
        """
        Get flow data from the bookkeeper plugin.
        
        Bookkeeper provides accounting-grade data for channel flows.
        We query for income events over the analysis window.
        
        Args:
            channel_id: Optional specific channel to query
            
        Returns:
            Dict mapping channel_id to {"in": sats_in, "out": sats_out}
        """
        flow_data = {}
        
        # Calculate time window
        window_seconds = self.config.flow_window_days * 86400
        start_time = int(time.time()) - window_seconds
        
        try:
            # Get income from bookkeeper
            # bkpr-listincome shows routing fees earned
            income = self.plugin.rpc.call("bkpr-listincome", {
                "consolidate_fees": False,
                "start_time": start_time
            })
            
            # Process income events to extract flow
            for event in income.get("income_events", []):
                event_channel = event.get("account", "")
                
                # Skip if looking for specific channel and this isn't it
                if channel_id and event_channel != channel_id:
                    continue
                
                # Initialize if needed
                if event_channel not in flow_data:
                    flow_data[event_channel] = {"in": 0, "out": 0}
                
                # Parse the event type and amount
                tag = event.get("tag", "")
                credit_msat = event.get("credit_msat", 0)
                debit_msat = event.get("debit_msat", 0)
                
                # Routing income indicates flow through the channel
                if tag == "routed":
                    # Credit = received fee = sats passed through
                    if credit_msat > 0:
                        flow_data[event_channel]["out"] += credit_msat // 1000
                
            # Also get actual forwards for volume data
            # bkpr-listforwards gives us the actual routing volume
            try:
                forwards = self.plugin.rpc.call("bkpr-listforwards")
                
                for fwd in forwards.get("forwards", []):
                    in_channel = fwd.get("in_channel", "")
                    out_channel = fwd.get("out_channel", "")
                    in_msat = fwd.get("in_msat", 0)
                    out_msat = fwd.get("out_msat", 0)
                    
                    # Filter by time if available
                    resolved_time = fwd.get("resolved_time", 0)
                    if resolved_time and resolved_time < start_time:
                        continue
                    
                    # Track inflow (received from peer)
                    if in_channel:
                        if in_channel not in flow_data:
                            flow_data[in_channel] = {"in": 0, "out": 0}
                        flow_data[in_channel]["in"] += in_msat // 1000
                    
                    # Track outflow (sent to peer)
                    if out_channel:
                        if out_channel not in flow_data:
                            flow_data[out_channel] = {"in": 0, "out": 0}
                        flow_data[out_channel]["out"] += out_msat // 1000
                        
            except RpcError:
                # bkpr-listforwards might not be available
                pass
                
        except RpcError as e:
            self.plugin.log(f"Error querying bookkeeper: {e}", level='warning')
            # Fall back to listforwards
            return self._get_flow_from_listforwards(channel_id)
        
        return flow_data
    
    def _get_flow_from_listforwards(self, channel_id: Optional[str] = None) -> Dict[str, Dict[str, int]]:
        """
        Get flow data from listforwards (fallback method).
        
        listforwards provides basic forward history when bookkeeper
        is not available.
        
        Args:
            channel_id: Optional specific channel to query
            
        Returns:
            Dict mapping channel_id to {"in": sats_in, "out": sats_out}
        """
        flow_data = {}
        
        # Calculate time window
        window_seconds = self.config.flow_window_days * 86400
        start_time = int(time.time()) - window_seconds
        
        try:
            # Query listforwards
            params = {"status": "settled"}
            if channel_id:
                # Try both in_channel and out_channel queries
                pass  # listforwards doesn't filter well, we'll filter in code
            
            result = self.plugin.rpc.listforwards(**params)
            
            for forward in result.get("forwards", []):
                # Check time window
                received_time = forward.get("received_time", 0)
                if received_time < start_time:
                    continue
                
                in_channel = forward.get("in_channel", "")
                out_channel = forward.get("out_channel", "")
                in_msat = forward.get("in_msat", 0)
                out_msat = forward.get("out_msat", 0)
                
                # Skip if looking for specific channel and neither matches
                if channel_id and in_channel != channel_id and out_channel != channel_id:
                    continue
                
                # Track inflow to in_channel
                if in_channel:
                    if in_channel not in flow_data:
                        flow_data[in_channel] = {"in": 0, "out": 0}
                    flow_data[in_channel]["in"] += in_msat // 1000
                
                # Track outflow from out_channel
                if out_channel:
                    if out_channel not in flow_data:
                        flow_data[out_channel] = {"in": 0, "out": 0}
                    flow_data[out_channel]["out"] += out_msat // 1000
                    
        except RpcError as e:
            self.plugin.log(f"Error querying listforwards: {e}", level='error')
        
        return flow_data
    
    def get_channel_state(self, channel_id: str) -> ChannelState:
        """
        Get the cached state of a channel.
        
        Args:
            channel_id: The channel to check
            
        Returns:
            The channel's current state classification
        """
        state_data = self.database.get_channel_state(channel_id)
        if state_data:
            return ChannelState(state_data.get("state", "unknown"))
        return ChannelState.UNKNOWN
    
    def get_sources(self) -> List[Dict[str, Any]]:
        """Get all channels classified as SOURCE (draining)."""
        return self.database.get_channels_by_state("source")
    
    def get_sinks(self) -> List[Dict[str, Any]]:
        """Get all channels classified as SINK (filling)."""
        return self.database.get_channels_by_state("sink")
    
    def get_balanced(self) -> List[Dict[str, Any]]:
        """Get all channels classified as BALANCED."""
        return self.database.get_channels_by_state("balanced")
