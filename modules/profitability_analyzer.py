"""
Channel Profitability Analyzer Module

This module tracks costs and revenue for each channel to determine profitability.
It provides actionable data for fee setting and rebalancing decisions.

Key metrics tracked:
- Channel open cost (on-chain fees)
- Rebalance costs (fees paid to acquire liquidity)
- Routing revenue (fees earned from forwarding)
- Volume routed (total sats forwarded)

Classifications:
- PROFITABLE: ROI > 0, earning more than costs
- BREAK_EVEN: ROI ~0, covering costs but not much profit
- UNDERWATER: ROI < 0, losing money
- ZOMBIE: Underwater + low volume, should consider closing
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple, TYPE_CHECKING
from enum import Enum
import time

from pyln.client import Plugin

if TYPE_CHECKING:
    from .metrics import PrometheusExporter


class ProfitabilityClass(Enum):
    """Channel profitability classification."""
    PROFITABLE = "profitable"      # ROI > 10%
    BREAK_EVEN = "break_even"      # ROI between -10% and 10%
    UNDERWATER = "underwater"      # ROI < -10%
    ZOMBIE = "zombie"              # Underwater + low volume for 30+ days


@dataclass
class ChannelCosts:
    """
    Cost tracking for a channel.
    
    Attributes:
        channel_id: Channel short ID
        peer_id: Peer node ID
        open_cost_sats: On-chain fees paid to open
        rebalance_cost_sats: Total fees paid for rebalancing
        total_cost_sats: Sum of all costs
    """
    channel_id: str
    peer_id: str
    open_cost_sats: int
    rebalance_cost_sats: int
    
    @property
    def total_cost_sats(self) -> int:
        return self.open_cost_sats + self.rebalance_cost_sats


@dataclass
class ChannelRevenue:
    """
    Revenue tracking for a channel.
    
    Attributes:
        channel_id: Channel short ID
        fees_earned_sats: Total routing fees earned
        volume_routed_sats: Total sats forwarded through channel
        forward_count: Number of successful forwards
    """
    channel_id: str
    fees_earned_sats: int
    volume_routed_sats: int
    forward_count: int


@dataclass 
class ChannelProfitability:
    """
    Complete profitability analysis for a channel.
    
    Attributes:
        channel_id: Channel short ID
        peer_id: Peer node ID
        capacity_sats: Channel capacity
        costs: Cost breakdown
        revenue: Revenue breakdown
        net_profit_sats: Revenue - Costs (Total/Accounting view)
        roi_percent: Return on investment percentage (Total ROI - includes open cost)
        classification: Profitability class
        cost_per_sat_routed: Average cost per sat of volume
        fee_per_sat_routed: Average fee earned per sat of volume
        days_open: How long the channel has been open
        last_routed: Timestamp of last routing activity
    
    Important Distinction:
        - roi_percent (Total ROI): Accounting view including sunk costs (open_cost_sats)
        - marginal_roi: Operational view - only considers ongoing costs (rebalance_costs)
        
    The marginal_roi is what matters for operational decisions:
    A channel that is covering its rebalancing costs is operationally profitable,
    even if it hasn't "paid back" the initial opening cost (sunk cost fallacy).
    """
    channel_id: str
    peer_id: str
    capacity_sats: int
    costs: ChannelCosts
    revenue: ChannelRevenue
    net_profit_sats: int
    roi_percent: float
    classification: ProfitabilityClass
    cost_per_sat_routed: float
    fee_per_sat_routed: float
    days_open: int
    last_routed: Optional[int]
    
    @property
    def marginal_roi(self) -> float:
        """
        Calculate Marginal ROI (Operational profitability).
        
        This metric EXCLUDES open_cost_sats (sunk cost) and focuses only on
        operational profitability: are we covering our rebalancing costs?
        
        Formula: (fees_earned - rebalance_costs) / max(1, rebalance_costs)
        
        Returns:
            Marginal ROI as a decimal (e.g., 0.5 = 50% marginal return)
            Returns 1.0 if no rebalance costs and earning fees (infinitely profitable operationally)
            Returns 0.0 if no rebalance costs and no fees (neutral)
        """
        fees_earned = self.revenue.fees_earned_sats
        rebalance_costs = self.costs.rebalance_cost_sats
        
        # If no rebalancing has occurred, check if channel is earning
        if rebalance_costs == 0:
            # No operational costs - if earning anything, it's pure profit
            return 1.0 if fees_earned > 0 else 0.0
        
        # Marginal profit = fees earned minus rebalancing costs (NO open cost!)
        marginal_profit = fees_earned - rebalance_costs
        
        # ROI relative to rebalancing investment
        return marginal_profit / max(1, rebalance_costs)
    
    @property
    def marginal_roi_percent(self) -> float:
        """Marginal ROI as a percentage."""
        return self.marginal_roi * 100
    
    @property
    def is_operationally_profitable(self) -> bool:
        """
        Check if channel is operationally profitable (covering rebalance costs).
        
        This is the key metric for fee decisions - we should NOT penalize channels
        just because they haven't paid back their opening cost.
        """
        return self.marginal_roi >= 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "capacity_sats": self.capacity_sats,
            "open_cost_sats": self.costs.open_cost_sats,
            "rebalance_cost_sats": self.costs.rebalance_cost_sats,
            "total_cost_sats": self.costs.total_cost_sats,
            "fees_earned_sats": self.revenue.fees_earned_sats,
            "volume_routed_sats": self.revenue.volume_routed_sats,
            "forward_count": self.revenue.forward_count,
            "net_profit_sats": self.net_profit_sats,
            "roi_percent": round(self.roi_percent, 2),
            "marginal_roi_percent": round(self.marginal_roi_percent, 2),
            "is_operationally_profitable": self.is_operationally_profitable,
            "classification": self.classification.value,
            "cost_per_sat_routed": round(self.cost_per_sat_routed, 6),
            "fee_per_sat_routed": round(self.fee_per_sat_routed, 6),
            "days_open": self.days_open,
            "last_routed": self.last_routed
        }


class ChannelProfitabilityAnalyzer:
    """
    Analyzes channel profitability for informed fee and rebalancing decisions.
    
    This module:
    1. Tracks costs (open fees, rebalance fees)
    2. Tracks revenue (routing fees earned)
    3. Calculates ROI and classifies channels
    4. Provides multipliers for fee and rebalance decisions
    """
    
    # Classification thresholds
    PROFITABLE_ROI_THRESHOLD = 0.10    # > 10% ROI
    UNDERWATER_ROI_THRESHOLD = -0.10   # < -10% ROI
    ZOMBIE_DAYS_INACTIVE = 30          # No routing for 30 days
    ZOMBIE_MIN_LOSS_SATS = 1000        # Minimum loss to be zombie
    
    def __init__(self, plugin: Plugin, config, database,
                 metrics_exporter: Optional["PrometheusExporter"] = None):
        """
        Initialize the profitability analyzer.
        
        Args:
            plugin: Reference to the pyln Plugin
            config: Configuration object
            database: Database instance for persistence
            metrics_exporter: Optional Prometheus metrics exporter for observability
        """
        self.plugin = plugin
        self.config = config
        self.database = database
        self.metrics = metrics_exporter
        
        # Cache for profitability data (refreshed periodically)
        self._profitability_cache: Dict[str, ChannelProfitability] = {}
        self._cache_timestamp: int = 0
        self._cache_ttl: int = 300  # 5 minutes
    
    def analyze_all_channels(self) -> Dict[str, ChannelProfitability]:
        """
        Analyze profitability for all channels.
        
        Returns:
            Dict mapping channel_id to ChannelProfitability
        """
        results = {}
        
        try:
            # Get all channels
            channels = self._get_all_channels()
            
            for channel_id, channel_info in channels.items():
                profitability = self.analyze_channel(channel_id, channel_info)
                if profitability:
                    results[channel_id] = profitability
            
            # Update cache
            self._profitability_cache = results
            self._cache_timestamp = int(time.time())
            
            # Log summary
            classifications = {}
            for p in results.values():
                cls = p.classification.value
                classifications[cls] = classifications.get(cls, 0) + 1
            
            self.plugin.log(
                f"Profitability analysis complete: {len(results)} channels - "
                f"{classifications}"
            )
            
        except Exception as e:
            self.plugin.log(f"Error in profitability analysis: {e}", level='error')
        
        return results
    
    def analyze_channel(self, channel_id: str, 
                       channel_info: Optional[Dict] = None) -> Optional[ChannelProfitability]:
        """
        Analyze profitability for a single channel.
        
        Args:
            channel_id: Channel to analyze
            channel_info: Optional channel info (fetched if not provided)
            
        Returns:
            ChannelProfitability object or None if analysis fails
        """
        try:
            # Get channel info if not provided
            if channel_info is None:
                channels = self._get_all_channels()
                channel_info = channels.get(channel_id)
                if not channel_info:
                    return None
            
            peer_id = channel_info.get("peer_id", "")
            capacity = channel_info.get("capacity", 0)
            funding_txid = channel_info.get("funding_txid", "")
            
            # Get costs from database
            costs = self._get_channel_costs(channel_id, peer_id, funding_txid, capacity)
            
            # Get revenue from routing history
            revenue = self._get_channel_revenue(channel_id)
            
            # Calculate metrics
            net_profit = revenue.fees_earned_sats - costs.total_cost_sats
            
            # ROI based on total investment (costs)
            if costs.total_cost_sats > 0:
                roi = net_profit / costs.total_cost_sats
            else:
                # No costs recorded - consider profitable if earning
                roi = 1.0 if revenue.fees_earned_sats > 0 else 0.0
            
            # Cost/fee per sat routed
            if revenue.volume_routed_sats > 0:
                cost_per_sat = costs.total_cost_sats / revenue.volume_routed_sats
                fee_per_sat = revenue.fees_earned_sats / revenue.volume_routed_sats
            else:
                cost_per_sat = 0.0
                fee_per_sat = 0.0
            
            # Days open
            open_timestamp = channel_info.get("open_timestamp", int(time.time()))
            days_open = (int(time.time()) - open_timestamp) // 86400
            
            # Last routing activity
            last_routed = self._get_last_routing_time(channel_id)
            
            # Classify
            classification = self._classify_channel(
                roi, net_profit, last_routed, days_open
            )
            
            profitability = ChannelProfitability(
                channel_id=channel_id,
                peer_id=peer_id,
                capacity_sats=capacity,
                costs=costs,
                revenue=revenue,
                net_profit_sats=net_profit,
                roi_percent=roi * 100,
                classification=classification,
                cost_per_sat_routed=cost_per_sat,
                fee_per_sat_routed=fee_per_sat,
                days_open=days_open,
                last_routed=last_routed
            )
            
            # Export metrics (Phase 2: Observability)
            if self.metrics:
                from .metrics import MetricNames, METRIC_HELP
                
                labels = {"channel_id": channel_id, "peer_id": peer_id}
                
                # Gauge: Marginal ROI percentage (operational profitability)
                self.metrics.set_gauge(
                    MetricNames.CHANNEL_MARGINAL_ROI_PERCENT,
                    profitability.marginal_roi_percent,
                    labels,
                    METRIC_HELP.get(MetricNames.CHANNEL_MARGINAL_ROI_PERCENT, "")
                )
                
                # Gauge: Channel capacity
                self.metrics.set_gauge(
                    MetricNames.CHANNEL_CAPACITY_SATS,
                    capacity,
                    labels,
                    METRIC_HELP.get(MetricNames.CHANNEL_CAPACITY_SATS, "")
                )
            
            return profitability
            
        except Exception as e:
            self.plugin.log(
                f"Error analyzing channel {channel_id}: {e}", 
                level='warn'
            )
            return None
    
    def get_profitability(self, channel_id: str) -> Optional[ChannelProfitability]:
        """
        Get profitability data for a channel (uses cache if fresh).
        
        Args:
            channel_id: Channel to look up
            
        Returns:
            ChannelProfitability or None
        """
        # Check cache freshness
        if (int(time.time()) - self._cache_timestamp) > self._cache_ttl:
            self.analyze_all_channels()
        
        return self._profitability_cache.get(channel_id)
    
    def get_fee_multiplier(self, channel_id: str) -> float:
        """
        Get fee multiplier based on channel's MARGINAL (operational) profitability.
        
        CRITICAL: Uses marginal_roi, NOT total ROI.
        
        This avoids the SUNK COST FALLACY:
        - A channel should NOT be penalized with high fees just because
          it had a high opening cost that hasn't been recovered yet.
        - What matters operationally is: Is this channel covering its
          ONGOING costs (rebalancing) with its fee revenue?
        
        If a channel is operationally profitable (marginal_roi >= 0),
        it's working well and should keep competitive fees to maintain volume.
        
        Args:
            channel_id: Channel to get multiplier for
            
        Returns:
            Fee multiplier (1.0 = no change)
        """
        profitability = self.get_profitability(channel_id)
        
        if not profitability:
            return 1.0  # No data, no adjustment
        
        # Use MARGINAL ROI for operational decisions
        # This ignores sunk costs (open_cost_sats)
        marginal_roi = profitability.marginal_roi
        
        # Fee multipliers based on operational profitability
        if marginal_roi > 0.20:  # > 20% marginal return
            # Highly profitable operationally - keep fees competitive
            return 0.95
        elif marginal_roi >= 0:  # Breaking even or better on operations
            # Covering costs - no change needed
            return 1.0
        elif marginal_roi >= -0.20:  # -20% to 0 marginal return
            # Slight operational loss - modest fee increase
            return 1.05
        elif marginal_roi >= -0.50:  # -50% to -20% marginal return
            # Significant operational loss - larger fee increase
            return 1.10
        else:  # < -50% marginal return
            # Severe operational loss - check if zombie
            if profitability.classification == ProfitabilityClass.ZOMBIE:
                return 1.0  # Don't bother adjusting zombies, flag for closure
            return 1.15  # Try to recover operational costs
    
    def get_marginal_roi(self, channel_id: str) -> Optional[float]:
        """
        Get the marginal ROI for a channel.
        
        This is the operational profitability metric that excludes sunk costs.
        
        Args:
            channel_id: Channel to get marginal ROI for
            
        Returns:
            Marginal ROI as decimal, or None if no data
        """
        profitability = self.get_profitability(channel_id)
        if not profitability:
            return None
        return profitability.marginal_roi
    
    def get_rebalance_priority(self, channel_id: str) -> float:
        """
        Get rebalance priority multiplier based on profitability.
        
        Higher priority = more worth rebalancing.
        
        Args:
            channel_id: Channel to get priority for
            
        Returns:
            Priority multiplier (1.0 = normal, >1 = higher, <1 = lower)
        """
        profitability = self.get_profitability(channel_id)
        
        if not profitability:
            return 1.0  # No data, normal priority
        
        # Priority based on classification
        priorities = {
            ProfitabilityClass.PROFITABLE: 1.5,     # High priority - proven earner
            ProfitabilityClass.BREAK_EVEN: 1.0,     # Normal priority
            ProfitabilityClass.UNDERWATER: 0.5,     # Low priority - not worth much
            ProfitabilityClass.ZOMBIE: 0.0,         # Skip entirely - don't waste fees
        }
        
        return priorities.get(profitability.classification, 1.0)
    
    def get_max_rebalance_fee_multiplier(self, channel_id: str) -> float:
        """
        Get multiplier for maximum rebalance fee budget.
        
        Profitable channels are worth paying more to keep full.
        
        Args:
            channel_id: Channel to get multiplier for
            
        Returns:
            Budget multiplier (1.0 = normal, >1 = pay more, <1 = pay less)
        """
        profitability = self.get_profitability(channel_id)
        
        if not profitability:
            return 1.0
        
        # Budget based on ROI - pay up to what the channel has proven to earn
        if profitability.classification == ProfitabilityClass.PROFITABLE:
            # Pay more for proven earners - up to 1.5x normal budget
            return min(1.5, 1.0 + (profitability.roi_percent / 100))
        elif profitability.classification == ProfitabilityClass.BREAK_EVEN:
            return 1.0
        elif profitability.classification == ProfitabilityClass.UNDERWATER:
            return 0.5  # Half budget - already losing money
        else:  # ZOMBIE
            return 0.0  # No budget - don't rebalance
    
    def should_rebalance(self, channel_id: str) -> Tuple[bool, str]:
        """
        Determine if a channel should be rebalanced based on profitability.
        
        Args:
            channel_id: Channel to check
            
        Returns:
            Tuple of (should_rebalance, reason)
        """
        profitability = self.get_profitability(channel_id)
        
        if not profitability:
            return True, "no_profitability_data"
        
        if profitability.classification == ProfitabilityClass.ZOMBIE:
            return False, f"zombie_channel (ROI={profitability.roi_percent:.1f}%, inactive {self._days_since_routed(profitability)}+ days)"
        
        if profitability.classification == ProfitabilityClass.UNDERWATER:
            # Allow rebalancing but log warning
            return True, f"underwater_channel (ROI={profitability.roi_percent:.1f}%) - consider if worth it"
        
        return True, f"{profitability.classification.value} (ROI={profitability.roi_percent:.1f}%)"
    
    def record_rebalance_cost(self, channel_id: str, peer_id: str, 
                              cost_sats: int, amount_sats: int):
        """
        Record a rebalance cost for a channel.
        
        Called after a successful rebalance to track costs.
        
        Args:
            channel_id: Channel that was rebalanced into
            peer_id: Peer node ID
            cost_sats: Fee paid for the rebalance
            amount_sats: Amount rebalanced
        """
        self.database.record_rebalance_cost(
            channel_id=channel_id,
            peer_id=peer_id,
            cost_sats=cost_sats,
            amount_sats=amount_sats,
            timestamp=int(time.time())
        )
        
        # Invalidate cache for this channel
        if channel_id in self._profitability_cache:
            del self._profitability_cache[channel_id]
    
    def record_channel_open_cost(self, channel_id: str, peer_id: str,
                                  open_cost_sats: int, capacity_sats: int):
        """
        Record the cost to open a channel.
        
        Args:
            channel_id: New channel ID
            peer_id: Peer node ID
            open_cost_sats: On-chain fees paid
            capacity_sats: Channel capacity
        """
        self.database.record_channel_open_cost(
            channel_id=channel_id,
            peer_id=peer_id,
            open_cost_sats=open_cost_sats,
            capacity_sats=capacity_sats,
            timestamp=int(time.time())
        )
    
    def get_zombie_channels(self) -> List[ChannelProfitability]:
        """
        Get list of zombie channels that should be considered for closure.
        
        Returns:
            List of ChannelProfitability for zombie channels
        """
        if (int(time.time()) - self._cache_timestamp) > self._cache_ttl:
            self.analyze_all_channels()
        
        return [
            p for p in self._profitability_cache.values()
            if p.classification == ProfitabilityClass.ZOMBIE
        ]
    
    def get_profitable_channels(self) -> List[ChannelProfitability]:
        """
        Get list of profitable channels (for prioritization).
        
        Returns:
            List of ChannelProfitability for profitable channels, sorted by ROI
        """
        if (int(time.time()) - self._cache_timestamp) > self._cache_ttl:
            self.analyze_all_channels()
        
        profitable = [
            p for p in self._profitability_cache.values()
            if p.classification == ProfitabilityClass.PROFITABLE
        ]
        
        return sorted(profitable, key=lambda p: p.roi_percent, reverse=True)
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get summary statistics for all channels.
        
        Returns:
            Summary dict with totals and breakdowns
        """
        if (int(time.time()) - self._cache_timestamp) > self._cache_ttl:
            self.analyze_all_channels()
        
        total_costs = 0
        total_revenue = 0
        total_volume = 0
        classifications = {}
        
        for p in self._profitability_cache.values():
            total_costs += p.costs.total_cost_sats
            total_revenue += p.revenue.fees_earned_sats
            total_volume += p.revenue.volume_routed_sats
            
            cls = p.classification.value
            classifications[cls] = classifications.get(cls, 0) + 1
        
        net_profit = total_revenue - total_costs
        overall_roi = (net_profit / total_costs * 100) if total_costs > 0 else 0
        
        return {
            "total_channels": len(self._profitability_cache),
            "total_cost_sats": total_costs,
            "total_revenue_sats": total_revenue,
            "net_profit_sats": net_profit,
            "overall_roi_percent": round(overall_roi, 2),
            "total_volume_routed_sats": total_volume,
            "classifications": classifications,
            "zombie_channels": len(self.get_zombie_channels()),
            "cache_age_seconds": int(time.time()) - self._cache_timestamp
        }
    
    # =========================================================================
    # Private Helper Methods
    # =========================================================================
    
    def _get_all_channels(self) -> Dict[str, Dict[str, Any]]:
        """Get all channels with their info."""
        channels = {}
        
        try:
            result = self.plugin.rpc.listpeerchannels()
            
            for channel in result.get("channels", []):
                state = channel.get("state", "")
                if state != "CHANNELD_NORMAL":
                    continue
                
                channel_id = channel.get("short_channel_id")
                if not channel_id:
                    continue
                
                # Calculate capacity
                capacity = channel.get("total_msat", 0)
                if isinstance(capacity, str):
                    capacity = int(capacity.replace("msat", ""))
                capacity = capacity // 1000  # Convert to sats
                
                # If capacity is 0, try spendable + receivable
                if capacity == 0:
                    spendable = channel.get("spendable_msat", 0)
                    receivable = channel.get("receivable_msat", 0)
                    if isinstance(spendable, str):
                        spendable = int(spendable.replace("msat", ""))
                    if isinstance(receivable, str):
                        receivable = int(receivable.replace("msat", ""))
                    capacity = (spendable + receivable) // 1000
                
                funding_txid = channel.get("funding_txid", "")
                
                # Get open timestamp from bookkeeper or estimate from SCID
                open_timestamp = self._get_channel_open_timestamp(
                    channel_id, funding_txid
                )
                
                channels[channel_id] = {
                    "peer_id": channel.get("peer_id", ""),
                    "capacity": capacity,
                    "funding_txid": funding_txid,
                    "open_timestamp": open_timestamp
                }
                
        except Exception as e:
            self.plugin.log(f"Error getting channels: {e}", level='error')
        
        return channels
    
    def _get_channel_open_timestamp(self, channel_id: str, funding_txid: str) -> int:
        """
        Get the timestamp when a channel was opened.
        
        Methods (in priority order):
        1. Bookkeeper channel_open event - has exact timestamp
        2. Estimate from SCID block height - approximate but reliable
        3. Fallback to 30 days ago
        
        Args:
            channel_id: Short channel ID (e.g., "902205x123x0")
            funding_txid: Funding transaction ID
            
        Returns:
            Unix timestamp of channel open
        """
        # Method 1: Query bookkeeper for channel_open event
        if funding_txid:
            bkpr_timestamp = self._get_open_timestamp_from_bookkeeper(funding_txid)
            if bkpr_timestamp:
                return bkpr_timestamp
        
        # Method 2: Estimate from SCID block height
        # SCID format is "blockheight x txindex x output"
        if channel_id and 'x' in channel_id:
            try:
                block_height = int(channel_id.split('x')[0])
                # Estimate: ~10 minutes per block, blocks since genesis
                # Bitcoin mainnet started ~Jan 3, 2009
                # Block 0 = 1231006505
                genesis_timestamp = 1231006505
                seconds_per_block = 600  # 10 minutes average
                estimated_timestamp = genesis_timestamp + (block_height * seconds_per_block)
                
                # Sanity check - should be in the past
                now = int(time.time())
                if estimated_timestamp < now:
                    return estimated_timestamp
                    
            except (ValueError, IndexError):
                pass
        
        # Method 3: Fallback to 30 days ago
        return int(time.time()) - (86400 * 30)
    
    def _get_open_timestamp_from_bookkeeper(self, funding_txid: str) -> Optional[int]:
        """
        Get channel open timestamp from bookkeeper.
        
        Bookkeeper records channel_open events with the exact timestamp.
        
        Args:
            funding_txid: The funding transaction ID
            
        Returns:
            Unix timestamp, or None if not found
        """
        try:
            # Bookkeeper account names use reversed txid bytes
            reversed_txid = self._reverse_txid(funding_txid)
            
            # Query bookkeeper for this account's events
            result = self.plugin.rpc.call(
                "bkpr-listaccountevents",
                {"account": reversed_txid}
            )
            
            events = result.get("events", [])
            
            # Look for channel_open event
            for event in events:
                if (event.get("type") == "chain" and 
                    event.get("tag") == "channel_open"):
                    timestamp = event.get("timestamp")
                    if timestamp:
                        return int(timestamp)
                        
        except Exception as e:
            self.plugin.log(
                f"Error getting open timestamp from bookkeeper: {e}",
                level='debug'
            )
        
        return None
    
    def _get_channel_costs(self, channel_id: str, peer_id: str, 
                          funding_txid: str, capacity_sats: int = 0) -> ChannelCosts:
        """
        Get costs for a channel from bookkeeper and database.
        
        Cost sources (in priority order):
        1. Bookkeeper onchain_fee events for the funding tx (most accurate)
        2. Database cached value (from previous lookup or manual entry)
        3. Config estimated_open_cost_sats (fallback)
        
        IMPORTANT: Includes sanity check and self-healing mechanism to detect
        and correct cases where principal capital (funding amount) was incorrectly
        recorded as an expense (open cost).
        """
        # Get rebalance costs - combine database records with bookkeeper data
        db_rebalance_costs = self.database.get_channel_rebalance_costs(channel_id)
        bkpr_rebalance_costs = self._get_rebalance_costs_from_bookkeeper(channel_id, funding_txid)
        
        # Use the higher value (bookkeeper may have more complete history)
        rebalance_costs = max(db_rebalance_costs, bkpr_rebalance_costs)
        
        # Try to get open cost from database cache first
        open_cost = self.database.get_channel_open_cost(channel_id)
        
        # SANITY CHECK: Detect invalid open_cost (capital mistaken as expense)
        # This triggers self-healing for existing bad data in the database
        if open_cost is not None and capacity_sats > 0:
            open_cost = self._sanity_check_open_cost(
                channel_id, peer_id, funding_txid, open_cost, capacity_sats
            )
        
        if open_cost is None and funding_txid:
            # Query bookkeeper for actual on-chain fee
            open_cost = self._get_open_cost_from_bookkeeper(funding_txid, capacity_sats)
            
            # Cache it in database for future lookups
            if open_cost is not None:
                self.database.record_channel_open_cost(
                    channel_id, peer_id, open_cost, capacity_sats
                )
        
        if open_cost is None:
            # Final fallback: use estimated cost from config
            open_cost = self.config.estimated_open_cost_sats
            self.plugin.log(
                f"Using estimated open cost ({open_cost} sats) for {channel_id} - "
                f"bookkeeper data not available",
                level='debug'
            )
        
        return ChannelCosts(
            channel_id=channel_id,
            peer_id=peer_id,
            open_cost_sats=open_cost,
            rebalance_cost_sats=rebalance_costs
        )
    
    def _sanity_check_open_cost(self, channel_id: str, peer_id: str,
                                 funding_txid: str, open_cost: int,
                                 capacity_sats: int) -> int:
        """
        Sanity check and self-heal invalid open_cost values.
        
        This detects cases where the channel funding amount (principal capital)
        was incorrectly recorded as an expense (open cost), causing healthy
        channels to appear as massive losses.
        
        Detection criteria:
        - open_cost >= capacity_sats (impossible - fee can't exceed channel size)
        - open_cost >= 90% of capacity (almost certainly the funding amount)
        
        Args:
            channel_id: Channel short ID
            peer_id: Peer node ID
            funding_txid: Funding transaction ID
            open_cost: The open cost value to validate
            capacity_sats: Channel capacity in sats
            
        Returns:
            Corrected open_cost value (either validated original, re-queried, or fallback)
        """
        # Only flag as invalid if open_cost is >= 90% of capacity
        # This is a clear indicator that the funding amount was recorded as fee
        # Normal on-chain fees are typically 0.1-5% of channel size at most
        is_invalid = open_cost >= (capacity_sats * 0.90)
        
        if not is_invalid:
            return open_cost  # Value looks reasonable
        
        # DETECTED INVALID OPEN COST - trigger self-healing
        self.plugin.log(
            f"SANITY CHECK: Detected invalid open_cost for {channel_id}: "
            f"{open_cost} sats (capacity: {capacity_sats} sats). "
            f"Capital likely counted as expense. Triggering recalculation.",
            level='warn'
        )
        
        # Step 1: Force re-query from bookkeeper
        corrected_cost = None
        if funding_txid:
            corrected_cost = self._get_open_cost_from_bookkeeper(funding_txid, capacity_sats)
        
        # Step 2: Validate the re-queried value - reject if still >= 90% of capacity
        if corrected_cost is not None and corrected_cost >= (capacity_sats * 0.90):
            self.plugin.log(
                f"Re-query still returned invalid value ({corrected_cost} sats). "
                f"Discarding and using fallback.",
                level='warn'
            )
            corrected_cost = None
        
        # Step 3: Use fallback if re-query failed or still invalid
        if corrected_cost is None:
            corrected_cost = self.config.estimated_open_cost_sats
            self.plugin.log(
                f"Using fallback estimated_open_cost_sats: {corrected_cost} sats",
                level='info'
            )
        else:
            self.plugin.log(
                f"Corrected open_cost for {channel_id}: {corrected_cost} sats "
                f"(was: {open_cost} sats)",
                level='info'
            )
        
        # Step 4: Update database with corrected value (self-healing)
        self.database.record_channel_open_cost(
            channel_id, peer_id, corrected_cost, capacity_sats
        )
        self.plugin.log(
            f"Database updated with corrected open_cost for {channel_id}",
            level='debug'
        )
        
        return corrected_cost
    
    def _get_open_cost_from_bookkeeper(self, funding_txid: str, 
                                        capacity_sats: int = 0) -> Optional[int]:
        """
        Query bookkeeper for actual on-chain fee paid for channel open.
        
        Bookkeeper tracks onchain_fee events per txid. For channel opens,
        the account is named by the funding txid (bytes reversed).
        
        HARDENED: Now validates that found fee is actually a fee and not
        the funding output amount (principal capital).
        
        Args:
            funding_txid: The funding transaction ID
            capacity_sats: Channel capacity in sats (for validation)
            
        Returns:
            On-chain fee in sats, or None if not found or invalid
        """
        try:
            # Bookkeeper account names use reversed txid bytes
            # e.g., txid 9e14b256... becomes account 940fec8a...
            reversed_txid = self._reverse_txid(funding_txid)
            
            # Query bookkeeper for this account's events
            result = self.plugin.rpc.call(
                "bkpr-listaccountevents",
                {"account": reversed_txid}
            )
            
            events = result.get("events", [])
            
            # Look for onchain_fee events with debit (fee paid)
            # The fee is the debit_msat on the channel's onchain_fee event
            for event in events:
                if (event.get("type") == "onchain_fee" and 
                    event.get("tag") == "onchain_fee" and
                    event.get("txid") == funding_txid):
                    
                    # Credit means fee was attributed TO this account
                    # This is the channel's share of the open fee
                    credit_msat = event.get("credit_msat", 0)
                    if credit_msat > 0:
                        fee_sats = credit_msat // 1000
                        
                        # HARDENING: Validate this is actually a fee, not the funding amount
                        if not self._is_valid_fee_amount(fee_sats, capacity_sats, funding_txid):
                            continue  # Skip this event, look for others
                        
                        self.plugin.log(
                            f"Found actual open cost for {funding_txid}: {fee_sats} sats",
                            level='debug'
                        )
                        return fee_sats
            
            # Alternative: check wallet account for the same txid
            # This catches cases where we opened the channel
            wallet_result = self.plugin.rpc.call(
                "bkpr-listaccountevents",
                {"account": "wallet"}
            )
            
            wallet_events = wallet_result.get("events", [])
            for event in wallet_events:
                if (event.get("type") == "onchain_fee" and
                    event.get("txid") == funding_txid):
                    
                    debit_msat = event.get("debit_msat", 0)
                    if debit_msat > 0:
                        fee_sats = debit_msat // 1000
                        
                        # HARDENING: Validate this is actually a fee, not the funding amount
                        if not self._is_valid_fee_amount(fee_sats, capacity_sats, funding_txid):
                            continue  # Skip this event, look for others
                        
                        self.plugin.log(
                            f"Found wallet open cost for {funding_txid}: {fee_sats} sats",
                            level='debug'
                        )
                        return fee_sats
                        
        except Exception as e:
            self.plugin.log(
                f"Error querying bookkeeper for {funding_txid}: {e}",
                level='debug'
            )
        
        return None
    
    def _is_valid_fee_amount(self, fee_sats: int, capacity_sats: int, 
                             funding_txid: str) -> bool:
        """
        Validate that a fee amount is actually a mining fee, not the funding output.
        
        This catches the bug where bookkeeper returns the channel capacity
        (funding output) instead of the actual on-chain mining fee.
        
        Args:
            fee_sats: The fee amount to validate
            capacity_sats: Channel capacity in sats
            funding_txid: Transaction ID (for logging)
            
        Returns:
            True if fee appears valid, False if it looks like funding amount
        """
        # If we don't have capacity info, accept any reasonable value
        if capacity_sats <= 0:
            return True
        
        # Reject if fee >= 90% of capacity - this is clearly the funding amount
        if fee_sats >= capacity_sats * 0.90:
            self.plugin.log(
                f"Rejecting invalid fee ({fee_sats} sats) for {funding_txid}: "
                f"equals/exceeds 90% of capacity ({capacity_sats} sats). "
                f"This is likely the funding output, not the mining fee.",
                level='debug'
            )
            return False
        
        return True
    
    def _get_rebalance_costs_from_bookkeeper(self, channel_id: str, funding_txid: Optional[str] = None) -> int:
        """
        Query bookkeeper for rebalance costs (circular payment fees).
        
        Circular rebalances show up in bookkeeper as:
        - 'invoice' events on the destination channel (we paid ourselves)
        - The fees_msat field shows what we paid in routing fees
        
        We look for invoice events where we paid to ourselves (circular)
        by checking if there's a matching credit on another of our channels.
        
        Args:
            channel_id: The channel to get rebalance costs for
            funding_txid: The funding transaction ID (optional, will look up if not provided)
            
        Returns:
            Total rebalance costs in sats
        """
        total_fees_sats = 0
        
        try:
            # Use provided funding_txid or look it up
            if not funding_txid:
                channels = self._get_all_channels()
                if channel_id not in channels:
                    return 0
                funding_txid = channels[channel_id].get("funding_txid", "")
            
            if not funding_txid:
                return 0
            
            reversed_txid = self._reverse_txid(funding_txid)
            
            # Query bookkeeper for this channel's events
            result = self.plugin.rpc.call(
                "bkpr-listaccountevents",
                {"account": reversed_txid}
            )
            
            events = result.get("events", [])
            
            # Look for invoice events with fees (payments we made)
            for event in events:
                if event.get("type") == "channel" and event.get("tag") == "invoice":
                    # Debit on invoice = we paid out (could be rebalance)
                    fees_msat = event.get("fees_msat", 0)
                    if fees_msat and fees_msat > 0:
                        # This is a fee we paid - likely a circular rebalance
                        total_fees_sats += fees_msat // 1000
            
            if total_fees_sats > 0:
                self.plugin.log(
                    f"Found {total_fees_sats} sats in rebalance costs from bookkeeper for {channel_id}",
                    level='debug'
                )
                        
        except Exception as e:
            self.plugin.log(
                f"Error getting rebalance costs from bookkeeper for {channel_id}: {e}",
                level='debug'
            )
        
        return total_fees_sats
    
    def _reverse_txid(self, txid: str) -> str:
        """
        Reverse a transaction ID (byte-swap).
        
        Bitcoin txids are displayed in reverse byte order.
        Bookkeeper uses the reversed form as account names.
        
        Args:
            txid: Transaction ID in standard display format
            
        Returns:
            Reversed txid (bytes swapped)
        """
        # Convert hex string to bytes, reverse, convert back
        try:
            txid_bytes = bytes.fromhex(txid)
            reversed_bytes = txid_bytes[::-1]
            return reversed_bytes.hex()
        except (ValueError, AttributeError):
            return txid
    
    def _get_channel_revenue(self, channel_id: str) -> ChannelRevenue:
        """Get revenue for a channel from routing history."""
        # Query listforwards for this channel's earnings
        fees_earned = 0
        volume_routed = 0
        forward_count = 0
        
        try:
            # Get forwards where we earned fees (out_channel = this channel)
            result = self.plugin.rpc.listforwards(
                out_channel=channel_id,
                status="settled"
            )
            
            for forward in result.get("forwards", []):
                fee_msat = forward.get("fee_msat", 0)
                if isinstance(fee_msat, str):
                    fee_msat = int(fee_msat.replace("msat", ""))
                fees_earned += fee_msat // 1000
                
                out_msat = forward.get("out_msat", 0)
                if isinstance(out_msat, str):
                    out_msat = int(out_msat.replace("msat", ""))
                volume_routed += out_msat // 1000
                
                forward_count += 1
                
        except Exception as e:
            self.plugin.log(
                f"Error getting revenue for {channel_id}: {e}", 
                level='warn'
            )
        
        return ChannelRevenue(
            channel_id=channel_id,
            fees_earned_sats=fees_earned,
            volume_routed_sats=volume_routed,
            forward_count=forward_count
        )
    
    def _get_last_routing_time(self, channel_id: str) -> Optional[int]:
        """Get timestamp of last routing activity on a channel."""
        try:
            result = self.plugin.rpc.listforwards(
                out_channel=channel_id,
                status="settled"
            )
            
            forwards = result.get("forwards", [])
            if not forwards:
                return None
            
            # Get most recent
            latest = max(forwards, key=lambda f: f.get("received_time", 0))
            return int(latest.get("received_time", 0))
            
        except Exception:
            return None
    
    def _classify_channel(self, roi: float, net_profit: int,
                         last_routed: Optional[int], days_open: int) -> ProfitabilityClass:
        """Classify a channel based on profitability metrics."""
        
        # Check for zombie first
        if last_routed:
            days_inactive = (int(time.time()) - last_routed) // 86400
        else:
            days_inactive = days_open  # Never routed
        
        is_zombie = (
            roi < self.UNDERWATER_ROI_THRESHOLD and
            days_inactive >= self.ZOMBIE_DAYS_INACTIVE and
            net_profit < -self.ZOMBIE_MIN_LOSS_SATS
        )
        
        if is_zombie:
            return ProfitabilityClass.ZOMBIE
        
        # Classify by ROI
        if roi > self.PROFITABLE_ROI_THRESHOLD:
            return ProfitabilityClass.PROFITABLE
        elif roi < self.UNDERWATER_ROI_THRESHOLD:
            return ProfitabilityClass.UNDERWATER
        else:
            return ProfitabilityClass.BREAK_EVEN
    
    def _days_since_routed(self, profitability: ChannelProfitability) -> int:
        """Calculate days since last routing activity."""
        if profitability.last_routed:
            return (int(time.time()) - profitability.last_routed) // 86400
        return profitability.days_open
