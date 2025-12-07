"""
EV-Based Rebalancer module for cl-revenue-ops

MODULE 3: EV-Based Rebalancing (Profit-Aware with Opportunity Cost)

This module implements Expected Value (EV) based rebalancing decisions.
Unlike clboss which often makes negative EV rebalances, this module only
triggers rebalances when the math shows positive expected profit.

Architecture Pattern: "Strategist and Driver"
- THIS MODULE (Strategist): Calculates EV, determines IF and HOW MUCH to rebalance
- CIRCULAR PLUGIN (Driver): Actually executes the circular payment

We call circular via RPC - we don't import it. This module is the "Business Logic"
layer that tells circular: "Move money from A to B if done below X ppm. Go."

Expected Value Theory for Rebalancing:
- Rebalancing moves liquidity from one channel to another
- It costs fees to move the liquidity
- It earns potential future routing fees from the refilled channel
- BUT: Draining the source channel has an OPPORTUNITY COST
- EV = Expected_Future_Fees - Rebalancing_Cost - Opportunity_Cost

NEW: Dynamic Utilization & Opportunity Cost:
1. OLD: Static 10% utilization estimate (unrealistic)
2. NEW: Dynamic turnover_rate = daily_volume / capacity
   - Uses actual channel performance to project revenue
   
3. OLD Spread: Dest_Fee - Inbound_Rebalance_Cost
4. NEW Spread: Dest_Fee - (Inbound_Rebalance_Cost + Source_Fee)
   - We're "selling" liquidity from Source to "buy" liquidity on Dest
   - If Source earns 500ppm and Dest earns 600ppm with 200ppm rebalance cost,
     that's actually a NET LOSS (600 - 200 - 500 = -100ppm)

CRITICAL FLOW STATE LOGIC:
- If Target Channel is SINK: ABORT rebalance!
  (Why pay fees to fill a channel that fills itself for free via routing?)
- If Target Channel is SOURCE: HIGH PRIORITY rebalance!
  (This channel prints money; keep it full)

Anti-Thrashing Protection:
- After successful rebalance, unmanage peer from clboss for extended duration
- This prevents clboss from immediately "fixing" our work and wasting fees

The key insight: We should never pay more to rebalance than we expect
to earn from the restored capacity MINUS the opportunity cost of the
liquidity we're draining from the source channel.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING

from pyln.client import Plugin, RpcError

from .config import Config
from .database import Database
from .clboss_manager import ClbossManager, ClbossTags

if TYPE_CHECKING:
    from .profitability_analyzer import ChannelProfitabilityAnalyzer


@dataclass
class RebalanceCandidate:
    """
    A candidate for rebalancing.
    
    Attributes:
        from_channel: Channel with excess liquidity (source)
        to_channel: Channel needing liquidity (destination)
        from_peer_id: Peer ID of source channel
        to_peer_id: Peer ID of destination channel
        amount_sats: Amount to rebalance
        amount_msat: Amount in millisatoshis (for RPC calls)
        outbound_fee_ppm: Fee we charge on destination channel
        inbound_fee_ppm: Estimated fee to route to us
        source_fee_ppm: Fee we charge on source channel (raw opportunity cost)
        weighted_opp_cost_ppm: Opportunity cost weighted by source turnover (7-day projection)
        spread_ppm: Net spread after weighted opportunity cost
        max_budget_sats: Maximum fee we should pay
        max_budget_msat: Maximum fee in msat (for RPC calls)
        max_fee_ppm: Maximum fee as PPM (for circular)
        expected_profit_sats: Expected profit if rebalanced
        liquidity_ratio: Current outbound ratio on destination
        dest_flow_state: Flow state of destination (SOURCE/SINK/BALANCED)
        dest_turnover_rate: Daily volume / capacity for destination
        source_turnover_rate: Daily volume / capacity for source
    """
    from_channel: str
    to_channel: str
    from_peer_id: str
    to_peer_id: str
    amount_sats: int
    amount_msat: int
    outbound_fee_ppm: int
    inbound_fee_ppm: int
    source_fee_ppm: int
    weighted_opp_cost_ppm: int  # NEW: Weighted opportunity cost (source_fee * turnover_weight)
    spread_ppm: int
    max_budget_sats: int
    max_budget_msat: int
    max_fee_ppm: int
    expected_profit_sats: int
    liquidity_ratio: float
    dest_flow_state: str
    dest_turnover_rate: float
    source_turnover_rate: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_channel": self.from_channel,
            "to_channel": self.to_channel,
            "from_peer_id": self.from_peer_id,
            "to_peer_id": self.to_peer_id,
            "amount_sats": self.amount_sats,
            "amount_msat": self.amount_msat,
            "outbound_fee_ppm": self.outbound_fee_ppm,
            "inbound_fee_ppm": self.inbound_fee_ppm,
            "source_fee_ppm": self.source_fee_ppm,
            "weighted_opp_cost_ppm": self.weighted_opp_cost_ppm,
            "spread_ppm": self.spread_ppm,
            "max_budget_sats": self.max_budget_sats,
            "max_budget_msat": self.max_budget_msat,
            "max_fee_ppm": self.max_fee_ppm,
            "expected_profit_sats": self.expected_profit_sats,
            "liquidity_ratio": round(self.liquidity_ratio, 4),
            "dest_flow_state": self.dest_flow_state,
            "dest_turnover_rate": round(self.dest_turnover_rate, 4),
            "source_turnover_rate": round(self.source_turnover_rate, 4)
        }


class EVRebalancer:
    """
    Expected Value based rebalancer.
    
    Only executes rebalances when the expected profit is positive.
    
    Key Principles:
    1. Fee Spread: Only rebalance if our outbound fee > inbound routing cost
    2. Budget Cap: Never pay more than the spread allows
    3. Minimum Profit: Require minimum profit threshold to justify complexity
    4. Liquidity Awareness: Focus on channels that need liquidity most
    """
    
    def __init__(self, plugin: Plugin, config: Config, database: Database,
                 clboss_manager: ClbossManager):
        """
        Initialize the rebalancer.
        
        Args:
            plugin: Reference to the pyln Plugin
            config: Configuration object
            database: Database instance
            clboss_manager: ClbossManager for handling overrides
        """
        self.plugin = plugin
        self.config = config
        self.database = database
        self.clboss = clboss_manager
        
        # Track pending rebalances to avoid duplicates
        self._pending: Dict[str, int] = {}  # channel_id -> timestamp
        
        # NOTE: Failure counts are now persisted to database (channel_failures table)
        # to survive plugin restarts and prevent "retry storms"
        # Use self.database.get_failure_count(), increment_failure_count(), etc.
        
        # Cache our node ID (fetched lazily on first use)
        self._our_node_id: Optional[str] = None
        
        # Optional profitability analyzer - set via setter to avoid circular imports
        self._profitability_analyzer: Optional['ChannelProfitabilityAnalyzer'] = None
    
    def _get_our_node_id(self) -> str:
        """
        Get our node ID, caching it for future use.
        
        Returns:
            Our node's public key (hex string)
        """
        if self._our_node_id is None:
            try:
                info = self.plugin.rpc.getinfo()
                self._our_node_id = info.get("id", "")
            except Exception as e:
                self.plugin.log(f"Error getting our node ID: {e}", level='error')
                self._our_node_id = ""
        return self._our_node_id
    
    def set_profitability_analyzer(self, analyzer: 'ChannelProfitabilityAnalyzer') -> None:
        """Set the profitability analyzer instance."""
        self._profitability_analyzer = analyzer
    
    def find_rebalance_candidates(self) -> List[RebalanceCandidate]:
        """
        Find channels that are profitable to rebalance.
        
        This is the main entry point, called periodically by the timer.
        
        Returns:
            List of RebalanceCandidate objects for profitable moves
        """
        candidates = []
        
        # GLOBAL CAPITAL CONTROLS: Check wallet reserve and daily budget
        if not self._check_capital_controls():
            return candidates
        
        # Get current channel states
        channels = self._get_channels_with_balances()
        
        if not channels:
            self.plugin.log("No channels available for rebalance analysis")
            return candidates
        
        # Find channels low on outbound (need rebalancing)
        depleted_channels = []
        source_channels = []
        
        for channel_id, info in channels.items():
            capacity = info.get("capacity", 0)
            spendable = info.get("spendable_sats", 0)
            
            if capacity == 0:
                continue
            
            outbound_ratio = spendable / capacity
            
            if outbound_ratio < self.config.low_liquidity_threshold:
                # This channel needs liquidity
                depleted_channels.append((channel_id, info, outbound_ratio))
            elif outbound_ratio > self.config.high_liquidity_threshold:
                # This channel has excess liquidity
                source_channels.append((channel_id, info, outbound_ratio))
        
        if not depleted_channels:
            self.plugin.log("No channels need rebalancing")
            return candidates
        
        if not source_channels:
            self.plugin.log("No source channels available for rebalancing")
            return candidates
        
        self.plugin.log(
            f"Found {len(depleted_channels)} depleted and "
            f"{len(source_channels)} source channels"
        )
        
        # Analyze each depleted channel for rebalance EV
        for dest_id, dest_info, dest_ratio in depleted_channels:
            # ADAPTIVE FAILURE BACKOFF (Task 3)
            # Use exponential backoff for channels that keep failing
            # This prevents wasting resources on "stuck" channels
            if self._is_pending_with_backoff(dest_id):
                continue
            
            # Skip if within cooldown period from DB (successful rebalance cooldown)
            last_rebalance = self.database.get_last_rebalance_time(dest_id)
            if last_rebalance:
                cooldown_seconds = self.config.rebalance_cooldown_hours * 3600
                time_since_last = int(time.time()) - last_rebalance
                if time_since_last < cooldown_seconds:
                    hours_remaining = (cooldown_seconds - time_since_last) / 3600
                    self.plugin.log(
                        f"Skipping {dest_id}: within {self.config.rebalance_cooldown_hours}h "
                        f"cooldown ({hours_remaining:.1f}h remaining)"
                    )
                    continue
            
            candidate = self._analyze_rebalance_ev(
                dest_id, dest_info, dest_ratio, source_channels
            )
            
            if candidate:
                candidates.append(candidate)
        
        # Sort by priority: SOURCE channels first (they're money printers!), then by profit
        # SOURCE channels get a priority boost since keeping them full is critical
        def sort_key(c):
            dest_state = self.database.get_channel_state(c.to_channel)
            flow_state = dest_state.get("state", "balanced") if dest_state else "balanced"
            # SOURCE = 2 (highest priority), BALANCED = 1, SINK should never appear here
            priority = 2 if flow_state == "source" else 1
            return (priority, c.expected_profit_sats)
        
        candidates.sort(key=sort_key, reverse=True)
        
        return candidates
    
    def _analyze_rebalance_ev(self, dest_channel: str, dest_info: Dict[str, Any],
                              dest_ratio: float,
                              sources: List[Tuple[str, Dict[str, Any], float]]
                              ) -> Optional[RebalanceCandidate]:
        """
        Analyze the expected value of rebalancing to a depleted channel.
        
        EV Calculation Steps:
        1. Check flow state - ABORT if channel is a SINK
        2. Determine rebalance amount (how much to move)
        3. Get our outbound fee on the destination channel
        4. Estimate inbound routing cost
        5. Calculate spread and max budget
        6. Check if profitable
        
        CRITICAL FLOW STATE LOGIC:
        - SINK channels fill themselves for free via routing
        - Paying to rebalance into a SINK is throwing money away
        - SOURCE channels are money printers - prioritize them
        
        Args:
            dest_channel: Channel needing liquidity
            dest_info: Channel information
            dest_ratio: Current outbound ratio
            sources: List of source channels to choose from
            
        Returns:
            RebalanceCandidate if profitable, None otherwise
        """
        # CRITICAL: Check flow state FIRST
        # Don't rebalance into SINK channels - they fill themselves for free!
        dest_state = self.database.get_channel_state(dest_channel)
        dest_flow_state = dest_state.get("state", "unknown") if dest_state else "unknown"
        
        if dest_flow_state == "sink":
            self.plugin.log(
                f"Skipping {dest_channel}: SINK channel fills itself for free. "
                f"Do NOT pay fees to rebalance into it!"
            )
            return None
        
        # Check channel profitability if analyzer is available
        # Skip or deprioritize underwater/zombie channels
        channel_profitability = None
        if self._profitability_analyzer:
            try:
                channel_profitability = self._profitability_analyzer.analyze_channel(dest_channel)
                if channel_profitability:
                    profitability_class = channel_profitability.classification.value
                    
                    # ZOMBIE channels: No routing activity - don't invest more
                    if profitability_class == "zombie":
                        self.plugin.log(
                            f"Skipping {dest_channel}: ZOMBIE channel has no routing activity. "
                            f"Don't invest more until it shows life."
                        )
                        return None
                    
                    # UNDERWATER channels: Negative ROI - be cautious
                    if profitability_class == "underwater":
                        # Only skip if deeply underwater (ROI < -50%)
                        if channel_profitability.roi_percent < -50:
                            self.plugin.log(
                                f"Skipping {dest_channel}: deeply UNDERWATER channel "
                                f"(ROI={channel_profitability.roi_percent:.1f}%). "
                                f"Fix economics before rebalancing."
                            )
                            return None
                        else:
                            self.plugin.log(
                                f"Channel {dest_channel} is UNDERWATER (ROI={channel_profitability.roi_percent:.1f}%) "
                                f"but proceeding with caution."
                            )
            except Exception as e:
                self.plugin.log(f"Error checking profitability for {dest_channel}: {e}")
        
        # Determine how much to rebalance
        capacity = dest_info.get("capacity", 0)
        spendable = dest_info.get("spendable_sats", 0)
        
        # DYNAMIC LIQUIDITY TARGETING (Task 1)
        # The old approach targeted 50% for all channels - the "Balance Fallacy".
        # Different channel types need different target balances:
        #
        # SOURCE (Draining): Target 85% Outbound
        #   - These channels drain quickly via routing
        #   - Keep them full so they can keep earning
        #   - We WANT outbound liquidity here
        #
        # BALANCED: Target 50% Outbound
        #   - Standard equilibrium target
        #   - Let natural flow determine direction
        #
        # SINK (Filling): Target 15% Outbound
        #   - These channels fill themselves for free via routing
        #   - Only rebalance if critically low
        #   - Waste of fees to fill what will fill naturally
        #
        # Note: We already skip pure SINK channels above, but this handles
        # channels that are "slightly sink" or transitioning states.
        
        if dest_flow_state == "source":
            # SOURCE channels are money printers - keep them full (85% outbound)
            target_ratio = 0.85
            target_spendable = int(capacity * target_ratio)
            self.plugin.log(
                f"Channel {dest_channel[:12]}... is SOURCE: targeting {target_ratio:.0%} outbound",
                level='debug'
            )
        elif dest_flow_state == "sink":
            # SINK channels fill naturally - only rebalance if critical (15% outbound)
            # Note: Pure sinks are already skipped above, this is for edge cases
            target_ratio = 0.15
            target_spendable = int(capacity * target_ratio)
            self.plugin.log(
                f"Channel {dest_channel[:12]}... is SINK: targeting only {target_ratio:.0%} outbound",
                level='debug'
            )
        else:
            # BALANCED or UNKNOWN - use standard 50% target
            target_ratio = 0.50
            target_spendable = capacity // 2
        
        amount_needed = target_spendable - spendable
        
        # If we're already above target, no need to rebalance
        if amount_needed <= 0:
            self.plugin.log(
                f"Skipping {dest_channel}: already at or above target "
                f"({spendable} >= {target_spendable} sats, target={target_ratio:.0%})",
                level='debug'
            )
            return None
        
        # Clamp to configured limits
        rebalance_amount = max(
            self.config.rebalance_min_amount,
            min(self.config.rebalance_max_amount, amount_needed)
        )
        
        # Convert to msat for RPC calls
        amount_msat = rebalance_amount * 1000
        
        # Get our outbound fee on destination channel
        outbound_fee_ppm = dest_info.get("fee_ppm", 0)
        
        # Estimate inbound routing cost
        # This is the fee we'll pay to route sats to ourselves
        inbound_fee_ppm = self._estimate_inbound_fee(dest_info.get("peer_id", ""))
        
        # Find best source channel FIRST (we need source fee for opportunity cost)
        best_source = self._select_source_channel(sources, rebalance_amount, dest_channel)
        if not best_source:
            return None
        
        source_id, source_info = best_source
        
        # Get source channel fee (OPPORTUNITY COST)
        # By draining the source channel, we lose potential routing income
        source_fee_ppm = source_info.get("fee_ppm", 0)
        
        # Calculate source turnover rate FIRST (needed for weighted opportunity cost)
        source_capacity = source_info.get("capacity", 1)
        source_turnover_rate = self._calculate_turnover_rate(source_id, source_capacity)
        
        # WEIGHTED OPPORTUNITY COST (Task 3A)
        # The full source fee assumes 100% probability the liquidity would have been used.
        # This is too aggressive for low-volume channels. Weight by actual turnover.
        # 
        # weighted_opp_cost = source_fee_ppm * min(1.0, source_turnover_rate * 7)
        # 
        # Example:
        # - High turnover (20%/day * 7 = 140% -> capped at 100%): Full opportunity cost
        # - Low turnover (2%/day * 7 = 14%): Only 14% of source fee counts as opp cost
        #
        # This prevents over-penalizing low-volume source channels that might never
        # route anyway, while still accounting for opportunity cost on active channels.
        turnover_weight = min(1.0, source_turnover_rate * 7)  # 7-day projection
        weighted_opp_cost = int(source_fee_ppm * turnover_weight)
        
        # Calculate spread INCLUDING WEIGHTED opportunity cost
        # Old: spread = dest_fee - inbound_cost - full_source_fee
        # New: spread = dest_fee - inbound_cost - weighted_opp_cost
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm - weighted_opp_cost
        
        # SANITY CHECK (Task 3B): Don't drain high-fee, active channels for low-fee destinations
        # If the source channel earns MORE than the destination AND is active (>10% turnover),
        # we should NOT drain it even if the spread math technically works.
        # This prevents draining your best channels to feed mediocre ones.
        if outbound_fee_ppm < source_fee_ppm and source_turnover_rate > 0.10:
            self.plugin.log(
                f"Skipping {dest_channel}: SANITY CHECK failed - "
                f"source channel earns MORE ({source_fee_ppm}ppm) than dest ({outbound_fee_ppm}ppm) "
                f"AND is highly active ({source_turnover_rate:.1%} daily turnover). "
                f"Don't drain a money-printing channel to fill a lower-fee one!"
            )
            return None
        
        # CRITICAL: Check spread FIRST before any calculations
        # If spread is negative, we LOSE money on every rebalance!
        if spread_ppm <= 0:
            self.plugin.log(
                f"Skipping {dest_channel}: negative/zero spread after weighted opportunity cost "
                f"(dest={outbound_fee_ppm}ppm - rebal={inbound_fee_ppm}ppm - "
                f"weighted_opp={weighted_opp_cost}ppm [raw={source_fee_ppm}ppm * {turnover_weight:.0%}] = {spread_ppm}ppm)"
            )
            return None
        
        # Calculate max budget (what we can pay and still profit)
        # max_budget = spread * amount / 1_000_000
        max_budget_sats = (spread_ppm * rebalance_amount) // 1_000_000
        max_budget_msat = max_budget_sats * 1000
        
        # Calculate max fee as PPM for circular
        # This is the KEY constraint we pass to circular
        # Note: We use inbound_fee_ppm here since that's what we're actually paying
        # The spread already accounts for opportunity cost in expected profit
        if amount_msat > 0:
            # Effective max fee = spread + inbound (since we want to pay at most what
            # makes us break even after opportunity cost)
            effective_max_fee_ppm = inbound_fee_ppm + (spread_ppm // 2)  # Leave margin
            max_fee_ppm = max(1, min(effective_max_fee_ppm, spread_ppm + inbound_fee_ppm))
        else:
            max_fee_ppm = 0
        
        # Check if max_fee_ppm is reasonable
        if max_fee_ppm <= 0:
            self.plugin.log(
                f"Skipping {dest_channel}: max_fee_ppm is zero or negative"
            )
            return None
        
        # Dynamic utilization based on actual channel turnover
        dest_turnover_rate = self._calculate_turnover_rate(dest_channel, capacity)
        # Note: source_turnover_rate was already calculated above for weighted opportunity cost
        
        # Project expected routing based on actual turnover, scaled by cooldown period
        # If channel turns over 5% daily and cooldown is 24h, expect ~5% utilization
        cooldown_days = self.config.rebalance_cooldown_hours / 24.0
        expected_utilization = min(dest_turnover_rate * cooldown_days, 1.0)  # Cap at 100%
        
        # Ensure minimum utilization estimate if no history
        expected_utilization = max(expected_utilization, 0.05)  # At least 5%
        
        expected_routing = rebalance_amount * expected_utilization
        expected_fee_income = (expected_routing * outbound_fee_ppm) // 1_000_000
        
        # Calculate WEIGHTED opportunity cost for expected profit calculation
        # Use the same weighted approach as the spread calculation
        expected_source_utilization = min(source_turnover_rate * cooldown_days, 1.0)
        expected_source_utilization = max(expected_source_utilization, 0.05)
        expected_source_routing = rebalance_amount * expected_source_utilization
        expected_source_loss = (expected_source_routing * source_fee_ppm * turnover_weight) // 1_000_000
        
        # Estimated profit = expected income - max budget - weighted opportunity cost
        expected_profit = expected_fee_income - max_budget_sats - expected_source_loss
        
        # CRITICAL: Check if expected profit meets minimum threshold
        # This is the EV check - only rebalance if we expect to profit
        if expected_profit < self.config.rebalance_min_profit:
            self.plugin.log(
                f"Skipping {dest_channel}: not profitable enough with weighted opportunity cost "
                f"(profit={expected_profit}sats < min={self.config.rebalance_min_profit}sats, "
                f"fee_income={expected_fee_income}sats, rebal_cost={max_budget_sats}sats, "
                f"weighted_opp_cost={expected_source_loss}sats [weight={turnover_weight:.0%}], "
                f"dest_turnover={dest_turnover_rate:.2%}, src_turnover={source_turnover_rate:.2%})"
            )
            return None
        
        # Log priority for SOURCE channels
        if dest_flow_state == "source":
            self.plugin.log(
                f"HIGH PRIORITY: {dest_channel} is a SOURCE (money printer). "
                f"NetSpread={spread_ppm}ppm (dest={outbound_fee_ppm}ppm, "
                f"rebal={inbound_fee_ppm}ppm, weighted_opp={weighted_opp_cost}ppm [raw={source_fee_ppm}ppm * {turnover_weight:.0%}]), "
                f"dest_turnover={dest_turnover_rate:.2%}, src_turnover={source_turnover_rate:.2%}"
            )
        
        return RebalanceCandidate(
            from_channel=source_id,
            to_channel=dest_channel,
            from_peer_id=source_info.get("peer_id", ""),
            to_peer_id=dest_info.get("peer_id", ""),
            amount_sats=rebalance_amount,
            amount_msat=amount_msat,
            outbound_fee_ppm=outbound_fee_ppm,
            inbound_fee_ppm=inbound_fee_ppm,
            source_fee_ppm=source_fee_ppm,
            weighted_opp_cost_ppm=weighted_opp_cost,
            spread_ppm=spread_ppm,
            max_budget_sats=max_budget_sats,
            max_budget_msat=max_budget_msat,
            max_fee_ppm=max_fee_ppm,
            expected_profit_sats=expected_profit,
            liquidity_ratio=dest_ratio,
            dest_flow_state=dest_flow_state,
            dest_turnover_rate=dest_turnover_rate,
            source_turnover_rate=source_turnover_rate
        )
    
    def _calculate_turnover_rate(self, channel_id: str, capacity: int) -> float:
        """
        Calculate the daily turnover rate for a channel.
        
        Turnover Rate = Daily Volume / Capacity
        
        This tells us how much of the channel's capacity is utilized per day.
        A channel with 1M capacity and 100k daily volume has 10% turnover.
        
        Args:
            channel_id: Channel to calculate turnover for
            capacity: Channel capacity in sats
            
        Returns:
            Turnover rate as decimal (e.g., 0.10 = 10% daily turnover)
        """
        if capacity <= 0:
            return 0.0
        
        try:
            # Get daily volume from channel state
            state = self.database.get_channel_state(channel_id)
            if not state:
                return 0.05  # Default 5% if no data
            
            # Calculate daily volume (sats_in + sats_out over flow_window_days)
            total_volume = state.get("sats_in", 0) + state.get("sats_out", 0)
            flow_window = max(self.config.flow_window_days, 1)
            daily_volume = total_volume / flow_window
            
            # Calculate turnover rate
            turnover_rate = daily_volume / capacity
            
            # Sanity bounds (0.01% to 100% daily turnover)
            turnover_rate = max(0.0001, min(1.0, turnover_rate))
            
            return turnover_rate
            
        except Exception as e:
            self.plugin.log(f"Error calculating turnover for {channel_id}: {e}", level='debug')
            return 0.05  # Default 5% on error
    
    def _estimate_inbound_fee(self, peer_id: str, amount_msat: int = 100000000) -> int:
        """
        Estimate the fee we'll pay to route to ourselves via a peer.
        
        REWRITTEN (Task 2): Last Hop Cost + Network Buffer
        
        The previous implementation was too generic and often underestimated costs.
        The new approach explicitly looks at the peer's fee policy towards us
        (the "last hop" cost) and adds a buffer for the rest of the network path.
        
        Priority Order:
        1. Last Hop Cost: Peer's explicit fee to route TO US (from listchannels)
           + Network buffer for hops 1 to N-1
        2. Historical data: What we've actually paid before
        3. getroute probe: Network fees (may not reflect actual circular path)
        4. Fallback default: Conservative estimate
        
        Args:
            peer_id: The peer we're routing through
            amount_msat: Amount to estimate fees for (default 100k sats)
            
        Returns:
            Estimated inbound fee in PPM
        """
        # Method 1 (PRIORITY): Get the LAST HOP COST explicitly
        # This is the fee the peer charges to route TO US
        # It's the most important component and was previously underweighted
        last_hop_fee = self._get_last_hop_fee(peer_id)
        if last_hop_fee is not None:
            # Add network buffer for hops 1 to N-1
            # The last hop is the peer -> us, but we also need to pay
            # for the network path from us -> ... -> peer
            network_buffer = self.config.inbound_fee_estimate_ppm
            total_estimate = last_hop_fee + network_buffer
            
            self.plugin.log(
                f"Inbound fee estimate for {peer_id[:12]}...: {total_estimate} PPM "
                f"(last_hop={last_hop_fee}ppm + network_buffer={network_buffer}ppm)",
                level='debug'
            )
            return total_estimate
        
        # Method 2: Check historical rebalance costs from database
        historical_fee = self._get_historical_inbound_fee(peer_id)
        if historical_fee is not None:
            return historical_fee
        
        # Method 3: Use getroute to probe actual network fees
        # Note: This may not reflect actual circular path costs accurately
        route_fee = self._get_route_fee_estimate(peer_id, amount_msat)
        if route_fee is not None:
            return route_fee
        
        # Method 4: Fallback default
        self.plugin.log(
            f"Using default inbound fee estimate for peer {peer_id[:12]}...",
            level='debug'
        )
        return 1000  # Conservative default (1000 ppm)
    
    def _get_last_hop_fee(self, peer_id: str) -> Optional[int]:
        """
        Get the peer's fee to route TO US (the "last hop" cost).
        
        This queries listchannels for channels from the peer to us,
        and extracts the fee_per_millionth they charge.
        
        This is the most accurate component of inbound fee estimation
        because it's the explicit fee policy for the final hop.
        
        Args:
            peer_id: Peer node ID
            
        Returns:
            Peer's fee to us in PPM, or None if not found/not public
        """
        try:
            our_node_id = self._get_our_node_id()
            if not our_node_id:
                return None
            
            # Query channels FROM peer (peer as source)
            channels = self.plugin.rpc.listchannels(source=peer_id)
            
            for channel in channels.get("channels", []):
                # Find the channel where destination is US
                if channel.get("destination") == our_node_id:
                    fee_ppm = channel.get("fee_per_millionth", 0)
                    fee_base_msat = channel.get("base_fee_millisatoshi", 0)
                    
                    # For better accuracy with base fee:
                    # Effective PPM = fee_ppm + (base_fee_msat * 1000 / amount_msat)
                    # But for simplicity and since amounts vary, we use fee_ppm
                    # plus a small adjustment for base fee
                    if fee_base_msat > 0:
                        # Add ~1 ppm per 1 msat base fee (rough adjustment)
                        fee_ppm += fee_base_msat // 1000
                    
                    self.plugin.log(
                        f"Last hop fee from {peer_id[:12]}... to us: {fee_ppm} PPM "
                        f"(base={fee_base_msat}msat)",
                        level='debug'
                    )
                    return fee_ppm
            
            # Channel not found - might be private or not announced
            self.plugin.log(
                f"No public channel from {peer_id[:12]}... to us found in gossip",
                level='debug'
            )
            return None
                    
        except Exception as e:
            self.plugin.log(
                f"Error getting last hop fee for {peer_id[:12]}...: {e}",
                level='debug'
            )
        
        return None
    
    def _get_route_fee_estimate(self, peer_id: str, amount_msat: int) -> Optional[int]:
        """
        Use getroute to probe actual network fees to reach a peer.
        
        This gives us the real routing cost through the network.
        We route TO the peer (not ourselves) since that's where we need
        to push liquidity from.
        
        Args:
            peer_id: Target peer node ID
            amount_msat: Amount to route
            
        Returns:
            Fee in PPM, or None if route not found
        """
        try:
            # Get route to the peer
            # Use a reasonable risk factor and max hops
            route = self.plugin.rpc.getroute(
                id=peer_id,
                amount_msat=amount_msat,
                riskfactor=10,
                maxhops=6
            )
            
            route_hops = route.get("route", [])
            if not route_hops:
                return None
            
            # Calculate total fees from route
            total_fee_msat = 0
            for hop in route_hops:
                # Each hop has the amount at that point
                # The difference from our send amount is the cumulative fee
                pass
            
            # Simpler: first hop amount - final amount = total fee
            if len(route_hops) >= 1:
                first_hop_msat = route_hops[0].get("amount_msat", amount_msat)
                if isinstance(first_hop_msat, str):
                    first_hop_msat = int(first_hop_msat.replace("msat", ""))
                
                total_fee_msat = first_hop_msat - amount_msat
                
                # Convert to PPM
                if amount_msat > 0:
                    fee_ppm = int((total_fee_msat / amount_msat) * 1_000_000)
                    
                    self.plugin.log(
                        f"Route probe to {peer_id[:12]}...: {fee_ppm} PPM "
                        f"({total_fee_msat // 1000} sats for {amount_msat // 1000} sats, "
                        f"{len(route_hops)} hops)",
                        level='debug'
                    )
                    return fee_ppm
                    
        except Exception as e:
            self.plugin.log(
                f"getroute probe failed for {peer_id[:12]}...: {e}",
                level='debug'
            )
        
        return None
    
    def _get_historical_inbound_fee(self, peer_id: str) -> Optional[int]:
        """
        Get average fee from historical successful rebalances to this peer.
        
        Args:
            peer_id: Peer node ID
            
        Returns:
            Average historical fee in PPM, or None if no history
        """
        try:
            # Query database for past rebalances to channels with this peer
            history = self.database.get_rebalance_history_by_peer(peer_id)
            
            if not history:
                return None
            
            # Calculate average PPM from successful rebalances
            total_ppm = 0
            count = 0
            
            for record in history:
                if record.get("status") == "success":
                    fee_paid = record.get("fee_paid_msat", 0)
                    amount = record.get("amount_msat", 0)
                    
                    if amount > 0:
                        ppm = int((fee_paid / amount) * 1_000_000)
                        total_ppm += ppm
                        count += 1
            
            if count > 0:
                avg_ppm = total_ppm // count
                self.plugin.log(
                    f"Historical inbound fee for {peer_id[:12]}...: {avg_ppm} PPM "
                    f"(from {count} rebalances)",
                    level='debug'
                )
                return avg_ppm
                
        except Exception as e:
            self.plugin.log(
                f"Error getting historical fees for {peer_id[:12]}...: {e}",
                level='debug'
            )
        
        return None
    
    def _select_source_channel(self, sources: List[Tuple[str, Dict[str, Any], float]],
                               amount_needed: int,
                               dest_channel: Optional[str] = None) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Select the best source channel for rebalancing.
        
        NEW: Now considers opportunity cost when scoring sources.
        Lower fee sources are preferred because draining them costs us less
        in opportunity cost.
        
        Criteria:
        1. Has enough excess liquidity
        2. Ideally a channel that's filling (sink state) - no opportunity cost!
        3. LOWEST fees preferred (minimize opportunity cost)
        4. Peer is connected and reliable
        
        Args:
            sources: List of source channel candidates
            amount_needed: How much we need to move
            dest_channel: Optional destination channel (for logging)
            
        Returns:
            (channel_id, channel_info) tuple or None
        """
        best_source = None
        best_score = -float('inf')
        
        # Get peer connection status for reliability scoring
        peer_status = self._get_peer_connection_status()
        
        for channel_id, info, outbound_ratio in sources:
            spendable = info.get("spendable_sats", 0)
            peer_id = info.get("peer_id", "")
            
            # Check if has enough liquidity
            if spendable < amount_needed:
                continue
            
            # Check if peer is connected - skip disconnected peers
            if peer_id and peer_id in peer_status:
                if not peer_status[peer_id].get("connected", False):
                    self.plugin.log(
                        f"Skipping source {channel_id}: peer not connected",
                        level='debug'
                    )
                    continue
            
            # Calculate score (higher = better source)
            # CHANGED: Now HEAVILY penalize high-fee sources (opportunity cost)
            fee_ppm = info.get("fee_ppm", 1000)
            
            # NEW scoring formula emphasizing LOW opportunity cost:
            # - Ratio bonus: channels with more excess (up to 50 points)
            # - Fee PENALTY: high-fee channels are expensive to drain
            #   (fee_ppm / 10 means 1000ppm = -100 points penalty)
            score = (outbound_ratio * 50) - (fee_ppm / 10)
            
            # BIG Bonus if channel is sink (filling up)
            # Sinks have essentially ZERO opportunity cost - they refill for free!
            state = self.database.get_channel_state(channel_id)
            if state and state.get("state") == "sink":
                score += 100  # Major bonus - sinks are ideal sources
                self.plugin.log(
                    f"Source {channel_id} is SINK (ideal - zero opp cost), fee={fee_ppm}ppm",
                    level='debug'
                )
            elif state and state.get("state") == "balanced":
                score += 20  # Moderate bonus for balanced channels
            # SOURCE channels get no bonus - they're earning money, don't drain them!
            
            # Bonus for reliable peers (connected with features)
            if peer_id and peer_id in peer_status:
                ps = peer_status[peer_id]
                if ps.get("connected"):
                    score += 10  # Connected bonus
                    
                    # Additional bonus for peers with more channels (established)
                    num_channels = ps.get("num_channels", 1)
                    score += min(5, num_channels)  # Up to +5 for multi-channel peers
            
            if score > best_score:
                best_score = score
                best_source = (channel_id, info)
        
        return best_source
    
    def _get_peer_connection_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Get connection status for all peers.
        
        Uses listpeers to check which peers are currently connected
        and gather reliability metrics.
        
        Returns:
            Dict mapping peer_id to status info
        """
        status = {}
        
        try:
            result = self.plugin.rpc.listpeers()
            
            for peer in result.get("peers", []):
                peer_id = peer.get("id", "")
                if not peer_id:
                    continue
                
                status[peer_id] = {
                    "connected": peer.get("connected", False),
                    "num_channels": len(peer.get("channels", [])),
                    "features": peer.get("features", "")
                }
                
        except Exception as e:
            self.plugin.log(f"Error getting peer status: {e}", level='debug')
        
        return status
    
    def execute_rebalance(self, candidate: RebalanceCandidate) -> Dict[str, Any]:
        """
        Execute a rebalance using the configured rebalancer plugin.
        
        MANAGER-OVERRIDE PATTERN:
        1. Unmanage both channels from clboss
        2. Record the rebalance attempt
        3. Call the rebalancer plugin with strict budget
        4. Record the result
        
        Args:
            candidate: The RebalanceCandidate to execute
            
        Returns:
            Result dict with success status and details
        """
        result = {
            "success": False,
            "candidate": candidate.to_dict(),
            "message": ""
        }
        
        # Mark as pending
        self._pending[candidate.to_channel] = int(time.time())
        
        try:
            # Step 1: Unmanage from clboss (both fee AND balance)
            # This tells clboss: "Don't touch fees AND don't rebalance these channels"
            # ClbossTags.FEE_AND_BALANCE is "lnfee,balance" as comma-separated string
            self.clboss.ensure_unmanaged_for_channel(
                candidate.from_channel, candidate.from_peer_id,
                ClbossTags.FEE_AND_BALANCE, self.database
            )
            self.clboss.ensure_unmanaged_for_channel(
                candidate.to_channel, candidate.to_peer_id,
                ClbossTags.FEE_AND_BALANCE, self.database
            )
            
            # Step 2: Record the attempt
            rebalance_id = self.database.record_rebalance(
                from_channel=candidate.from_channel,
                to_channel=candidate.to_channel,
                amount_sats=candidate.amount_sats,
                max_fee_sats=candidate.max_budget_sats,
                expected_profit_sats=candidate.expected_profit_sats,
                status='pending'
            )
            
            # Step 3: Execute via rebalancer plugin
            if self.config.dry_run:
                self.plugin.log(
                    f"[DRY RUN] Would rebalance {candidate.amount_sats} sats "
                    f"from {candidate.from_channel} to {candidate.to_channel} "
                    f"with max fee {candidate.max_budget_sats} sats ({candidate.max_fee_ppm} ppm)"
                )
                result["success"] = True
                result["message"] = "Dry run - no changes made"
                self.database.update_rebalance_result(
                    rebalance_id, 'success',
                    actual_fee_sats=0,
                    actual_profit_sats=candidate.expected_profit_sats
                )
                return result
            
            # Call appropriate rebalancer
            if self.config.rebalancer_plugin == 'circular':
                rebalance_result = self._execute_circular(candidate)
            elif self.config.rebalancer_plugin == 'sling':
                rebalance_result = self._execute_sling(candidate)
            else:
                result["message"] = f"Unknown rebalancer: {self.config.rebalancer_plugin}"
                self.database.update_rebalance_result(
                    rebalance_id, 'failed',
                    error_message=result["message"]
                )
                return result
            
            # Step 4: Record result and apply anti-thrashing protection
            if rebalance_result.get("success"):
                actual_fee = rebalance_result.get("fee_sats", 0)
                actual_profit = candidate.expected_profit_sats - actual_fee
                
                self.database.update_rebalance_result(
                    rebalance_id, 'success',
                    actual_fee_sats=actual_fee,
                    actual_profit_sats=actual_profit
                )
                
                result["success"] = True
                result["actual_fee_sats"] = actual_fee
                result["actual_profit_sats"] = actual_profit
                result["message"] = "Rebalance completed successfully"
                
                # ADAPTIVE BACKOFF: Reset failure count on success (persisted to DB)
                self.database.reset_failure_count(candidate.to_channel)
                
                # ANTI-THRASHING: Keep clboss unmanaged for extended period
                # This prevents clboss from immediately "fixing" the balance
                # we just paid fees to establish
                self.plugin.log(
                    f"Rebalanced {candidate.amount_sats} sats to {candidate.to_channel}, "
                    f"fee={actual_fee}, profit={actual_profit}. "
                    f"Keeping clboss unmanaged to prevent thrashing."
                )
            else:
                error = rebalance_result.get("error", "Unknown error")
                self.database.update_rebalance_result(
                    rebalance_id, 'failed',
                    error_message=error
                )
                result["message"] = f"Rebalance failed: {error}"
                
                # ADAPTIVE BACKOFF: Increment failure count (persisted to DB)
                new_failure_count = self.database.increment_failure_count(candidate.to_channel)
                
                # Calculate next backoff time for logging
                base_cooldown = 600  # 10 minutes base
                next_cooldown = base_cooldown * (2 ** new_failure_count)
                next_cooldown_mins = next_cooldown // 60
                
                self.plugin.log(
                    f"Rebalance failed: {error}. "
                    f"Failure #{new_failure_count} for {candidate.to_channel[:12]}..., "
                    f"next attempt backoff: {next_cooldown_mins} minutes",
                    level='warn'
                )
                
        except Exception as e:
            result["message"] = f"Error: {str(e)}"
            self.plugin.log(f"Error executing rebalance: {e}", level='error')
            
            # ADAPTIVE BACKOFF: Also count exceptions as failures (persisted to DB)
            self.database.increment_failure_count(candidate.to_channel)
        finally:
            # Clear pending status after some time
            pass
        
        return result
    
    def _execute_circular(self, candidate: RebalanceCandidate) -> Dict[str, Any]:
        """
        Execute rebalance using the circular plugin via RPC.
        
        THE STRATEGIST AND DRIVER PATTERN:
        - This module (Strategist) has already calculated the EV and constraints
        - Circular (Driver) handles the actual pathfinding and payment execution
        - We tell circular: "Move X sats from A to B, max Y ppm fee. Go."
        
        Command: circular <outgoing_scid> <incoming_scid> <amount_msat> <maxppm> <retry_count>
        
        Args:
            candidate: The RebalanceCandidate with pre-calculated constraints
            
        Returns:
            Result dict with success status and fee info
        """
        result = {"success": False}
        
        self.plugin.log(
            f"Executing circular rebalance: "
            f"Out={candidate.from_channel} -> In={candidate.to_channel}"
        )
        self.plugin.log(
            f"EV Constraint: Amount={candidate.amount_msat}msat, "
            f"MaxFee={candidate.max_budget_msat}msat ({candidate.max_fee_ppm}ppm)"
        )
        
        try:
            # Call circular via RPC with positional arguments:
            # circular outscid inscid [amount] [maxppm] [attempts] [maxhops]
            response = self.plugin.rpc.circular(
                candidate.from_channel,      # outscid - outgoing channel
                candidate.to_channel,        # inscid - incoming channel
                candidate.amount_msat,       # amount in msat
                candidate.max_fee_ppm,       # maxppm - THE CRITICAL CONSTRAINT
                3,                           # attempts
                10                           # maxhops
            )
            
            # Analyze Result
            if response.get("status") == "success":
                fee_msat = response.get("fee", 0)
                result["success"] = True
                result["fee_sats"] = fee_msat // 1000
                result["fee_msat"] = fee_msat
                self.plugin.log(f"Circular SUCCESS. Paid: {fee_msat} msat")
            else:
                result["error"] = response.get("message", "Rebalance did not complete")
                self.plugin.log(f"Circular FAILED: {result['error']}")
                
        except RpcError as e:
            # Handle case where circular is not installed or crashes
            error_msg = str(e)
            if "Unknown command" in error_msg:
                result["error"] = "circular plugin not installed. Install from: https://github.com/giovannizotta/circular"
            else:
                result["error"] = f"RPC Error: {error_msg}"
            self.plugin.log(f"RPC Error calling circular: {error_msg}", level='error')
        except Exception as e:
            result["error"] = str(e)
            self.plugin.log(f"Error calling circular: {e}", level='error')
        
        return result
    
    def _execute_sling(self, candidate: RebalanceCandidate) -> Dict[str, Any]:
        """
        Execute rebalance using the sling plugin.
        
        sling is another CLN rebalancing plugin with different semantics.
        
        Args:
            candidate: The rebalance to execute
            
        Returns:
            Result dict with success status and fee info
        """
        result = {"success": False}
        
        try:
            # sling uses a job-based approach
            # First, create a job
            job_result = self.plugin.rpc.call(
                "sling-job",
                {
                    "channel": candidate.to_channel,  # Channel to fill
                    "direction": "pull",  # Pull liquidity from network
                    "amount": candidate.amount_sats * 1000,  # msat
                    "maxppm": candidate.max_budget_sats * 1_000_000 // candidate.amount_sats,
                    "outppm": candidate.outbound_fee_ppm
                }
            )
            
            # Check result
            if job_result.get("status") in ["complete", "success"]:
                result["success"] = True
                result["fee_sats"] = job_result.get("fee_msat", 0) // 1000
            else:
                result["error"] = job_result.get("message", "Sling job did not complete")
                
        except RpcError as e:
            # sling might not be installed
            if "Unknown command" in str(e):
                result["error"] = "sling plugin not available"
            else:
                result["error"] = str(e)
        except Exception as e:
            result["error"] = str(e)
        
        return result
    
    def manual_rebalance(self, from_channel: str, to_channel: str,
                        amount_sats: int, max_fee_sats: Optional[int] = None
                        ) -> Dict[str, Any]:
        """
        Execute a manual rebalance with EV checks.
        
        Even for manual rebalances, we calculate and display the EV
        to help the operator make informed decisions.
        
        Args:
            from_channel: Source channel
            to_channel: Destination channel
            amount_sats: Amount to move
            max_fee_sats: Optional fee cap (calculated if not provided)
            
        Returns:
            Result dict with execution details
        """
        # Get channel info
        channels = self._get_channels_with_balances()
        
        from_info = channels.get(from_channel)
        to_info = channels.get(to_channel)
        
        if not from_info or not to_info:
            return {"success": False, "error": "Channel not found"}
        
        # Check flow state for warning
        dest_state = self.database.get_channel_state(to_channel)
        dest_flow_state = dest_state.get("state", "unknown") if dest_state else "unknown"
        
        # Warn if trying to rebalance into a SINK
        if dest_flow_state == "sink":
            self.plugin.log(
                f"Warning: {to_channel} is a SINK channel. It fills itself for free. "
                f"Consider NOT rebalancing into it.",
                level='warn'
            )
        
        # Calculate EV even for manual
        outbound_fee_ppm = to_info.get("fee_ppm", 0)
        inbound_fee_ppm = self._estimate_inbound_fee(to_info.get("peer_id", ""))
        
        # Get source channel fee (opportunity cost)
        source_fee_ppm = from_info.get("fee_ppm", 0)
        
        # Calculate spread including opportunity cost
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm - source_fee_ppm
        
        # Convert to msat
        amount_msat = amount_sats * 1000
        
        # Calculate max budget if not provided
        if max_fee_sats is None:
            max_fee_sats = max(0, (spread_ppm * amount_sats) // 1_000_000)
        
        max_budget_msat = max_fee_sats * 1000
        
        # Calculate max fee PPM for circular
        if amount_msat > 0:
            max_fee_ppm = int((max_budget_msat / amount_msat) * 1_000_000)
        else:
            max_fee_ppm = 0
        
        # Calculate turnover rates for tracking
        dest_capacity = to_info.get("capacity", 1)
        source_capacity = from_info.get("capacity", 1)
        dest_turnover_rate = self._calculate_turnover_rate(to_channel, dest_capacity)
        source_turnover_rate = self._calculate_turnover_rate(from_channel, source_capacity)
        
        # Calculate weighted opportunity cost (same as automatic rebalance logic)
        turnover_weight = min(1.0, source_turnover_rate * 7)  # 7-day projection
        weighted_opp_cost = int(source_fee_ppm * turnover_weight)
        
        # Recalculate spread with weighted opportunity cost
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm - weighted_opp_cost
        
        # Calculate expected profit including weighted opportunity cost
        expected_utilization = max(dest_turnover_rate * (self.config.rebalance_cooldown_hours / 24.0), 0.05)
        expected_routing = amount_sats * expected_utilization
        expected_fee_income = (expected_routing * outbound_fee_ppm) // 1_000_000
        expected_source_loss = (expected_routing * source_fee_ppm * turnover_weight) // 1_000_000
        expected_profit = expected_fee_income - max_fee_sats - expected_source_loss
        
        # Build candidate
        candidate = RebalanceCandidate(
            from_channel=from_channel,
            to_channel=to_channel,
            from_peer_id=from_info.get("peer_id", ""),
            to_peer_id=to_info.get("peer_id", ""),
            amount_sats=amount_sats,
            amount_msat=amount_msat,
            outbound_fee_ppm=outbound_fee_ppm,
            inbound_fee_ppm=inbound_fee_ppm,
            source_fee_ppm=source_fee_ppm,
            weighted_opp_cost_ppm=weighted_opp_cost,
            spread_ppm=spread_ppm,
            max_budget_sats=max_fee_sats,
            max_budget_msat=max_budget_msat,
            max_fee_ppm=max_fee_ppm,
            expected_profit_sats=expected_profit,
            liquidity_ratio=to_info.get("spendable_sats", 0) / max(to_info.get("capacity", 1), 1),
            dest_flow_state=dest_flow_state,
            dest_turnover_rate=dest_turnover_rate,
            source_turnover_rate=source_turnover_rate
        )
        
        # Warn if negative EV (including weighted opportunity cost)
        if spread_ppm <= 0:
            self.plugin.log(
                f"Warning: Manual rebalance has negative EV after weighted opportunity cost "
                f"(dest={outbound_fee_ppm}ppm - rebal={inbound_fee_ppm}ppm - "
                f"weighted_opp={weighted_opp_cost}ppm [raw={source_fee_ppm}ppm * {turnover_weight:.0%}] = {spread_ppm}ppm)",
                level='warn'
            )
        
        return self.execute_rebalance(candidate)
    
    def _check_capital_controls(self) -> bool:
        """
        Check Global Capital Controls before allowing rebalancing.
        
        This prevents over-spending by:
        1. Checking if total wallet reserve (on-chain + channel receivable) is above minimum
        2. Checking if daily rebalancing budget has been exceeded
        
        Returns:
            True if rebalancing is allowed, False if capital controls block it
        """
        # Check 1: Minimum Wallet Reserve
        # If total spendable (on-chain + channel receivable) < min_wallet_reserve, ABORT
        try:
            listfunds = self.plugin.rpc.listfunds()
            
            # Sum on-chain spendable (confirmed outputs)
            onchain_sats = 0
            for output in listfunds.get("outputs", []):
                if output.get("status") == "confirmed":
                    amount_msat = output.get("amount_msat", 0)
                    if isinstance(amount_msat, str):
                        # Handle "123456msat" format
                        amount_msat = int(amount_msat.replace("msat", ""))
                    onchain_sats += amount_msat // 1000
            
            # Sum channel spendable (our local balance - this is OUR money)
            channel_spendable_sats = 0
            for channel in listfunds.get("channels", []):
                if channel.get("state") != "CHANNELD_NORMAL":
                    continue
                our_amount_msat = channel.get("our_amount_msat", 0)
                if isinstance(our_amount_msat, str):
                    our_amount_msat = int(our_amount_msat.replace("msat", ""))
                # Spendable = our_amount (This is Our Money, which pays the fees)
                spendable = our_amount_msat // 1000
                if spendable > 0:
                    channel_spendable_sats += spendable
            
            total_reserve = onchain_sats + channel_spendable_sats
            min_reserve = self.config.min_wallet_reserve
            
            if total_reserve < min_reserve:
                self.plugin.log(
                    f"CAPITAL CONTROL: Wallet reserve too low! "
                    f"on-chain={onchain_sats:,} + channel_spendable={channel_spendable_sats:,} = "
                    f"{total_reserve:,} sats < min_reserve={min_reserve:,} sats. "
                    f"ABORTING all rebalancing operations.",
                    level='warning'
                )
                return False
            
        except RpcError as e:
            self.plugin.log(f"Error checking wallet reserve: {e}", level='error')
            # Fail-safe: allow rebalancing if we can't check
            self.plugin.log("Continuing with rebalance (could not verify reserve)")
        
        # Check 2: Daily Budget
        # Sum fees spent in last 24 hours, abort if over budget
        try:
            now = int(time.time())
            twenty_four_hours_ago = now - (24 * 60 * 60)
            
            fees_spent_24h = self.database.get_total_rebalance_fees(twenty_four_hours_ago)
            daily_budget = self.config.daily_budget_sats
            
            if fees_spent_24h >= daily_budget:
                self.plugin.log(
                    f"CAPITAL CONTROL: Daily rebalancing budget exceeded! "
                    f"Spent {fees_spent_24h:,} sats in last 24h >= budget of {daily_budget:,} sats. "
                    f"ABORTING rebalancing until budget resets.",
                    level='warning'
                )
                return False
            
            # Log remaining budget
            remaining = daily_budget - fees_spent_24h
            self.plugin.log(
                f"Capital controls OK: {fees_spent_24h:,}/{daily_budget:,} sats spent, "
                f"{remaining:,} sats remaining in daily budget",
                level='debug'
            )
            
        except Exception as e:
            self.plugin.log(f"Error checking daily budget: {e}", level='error')
            # Fail-safe: allow rebalancing if we can't check
            self.plugin.log("Continuing with rebalance (could not verify budget)")
        
        return True
    
    def _get_channels_with_balances(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all channels with their current balances.
        
        Returns:
            Dict mapping channel_id to channel info including balances
        """
        channels = {}
        
        try:
            result = self.plugin.rpc.listpeerchannels()
            
            for channel in result.get("channels", []):
                if channel.get("state") != "CHANNELD_NORMAL":
                    continue
                
                channel_id = channel.get("short_channel_id") or channel.get("channel_id")
                if not channel_id:
                    continue
                
                # Extract balance info
                spendable_msat = channel.get("spendable_msat", 0) or 0
                receivable_msat = channel.get("receivable_msat", 0) or 0
                
                # Calculate capacity - may be null in some CLN versions
                total_msat = channel.get("total_msat") or channel.get("capacity_msat")
                if not total_msat:
                    total_msat = spendable_msat + receivable_msat
                
                # Get fee info - in newer CLN it's under updates.local
                updates = channel.get("updates", {})
                local_updates = updates.get("local", {})
                
                # Try updates.local first, fall back to top-level
                fee_base = local_updates.get("fee_base_msat") or channel.get("fee_base_msat", 0)
                fee_ppm = local_updates.get("fee_proportional_millionths") or channel.get("fee_proportional_millionths", 0)
                
                channels[channel_id] = {
                    "channel_id": channel_id,
                    "peer_id": channel.get("peer_id", ""),
                    "capacity": total_msat // 1000 if total_msat else 0,
                    "spendable_sats": spendable_msat // 1000,
                    "receivable_sats": receivable_msat // 1000,
                    "fee_base_msat": fee_base,
                    "fee_ppm": fee_ppm
                }
                
        except RpcError as e:
            self.plugin.log(f"Error getting channel balances: {e}", level='error')
        
        return channels
    
    def _is_pending_with_backoff(self, channel_id: str) -> bool:
        """
        Check if a channel should be skipped due to pending status or adaptive backoff.
        
        ADAPTIVE FAILURE BACKOFF (Task 3):
        Instead of a fixed 10-minute cooldown, we use exponential backoff
        based on consecutive failures:
        
        - 0 failures: 10 min base cooldown
        - 1 failure:  20 min (10 * 2^1)
        - 2 failures: 40 min (10 * 2^2)
        - 3 failures: 80 min (10 * 2^3)
        - etc.
        
        PERSISTENCE (Day 2 Task 2):
        Failure counts are now stored in the database (channel_failures table)
        to survive plugin restarts and prevent "retry storms" on broken channels.
        
        This prevents wasting resources on "stuck" channels that keep failing,
        while still allowing quick retries for channels that just had a one-off issue.
        
        Args:
            channel_id: Channel to check
            
        Returns:
            True if channel should be skipped (pending or in backoff)
        """
        pending_time = self._pending.get(channel_id, 0)
        if pending_time == 0:
            return False
        
        # Get failure count from database (persistent across restarts)
        failure_count, _ = self.database.get_failure_count(channel_id)
        base_cooldown = 600  # 10 minutes base
        
        # Exponential backoff: base * 2^failures
        # Cap at ~2.7 hours (10 min * 2^4 = 160 min) to prevent infinite waits
        max_failures_for_backoff = 4
        effective_failures = min(failure_count, max_failures_for_backoff)
        cooldown = base_cooldown * (2 ** effective_failures)
        
        time_since_attempt = int(time.time()) - pending_time
        
        if time_since_attempt > cooldown:
            # Cooldown expired, clear pending status
            del self._pending[channel_id]
            return False
        
        # Still in cooldown
        if failure_count > 0:
            remaining = cooldown - time_since_attempt
            self.plugin.log(
                f"Skipping {channel_id[:12]}...: in backoff after {failure_count} failures "
                f"({remaining // 60} min remaining)",
                level='debug'
            )
        
        return True
    
    def _is_pending(self, channel_id: str, timeout: int = 600) -> bool:
        """
        Check if a channel has a pending rebalance.
        
        DEPRECATED: Use _is_pending_with_backoff() for adaptive backoff.
        Kept for backward compatibility.
        
        Args:
            channel_id: Channel to check
            timeout: How long to consider pending (default 10 min)
            
        Returns:
            True if a rebalance is pending
        """
        pending_time = self._pending.get(channel_id, 0)
        if pending_time == 0:
            return False
        
        # Check if timed out
        if int(time.time()) - pending_time > timeout:
            del self._pending[channel_id]
            return False
        
        return True
    
    def reset_failure_count(self, channel_id: str) -> None:
        """
        Manually reset the failure count for a channel.
        
        Use this when manually intervening or when conditions change.
        
        Args:
            channel_id: Channel to reset
        """
        self.database.reset_failure_count(channel_id)
        self.plugin.log(f"Reset failure count for {channel_id}")
    
    def get_failure_counts(self) -> Dict[str, Tuple[int, int]]:
        """
        Get all current failure counts.
        
        Returns:
            Dict mapping channel_id to (failure_count, last_failure_time)
        """
        return self.database.get_all_failure_counts()
