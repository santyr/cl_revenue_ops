"""
Hill Climbing Fee Controller module for cl-revenue-ops

MODULE 2: Revenue-Maximizing Fee Controller (Dynamic Pricing)

This module implements a Hill Climbing (Perturb & Observe) algorithm
for dynamically adjusting channel fees to maximize revenue.

Why Hill Climbing Instead of PID?
- PID targets a static flow rate, ignoring price elasticity
- Hill Climbing actively seeks the revenue-maximizing fee point
- It adapts to changing market conditions and peer behavior

Hill Climbing Algorithm:
1. Perturb: Make a small fee change in a direction
2. Observe: Measure the resulting revenue change
3. Decide:
   - If Revenue Increased: Keep going in the same direction
   - If Revenue Decreased: Reverse direction (we went too far)
4. Repeat: Continuously seek the optimal fee point

Revenue Calculation:
- Revenue = Volume * Fee
- We track revenue over time windows to measure impact of changes

Constraints:
- Never drop below floor (economic minimum based on chain costs)
- Never exceed ceiling (prevent absurd fees)
- Use liquidity bucket multipliers as secondary weighting
- Unmanage from clboss before setting fees

The Hill Climber provides adaptive, revenue-seeking fee adjustments that
find the optimal price point where volume * fee is maximized.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING

from pyln.client import Plugin, RpcError

from .config import Config, ChainCostDefaults, LiquidityBuckets
from .database import Database
from .clboss_manager import ClbossManager, ClbossTags

if TYPE_CHECKING:
    from .profitability_analyzer import ChannelProfitabilityAnalyzer


@dataclass
class HillClimbState:
    """
    State of the Hill Climbing fee optimizer for one channel.
    
    Attributes:
        last_revenue_sats: Revenue observed in the previous period
        last_fee_ppm: Fee that was in effect during last period
        trend_direction: Current search direction (1 = increasing, -1 = decreasing)
        last_update: Timestamp of last update
        consecutive_same_direction: How many times we've moved in same direction
    """
    last_revenue_sats: int = 0
    last_fee_ppm: int = 0
    trend_direction: int = 1  # 1 = try increasing fee, -1 = try decreasing
    last_update: int = 0
    consecutive_same_direction: int = 0


@dataclass
class FeeAdjustment:
    """
    Record of a fee adjustment.
    
    Attributes:
        channel_id: Channel that was adjusted
        peer_id: Peer node ID
        old_fee_ppm: Previous fee
        new_fee_ppm: New fee after adjustment
        reason: Explanation of the adjustment
        hill_climb_values: Hill Climbing algorithm internal values
    """
    channel_id: str
    peer_id: str
    old_fee_ppm: int
    new_fee_ppm: int
    reason: str
    hill_climb_values: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "old_fee_ppm": self.old_fee_ppm,
            "new_fee_ppm": self.new_fee_ppm,
            "reason": self.reason,
            "hill_climb_values": self.hill_climb_values
        }


class HillClimbingFeeController:
    """
    Hill Climbing (Perturb & Observe) fee controller for revenue maximization.
    
    The controller aims to find the revenue-maximizing fee by iteratively
    adjusting fees and observing the revenue impact.
    
    Key Principles:
    1. Revenue Focus: Maximize Volume * Fee, not just volume
    2. Adaptive: Learns from revenue changes to find optimal fees
    3. Bounded: Respects floor/ceiling constraints
    4. Liquidity-aware: Uses bucket multipliers as weights
    5. clboss override: Unmanages from clboss before setting fees
    
    Hill Climbing Parameters:
    - step_ppm: Base fee change per iteration (default 50 ppm)
    - step_percent: Alternative step as percentage (default 5%)
    - min_observation_window: Minimum time between changes (default 6 hours)
    """
    
    # Hill Climbing parameters
    STEP_PPM = 50           # Fixed step size in PPM
    STEP_PERCENT = 0.05     # Percentage step size (5%)
    MIN_STEP_PPM = 10       # Minimum step size
    MAX_STEP_PPM = 200      # Maximum step size
    MAX_CONSECUTIVE = 5     # Max consecutive moves in same direction before reducing step
    
    def __init__(self, plugin: Plugin, config: Config, database: Database, 
                 clboss_manager: ClbossManager,
                 profitability_analyzer: Optional["ChannelProfitabilityAnalyzer"] = None):
        """
        Initialize the fee controller.
        
        Args:
            plugin: Reference to the pyln Plugin
            config: Configuration object
            database: Database instance
            clboss_manager: ClbossManager for handling overrides
            profitability_analyzer: Optional profitability analyzer for ROI-based adjustments
        """
        self.plugin = plugin
        self.config = config
        self.database = database
        self.clboss = clboss_manager
        self.profitability = profitability_analyzer
        
        # In-memory cache of Hill Climbing states (also persisted to DB)
        self._hill_climb_states: Dict[str, HillClimbState] = {}
    
    def adjust_all_fees(self) -> List[FeeAdjustment]:
        """
        Adjust fees for all channels using Hill Climbing optimization.
        
        This is the main entry point, called periodically by the timer.
        
        Returns:
            List of FeeAdjustment records for channels that were adjusted
        """
        adjustments = []
        
        # Get all channel states from flow analysis
        channel_states = self.database.get_all_channel_states()
        
        if not channel_states:
            self.plugin.log("No channel state data for fee adjustment")
            return adjustments
        
        # Get current channel info for capacity and balance
        channels = self._get_channels_info()
        
        for state in channel_states:
            channel_id = state.get("channel_id")
            peer_id = state.get("peer_id")
            
            if not channel_id or not peer_id:
                continue
            
            # Get channel info
            channel_info = channels.get(channel_id)
            if not channel_info:
                continue
            
            try:
                adjustment = self._adjust_channel_fee(
                    channel_id=channel_id,
                    peer_id=peer_id,
                    state=state,
                    channel_info=channel_info
                )
                
                if adjustment:
                    adjustments.append(adjustment)
                    
            except Exception as e:
                self.plugin.log(f"Error adjusting fee for {channel_id}: {e}", level='error')
        
        return adjustments
    
    def _adjust_channel_fee(self, channel_id: str, peer_id: str,
                           state: Dict[str, Any], 
                           channel_info: Dict[str, Any]) -> Optional[FeeAdjustment]:
        """
        Adjust fee for a single channel using Hill Climbing optimization.
        
        Hill Climbing (Perturb & Observe) Algorithm:
        1. Calculate current revenue (volume * fee)
        2. Compare to last period's revenue
        3. If revenue increased: continue in same direction
        4. If revenue decreased: reverse direction
        5. Apply step change in calculated direction
        
        Args:
            channel_id: Channel to adjust
            peer_id: Peer node ID
            state: Channel state from flow analysis
            channel_info: Current channel info (capacity, balance, etc.)
            
        Returns:
            FeeAdjustment if fee was changed, None otherwise
        """
        # Get current fee
        current_fee_ppm = channel_info.get("fee_proportional_millionths", 0)
        if current_fee_ppm == 0:
            current_fee_ppm = self.config.min_fee_ppm  # Initialize if not set
        
        # Load Hill Climbing state
        hc_state = self._get_hill_climb_state(channel_id)
        
        now = int(time.time())
        
        # Calculate current revenue for this observation window
        # Revenue = Volume * Fee_PPM / 1_000_000
        daily_volume = state.get("sats_in", 0) + state.get("sats_out", 0)
        daily_volume = daily_volume // max(self.config.flow_window_days, 1)
        
        current_revenue_sats = (daily_volume * current_fee_ppm) // 1_000_000
        
        # Get capacity and balance for liquidity adjustments
        capacity = channel_info.get("capacity", 1)
        spendable = channel_info.get("spendable_msat", 0) // 1000
        outbound_ratio = spendable / capacity if capacity > 0 else 0.5
        
        bucket = LiquidityBuckets.get_bucket(outbound_ratio)
        liquidity_multiplier = LiquidityBuckets.get_fee_multiplier(bucket)
        
        # Get flow state for bias
        flow_state = state.get("state", "balanced")
        flow_state_multiplier = 1.0
        if flow_state == "source":
            flow_state_multiplier = 1.25  # Sources are scarce - higher fees
        elif flow_state == "sink":
            flow_state_multiplier = 0.80  # Sinks fill for free - lower floor
        
        # Get profitability multiplier (uses marginal ROI now)
        profitability_multiplier = 1.0
        marginal_roi_info = "unknown"
        if self.profitability:
            profitability_multiplier = self.profitability.get_fee_multiplier(channel_id)
            prof_data = self.profitability.get_profitability(channel_id)
            if prof_data:
                marginal_roi_info = f"marginal_roi={prof_data.marginal_roi_percent:.1f}%"
        
        # Calculate floor and ceiling
        floor_ppm = self._calculate_floor(capacity)
        floor_ppm = max(floor_ppm, self.config.min_fee_ppm)
        # Apply flow state to floor (sinks can go lower)
        floor_ppm = int(floor_ppm * flow_state_multiplier)
        floor_ppm = max(floor_ppm, 1)  # Never go below 1 ppm
        
        ceiling_ppm = self.config.max_fee_ppm
        
        # HILL CLIMBING DECISION
        # Compare current revenue to last observed revenue
        revenue_change = current_revenue_sats - hc_state.last_revenue_sats
        last_direction = hc_state.trend_direction
        previous_revenue = hc_state.last_revenue_sats  # Save for logging before update
        
        # Determine new direction based on revenue change
        if hc_state.last_update == 0:
            # First run - start by trying to increase (default direction)
            new_direction = 1
            decision_reason = "initial"
        elif revenue_change > 0:
            # Revenue increased! Keep going in same direction
            new_direction = last_direction
            decision_reason = f"revenue_up_{revenue_change}sats_continue"
            hc_state.consecutive_same_direction += 1
        elif revenue_change < 0:
            # Revenue decreased! Reverse direction
            new_direction = -last_direction
            decision_reason = f"revenue_down_{abs(revenue_change)}sats_reverse"
            hc_state.consecutive_same_direction = 0
        else:
            # Revenue unchanged - try opposite direction (we may be at optimum)
            new_direction = -last_direction
            decision_reason = "revenue_flat_try_opposite"
            hc_state.consecutive_same_direction = 0
        
        # Calculate step size
        # Use percentage OR fixed PPM, whichever is larger
        step_percent = max(current_fee_ppm * self.STEP_PERCENT, self.MIN_STEP_PPM)
        step_ppm = min(max(self.STEP_PPM, step_percent), self.MAX_STEP_PPM)
        
        # Reduce step if we've been moving in same direction too long
        # (we might be oscillating around the optimum)
        if hc_state.consecutive_same_direction > self.MAX_CONSECUTIVE:
            step_ppm = step_ppm // 2
            step_ppm = max(step_ppm, self.MIN_STEP_PPM)
        
        # Calculate base new fee
        base_new_fee = current_fee_ppm + (new_direction * step_ppm)
        
        # Apply multipliers (secondary weighting)
        new_fee_ppm = int(base_new_fee * liquidity_multiplier * profitability_multiplier)
        
        # Enforce floor and ceiling
        new_fee_ppm = max(floor_ppm, min(ceiling_ppm, new_fee_ppm))
        
        # Check if fee changed meaningfully
        fee_change = abs(new_fee_ppm - current_fee_ppm)
        min_change = max(5, current_fee_ppm * 0.03)  # 3% or 5 ppm minimum
        
        # Always update state for tracking
        hc_state.last_revenue_sats = current_revenue_sats
        hc_state.last_fee_ppm = current_fee_ppm
        hc_state.trend_direction = new_direction
        hc_state.last_update = now
        self._save_hill_climb_state(channel_id, hc_state)
        
        if fee_change < min_change:
            # Not enough change to warrant update
            return None
        
        # Build reason string
        reason = (f"HillClimb: revenue={current_revenue_sats}sats ({decision_reason}), "
                 f"direction={'up' if new_direction > 0 else 'down'}, "
                 f"step={step_ppm}ppm, state={flow_state}, "
                 f"liquidity={bucket} ({outbound_ratio:.0%}), "
                 f"{marginal_roi_info}")
        
        # Apply the fee change
        result = self.set_channel_fee(channel_id, new_fee_ppm, reason=reason)
        
        if result.get("success"):
            return FeeAdjustment(
                channel_id=channel_id,
                peer_id=peer_id,
                old_fee_ppm=current_fee_ppm,
                new_fee_ppm=new_fee_ppm,
                reason=reason,
                hill_climb_values={
                    "current_revenue_sats": current_revenue_sats,
                    "previous_revenue_sats": previous_revenue,
                    "revenue_change": revenue_change,
                    "direction": new_direction,
                    "step_ppm": step_ppm,
                    "consecutive_same_direction": hc_state.consecutive_same_direction,
                    "daily_volume": daily_volume
                }
            )
        
        return None
    
    def set_channel_fee(self, channel_id: str, fee_ppm: int, 
                       reason: str = "manual", manual: bool = False) -> Dict[str, Any]:
        """
        Set the fee for a channel, handling clboss override.
        
        MANAGER-OVERRIDE PATTERN:
        1. Get peer ID for the channel
        2. Call clboss-unmanage to prevent conflicts
        3. Set the fee using setchannelfee
        4. Record the change
        
        Args:
            channel_id: Channel to update
            fee_ppm: New fee in parts per million
            reason: Explanation for the change
            manual: True if manually triggered (vs automatic)
            
        Returns:
            Result dict with success status and details
        """
        result = {
            "success": False,
            "channel_id": channel_id,
            "fee_ppm": fee_ppm,
            "message": ""
        }
        
        try:
            # Get channel info to find peer ID and current fee
            channels = self._get_channels_info()
            channel_info = channels.get(channel_id)
            
            if not channel_info:
                result["message"] = f"Channel {channel_id} not found"
                return result
            
            peer_id = channel_info.get("peer_id", "")
            old_fee_ppm = channel_info.get("fee_proportional_millionths", 0)
            
            # Step 1: Unmanage from clboss
            # This is critical - we MUST do this before setting fees
            if not self.clboss.ensure_unmanaged_for_channel(
                channel_id, peer_id, ClbossTags.FEE, self.database
            ):
                self.plugin.log(
                    f"Warning: Could not unmanage {peer_id} from clboss, "
                    "fee may be reverted", level='warn'
                )
            
            # Step 2: Set the fee
            if self.config.dry_run:
                self.plugin.log(f"[DRY RUN] Would set fee for {channel_id} to {fee_ppm} PPM")
                result["success"] = True
                result["message"] = "Dry run - no changes made"
                return result
            
            # Use setchannel command
            # setchannel id [feebase] [feeppm] [htlcmin] [htlcmax] [enforcedelay] [ignorefeelimits]
            self.plugin.rpc.setchannel(
                channel_id,                    # id
                self.config.base_fee_msat,     # feebase (msat)
                fee_ppm                        # feeppm
            )
            
            # Step 3: Record the change
            self.database.record_fee_change(
                channel_id=channel_id,
                peer_id=peer_id,
                old_fee_ppm=old_fee_ppm,
                new_fee_ppm=fee_ppm,
                reason=reason,
                manual=manual
            )
            
            result["success"] = True
            result["old_fee_ppm"] = old_fee_ppm
            result["message"] = f"Fee set to {fee_ppm} PPM"
            
            self.plugin.log(
                f"Set fee for {channel_id[:16]}...: {old_fee_ppm} -> {fee_ppm} PPM "
                f"({reason})"
            )
            
        except RpcError as e:
            result["message"] = f"RPC error: {str(e)}"
            self.plugin.log(f"Failed to set fee for {channel_id}: {e}", level='error')
        except Exception as e:
            result["message"] = f"Error: {str(e)}"
            self.plugin.log(f"Error setting fee: {e}", level='error')
        
        return result
    
    def _calculate_floor(self, capacity_sats: int) -> int:
        """
        Calculate the economic floor fee for a channel.
        
        The floor ensures we never charge less than the channel costs us.
        Uses live mempool fee rates when available for accurate cost estimation.
        
        floor_ppm = (open_cost + close_cost) / estimated_lifetime_volume * 1M
        
        Args:
            capacity_sats: Channel capacity
            
        Returns:
            Minimum fee in PPM
        """
        # Try to get dynamic chain costs from feerates RPC
        dynamic_costs = self._get_dynamic_chain_costs()
        
        if dynamic_costs:
            open_cost = dynamic_costs.get("open_cost_sats", ChainCostDefaults.CHANNEL_OPEN_COST_SATS)
            close_cost = dynamic_costs.get("close_cost_sats", ChainCostDefaults.CHANNEL_CLOSE_COST_SATS)
            
            total_chain_cost = open_cost + close_cost
            estimated_lifetime_volume = ChainCostDefaults.DAILY_VOLUME_SATS * ChainCostDefaults.CHANNEL_LIFETIME_DAYS
            
            if estimated_lifetime_volume > 0:
                floor_ppm = (total_chain_cost / estimated_lifetime_volume) * 1_000_000
                return max(1, int(floor_ppm))
        
        # Fallback to static defaults
        return ChainCostDefaults.calculate_floor_ppm(capacity_sats)
    
    def _get_dynamic_chain_costs(self) -> Optional[Dict[str, int]]:
        """
        Get dynamic chain cost estimates from feerates RPC.
        
        Uses current mempool fee rates to estimate:
        - Channel open cost (funding tx, ~140 vbytes typical)
        - Channel close cost (commitment tx, ~200 vbytes typical)
        
        Returns:
            Dict with open_cost_sats and close_cost_sats, or None if unavailable
        """
        try:
            # Query feerates - prefer 'perkb' style for calculations
            feerates = self.plugin.rpc.feerates(style="perkb")
            
            # Get a medium-term estimate (12 blocks ~2 hours)
            perkb = feerates.get("perkb", {})
            
            # Try different fee rate estimates in order of preference
            sat_per_kvb = (
                perkb.get("opening") or      # CLN's channel opening estimate
                perkb.get("mutual_close") or  # Mutual close estimate  
                perkb.get("unilateral_close") or  # Unilateral close estimate
                perkb.get("floor") or         # Minimum relay fee
                1000                          # Fallback 1 sat/vbyte
            )
            
            # Convert to sat/vbyte
            sat_per_vbyte = sat_per_kvb / 1000
            
            # Typical transaction sizes (conservative estimates)
            # Funding tx: ~140 vbytes (1 input, 2 outputs)
            # Mutual close: ~170 vbytes  
            # Unilateral close: ~200 vbytes (with anchor outputs)
            FUNDING_TX_VBYTES = 140
            CLOSE_TX_VBYTES = 200  # Use unilateral as worst case
            
            open_cost_sats = int(sat_per_vbyte * FUNDING_TX_VBYTES)
            close_cost_sats = int(sat_per_vbyte * CLOSE_TX_VBYTES)
            
            # Sanity bounds
            open_cost_sats = max(500, min(50000, open_cost_sats))
            close_cost_sats = max(300, min(50000, close_cost_sats))
            
            self.plugin.log(
                f"Dynamic chain costs: open={open_cost_sats} sats, close={close_cost_sats} sats "
                f"(at {sat_per_vbyte:.1f} sat/vB)",
                level='debug'
            )
            
            return {
                "open_cost_sats": open_cost_sats,
                "close_cost_sats": close_cost_sats,
                "sat_per_vbyte": sat_per_vbyte
            }
            
        except Exception as e:
            self.plugin.log(f"Error getting feerates: {e}", level='debug')
            return None
    
    def _get_hill_climb_state(self, channel_id: str) -> HillClimbState:
        """
        Get Hill Climbing state for a channel.
        
        Checks in-memory cache first, then database.
        """
        if channel_id in self._hill_climb_states:
            return self._hill_climb_states[channel_id]
        
        # Load from database (uses the fee_strategy_state table)
        db_state = self.database.get_fee_strategy_state(channel_id)
        
        hc_state = HillClimbState(
            last_revenue_sats=db_state.get("last_revenue_sats", 0),
            last_fee_ppm=db_state.get("last_fee_ppm", 0),
            trend_direction=db_state.get("trend_direction", 1),
            last_update=db_state.get("last_update", 0),
            consecutive_same_direction=db_state.get("consecutive_same_direction", 0)
        )
        
        self._hill_climb_states[channel_id] = hc_state
        return hc_state
    
    def _save_hill_climb_state(self, channel_id: str, state: HillClimbState):
        """Save Hill Climbing state to cache and database."""
        self._hill_climb_states[channel_id] = state
        self.database.update_fee_strategy_state(
            channel_id=channel_id,
            last_revenue_sats=state.last_revenue_sats,
            last_fee_ppm=state.last_fee_ppm,
            trend_direction=state.trend_direction,
            consecutive_same_direction=state.consecutive_same_direction
        )
    
    def _get_channels_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Get current info for all channels.
        
        Returns:
            Dict mapping channel_id to channel info
        """
        channels = {}
        
        try:
            result = self.plugin.rpc.listpeerchannels()
            
            for channel in result.get("channels", []):
                if channel.get("state") != "CHANNELD_NORMAL":
                    continue
                
                channel_id = channel.get("short_channel_id") or channel.get("channel_id")
                if channel_id:
                    # Get balance info
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
                        "spendable_msat": spendable_msat,
                        "receivable_msat": receivable_msat,
                        "fee_base_msat": fee_base,
                        "fee_proportional_millionths": fee_ppm
                    }
                    
        except RpcError as e:
            self.plugin.log(f"Error getting channel info: {e}", level='error')
        
        return channels
    
    def reset_hill_climb_state(self, channel_id: str):
        """
        Reset Hill Climbing state for a channel.
        
        Use this when manually intervening or if the controller
        is behaving erratically.
        """
        hc_state = HillClimbState()
        self._save_hill_climb_state(channel_id, hc_state)
        self.plugin.log(f"Reset Hill Climbing state for {channel_id}")


# Keep alias for backward compatibility
PIDFeeController = HillClimbingFeeController
