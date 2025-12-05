"""
PID Fee Controller module for cl-revenue-ops

MODULE 2: PID Fee Controller (Dynamic Pricing)

This module implements a PID (Proportional-Integral-Derivative) controller
for dynamically adjusting channel fees based on routing flow.

PID Control Theory Applied to Fees:
- Target: A desired flow rate (e.g., 100,000 sats/day routed)
- Error: Difference between actual and target flow
- P (Proportional): React to current error
- I (Integral): Account for accumulated error over time
- D (Derivative): React to rate of change of error

Fee Adjustment Logic:
- If ActualFlow > Target: Increase fees (capture margin, slow drain)
- If ActualFlow < Target: Decrease fees (encourage volume)

Constraints:
- Never drop below floor (economic minimum based on chain costs)
- Never exceed ceiling (prevent absurd fees)
- Unmanage from clboss before setting fees

The PID controller provides smooth, stable fee adjustments that avoid
the oscillation problems of simple threshold-based approaches.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple

from pyln.client import Plugin, RpcError

from .config import Config, ChainCostDefaults, LiquidityBuckets
from .database import Database
from .clboss_manager import ClbossManager, ClbossTags


@dataclass
class PIDState:
    """
    State of a PID controller for one channel.
    
    Attributes:
        integral: Accumulated error over time
        last_error: Error from previous iteration (for derivative)
        last_fee_ppm: Last fee we set
        last_update: Timestamp of last update
    """
    integral: float = 0.0
    last_error: float = 0.0
    last_fee_ppm: int = 0
    last_update: int = 0


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
        pid_values: PID controller internal values
    """
    channel_id: str
    peer_id: str
    old_fee_ppm: int
    new_fee_ppm: int
    reason: str
    pid_values: Dict[str, float]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "channel_id": self.channel_id,
            "peer_id": self.peer_id,
            "old_fee_ppm": self.old_fee_ppm,
            "new_fee_ppm": self.new_fee_ppm,
            "reason": self.reason,
            "pid_values": self.pid_values
        }


class PIDFeeController:
    """
    PID-based fee controller for dynamic channel pricing.
    
    The controller aims to maintain a target flow rate by adjusting fees.
    It uses a PID algorithm for smooth, stable adjustments.
    
    Key Features:
    1. Flow-based targeting: Adjusts fees based on actual routing volume
    2. Liquidity-aware: Considers channel balance when setting fees
    3. Economic floor: Never charges less than channel costs
    4. clboss override: Unmanages from clboss before setting fees
    """
    
    def __init__(self, plugin: Plugin, config: Config, database: Database, 
                 clboss_manager: ClbossManager):
        """
        Initialize the fee controller.
        
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
        
        # In-memory cache of PID states (also persisted to DB)
        self._pid_states: Dict[str, PIDState] = {}
    
    def adjust_all_fees(self) -> List[FeeAdjustment]:
        """
        Adjust fees for all channels based on PID control.
        
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
        Adjust fee for a single channel using PID control.
        
        PID Formula:
        output = Kp * error + Ki * integral + Kd * derivative
        
        Where:
        - error = target_flow - actual_flow
        - integral = sum of errors over time
        - derivative = change in error since last iteration
        
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
        
        # Load PID state
        pid_state = self._get_pid_state(channel_id)
        
        # Calculate error
        # Error = Target - Actual (positive = below target, need lower fees)
        daily_volume = state.get("sats_in", 0) + state.get("sats_out", 0)
        daily_volume = daily_volume // max(self.config.flow_window_days, 1)
        
        target_flow = self.config.target_flow
        error = target_flow - daily_volume
        
        # Time since last update (for integral and derivative)
        now = int(time.time())
        dt = max(1, now - pid_state.last_update) if pid_state.last_update > 0 else self.config.fee_interval
        dt_hours = dt / 3600.0  # Convert to hours for stable scaling
        
        # Calculate PID components
        # P: Proportional to current error
        p_term = self.config.pid_kp * error
        
        # I: Integral of error over time
        pid_state.integral += error * dt_hours
        # Clamp integral to prevent windup
        pid_state.integral = max(-self.config.pid_integral_max, 
                                 min(self.config.pid_integral_max, pid_state.integral))
        i_term = self.config.pid_ki * pid_state.integral
        
        # D: Derivative (rate of change of error)
        # CRITICAL: Skip derivative on first run to prevent spike
        # When last_update == 0, we have no valid previous error to compare
        if pid_state.last_update == 0:
            derivative = 0.0
        else:
            derivative = (error - pid_state.last_error) / max(dt_hours, 0.1)
        d_term = self.config.pid_kd * derivative
        
        # Combined PID output (in fee adjustment direction)
        # Negative output = increase fees (below target flow)
        # Positive output = decrease fees (above target flow)
        pid_output = p_term + i_term + d_term
        
        # Clamp output
        pid_output = max(-self.config.pid_output_max, 
                        min(self.config.pid_output_max, pid_output))
        
        # Convert PID output to fee adjustment
        # Scale: output in "flow sats" -> fee PPM adjustment
        # Higher flow deficit -> decrease fee more
        # Flow surplus -> increase fee
        fee_adjustment = -pid_output / 1000  # Scale factor
        
        # Apply liquidity-based multiplier
        capacity = channel_info.get("capacity", 1)
        spendable = channel_info.get("spendable_msat", 0) // 1000
        outbound_ratio = spendable / capacity if capacity > 0 else 0.5
        
        bucket = LiquidityBuckets.get_bucket(outbound_ratio)
        liquidity_multiplier = LiquidityBuckets.get_fee_multiplier(bucket)
        
        # Apply flow state bias
        # SOURCE channels are money printers (draining) - charge higher fees (scarce)
        # SINK channels fill for free - lower fees to encourage outflow
        flow_state = state.get("state", "balanced")
        flow_state_multiplier = 1.0
        if flow_state == "source":
            flow_state_multiplier = 1.25  # 25% higher fees for sources
        elif flow_state == "sink":
            flow_state_multiplier = 0.80  # 20% lower fees for sinks
        
        # Calculate new fee
        base_fee = current_fee_ppm + fee_adjustment
        new_fee_ppm = int(base_fee * liquidity_multiplier * flow_state_multiplier)
        
        # Apply floor and ceiling
        floor_ppm = self._calculate_floor(capacity)
        floor_ppm = max(floor_ppm, self.config.min_fee_ppm)
        ceiling_ppm = self.config.max_fee_ppm
        
        new_fee_ppm = max(floor_ppm, min(ceiling_ppm, new_fee_ppm))
        
        # Check if fee changed significantly (at least 5% or 5 PPM)
        fee_change = abs(new_fee_ppm - current_fee_ppm)
        min_change = max(5, current_fee_ppm * 0.05)
        
        if fee_change < min_change:
            # Not enough change to warrant update
            # Still update PID state for next iteration
            pid_state.last_error = error
            pid_state.last_update = now
            self._save_pid_state(channel_id, pid_state)
            return None
        
        # Build reason string (flow_state already defined above for multiplier)
        reason = (f"PID adjustment: flow={daily_volume}/day (target={target_flow}), "
                 f"state={flow_state} (x{flow_state_multiplier}), liquidity={bucket} ({outbound_ratio:.0%})")
        
        # Apply the fee change
        result = self.set_channel_fee(channel_id, new_fee_ppm, reason=reason)
        
        if result.get("success"):
            # Update PID state
            pid_state.last_error = error
            pid_state.last_fee_ppm = new_fee_ppm
            pid_state.last_update = now
            self._save_pid_state(channel_id, pid_state)
            
            return FeeAdjustment(
                channel_id=channel_id,
                peer_id=peer_id,
                old_fee_ppm=current_fee_ppm,
                new_fee_ppm=new_fee_ppm,
                reason=reason,
                pid_values={
                    "error": error,
                    "p_term": p_term,
                    "i_term": i_term,
                    "d_term": d_term,
                    "output": pid_output,
                    "integral": pid_state.integral
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
                    "fee may be reverted", level='warning'
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
        
        floor_ppm = (open_cost + close_cost) / estimated_lifetime_volume * 1M
        
        Args:
            capacity_sats: Channel capacity
            
        Returns:
            Minimum fee in PPM
        """
        return ChainCostDefaults.calculate_floor_ppm(capacity_sats)
    
    def _get_pid_state(self, channel_id: str) -> PIDState:
        """
        Get PID controller state for a channel.
        
        Checks in-memory cache first, then database.
        """
        if channel_id in self._pid_states:
            return self._pid_states[channel_id]
        
        # Load from database
        db_state = self.database.get_pid_state(channel_id)
        
        pid_state = PIDState(
            integral=db_state.get("integral", 0.0),
            last_error=db_state.get("last_error", 0.0),
            last_fee_ppm=db_state.get("last_fee_ppm", 0),
            last_update=db_state.get("last_update", 0)
        )
        
        self._pid_states[channel_id] = pid_state
        return pid_state
    
    def _save_pid_state(self, channel_id: str, state: PIDState):
        """Save PID state to cache and database."""
        self._pid_states[channel_id] = state
        self.database.update_pid_state(
            channel_id=channel_id,
            integral=state.integral,
            last_error=state.last_error,
            last_fee_ppm=state.last_fee_ppm
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
    
    def reset_pid_state(self, channel_id: str):
        """
        Reset PID state for a channel.
        
        Use this when manually intervening or if the controller
        is behaving erratically.
        """
        pid_state = PIDState()
        self._save_pid_state(channel_id, pid_state)
        self.plugin.log(f"Reset PID state for {channel_id}")
