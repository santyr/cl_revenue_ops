"""
EV-Based Rebalancer module for cl-revenue-ops

MODULE 3: EV-Based Rebalancing (Profit-Aware)

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
- EV = Expected_Future_Fees - Rebalancing_Cost

EV Calculation:
1. Calculate Spread = OutboundFeePPM - InboundFeePPM
   - OutboundFeePPM: Fee we charge to route out of depleted channel
   - InboundFeePPM: Fee peers charge to route into our node (cost to refill)

2. MaxBudget = Spread * RebalanceAmount / 1_000_000
   - This is the maximum we can pay and still be profitable

3. Execution Decision:
   - If MaxBudget > MinProfit: Execute rebalance with budget cap
   - If MaxBudget <= MinProfit: Skip (negative or insufficient EV)

CRITICAL FLOW STATE LOGIC:
- If Target Channel is SINK: ABORT rebalance!
  (Why pay fees to fill a channel that fills itself for free via routing?)
- If Target Channel is SOURCE: HIGH PRIORITY rebalance!
  (This channel prints money; keep it full)

Anti-Thrashing Protection:
- After successful rebalance, unmanage peer from clboss for extended duration
- This prevents clboss from immediately "fixing" our work and wasting fees

The key insight: We should never pay more to rebalance than we expect
to earn from the restored capacity. This prevents the costly rebalancing
loops that plague naive automated systems.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple

from pyln.client import Plugin, RpcError

from .config import Config
from .database import Database
from .clboss_manager import ClbossManager, ClbossTags


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
        spread_ppm: Difference (our fee - their fee)
        max_budget_sats: Maximum fee we should pay
        max_budget_msat: Maximum fee in msat (for RPC calls)
        max_fee_ppm: Maximum fee as PPM (for circular)
        expected_profit_sats: Expected profit if rebalanced
        liquidity_ratio: Current outbound ratio on destination
        dest_flow_state: Flow state of destination (SOURCE/SINK/BALANCED)
    """
    from_channel: str
    to_channel: str
    from_peer_id: str
    to_peer_id: str
    amount_sats: int
    amount_msat: int
    outbound_fee_ppm: int
    inbound_fee_ppm: int
    spread_ppm: int
    max_budget_sats: int
    max_budget_msat: int
    max_fee_ppm: int
    expected_profit_sats: int
    liquidity_ratio: float
    dest_flow_state: str
    
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
            "spread_ppm": self.spread_ppm,
            "max_budget_sats": self.max_budget_sats,
            "max_budget_msat": self.max_budget_msat,
            "max_fee_ppm": self.max_fee_ppm,
            "expected_profit_sats": self.expected_profit_sats,
            "liquidity_ratio": round(self.liquidity_ratio, 4),
            "dest_flow_state": self.dest_flow_state
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
    
    def find_rebalance_candidates(self) -> List[RebalanceCandidate]:
        """
        Find channels that are profitable to rebalance.
        
        This is the main entry point, called periodically by the timer.
        
        Returns:
            List of RebalanceCandidate objects for profitable moves
        """
        candidates = []
        
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
            # Skip if recently attempted
            if self._is_pending(dest_id):
                continue
            
            candidate = self._analyze_rebalance_ev(
                dest_id, dest_info, dest_ratio, source_channels
            )
            
            if candidate:
                candidates.append(candidate)
        
        # Sort by expected profit (highest first)
        candidates.sort(key=lambda c: c.expected_profit_sats, reverse=True)
        
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
        
        # Determine how much to rebalance
        capacity = dest_info.get("capacity", 0)
        spendable = dest_info.get("spendable_sats", 0)
        
        # Target: bring to 50% (balanced)
        target_spendable = capacity // 2
        amount_needed = target_spendable - spendable
        
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
        
        # Calculate spread
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm
        
        # Calculate max budget (what we can pay and still profit)
        # max_budget = spread * amount / 1_000_000
        max_budget_sats = (spread_ppm * rebalance_amount) // 1_000_000
        max_budget_msat = max_budget_sats * 1000
        
        # Calculate max fee as PPM for circular
        # This is the KEY constraint we pass to circular
        # (max_fee_msat / amount_msat) * 1_000_000
        if amount_msat > 0:
            max_fee_ppm = int((max_budget_msat / amount_msat) * 1_000_000)
        else:
            max_fee_ppm = 0
        
        # Calculate expected profit (conservative estimate)
        # Assume we'll route ~10% of the amount we rebalance before needing to rebalance again
        utilization_estimate = 0.10
        expected_routing = rebalance_amount * utilization_estimate
        expected_fee_income = (expected_routing * outbound_fee_ppm) // 1_000_000
        
        # Estimated profit = expected income - max budget (worst case fee)
        expected_profit = expected_fee_income - max_budget_sats
        
        # Check if profitable
        if max_budget_sats < self.config.rebalance_min_profit:
            self.plugin.log(
                f"Skipping {dest_channel}: spread too small "
                f"(max_budget={max_budget_sats} < min={self.config.rebalance_min_profit})"
            )
            return None
        
        if spread_ppm <= 0:
            self.plugin.log(
                f"Skipping {dest_channel}: negative spread "
                f"(outbound={outbound_fee_ppm}, inbound={inbound_fee_ppm})"
            )
            return None
        
        if max_fee_ppm <= 0:
            self.plugin.log(
                f"Skipping {dest_channel}: max_fee_ppm is zero or negative"
            )
            return None
        
        # Find best source channel
        best_source = self._select_source_channel(sources, rebalance_amount)
        if not best_source:
            return None
        
        source_id, source_info = best_source
        
        # Log priority for SOURCE channels
        if dest_flow_state == "source":
            self.plugin.log(
                f"HIGH PRIORITY: {dest_channel} is a SOURCE (money printer). "
                f"Spread={spread_ppm}ppm, MaxFee={max_fee_ppm}ppm"
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
            spread_ppm=spread_ppm,
            max_budget_sats=max_budget_sats,
            max_budget_msat=max_budget_msat,
            max_fee_ppm=max_fee_ppm,
            expected_profit_sats=expected_profit,
            liquidity_ratio=dest_ratio,
            dest_flow_state=dest_flow_state
        )
    
    def _estimate_inbound_fee(self, peer_id: str) -> int:
        """
        Estimate the fee we'll pay to route to ourselves via a peer.
        
        This is a key part of the EV calculation. We need to estimate
        the routing cost based on:
        1. The peer's fees to us (known)
        2. Network fees to reach the peer (estimated)
        
        Args:
            peer_id: The peer we're routing through
            
        Returns:
            Estimated inbound fee in PPM
        """
        # Try to get peer's channel to us
        try:
            # Get our node ID
            info = self.plugin.rpc.getinfo()
            our_node_id = info.get("id", "")
            
            # Try to find peer's channel info
            # This requires looking at gossip data
            channels = self.plugin.rpc.listchannels(source=peer_id)
            
            for channel in channels.get("channels", []):
                if channel.get("destination") == our_node_id:
                    # Get the peer's fee to route to us
                    ppm = channel.get("fee_per_millionth", 0)
                    
                    # Add some buffer for network routing (500 PPM average)
                    network_fee_estimate = 500
                    
                    return ppm + network_fee_estimate
            
        except Exception as e:
            self.plugin.log(f"Could not estimate inbound fee: {e}", level='debug')
        
        # Default estimate if we can't determine exact fee
        return 1000  # 1000 PPM is a reasonable network average
    
    def _select_source_channel(self, sources: List[Tuple[str, Dict[str, Any], float]],
                               amount_needed: int) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        Select the best source channel for rebalancing.
        
        Criteria:
        1. Has enough excess liquidity
        2. Ideally a channel that's filling (sink state)
        3. Low fees (so we're not wasting good outbound)
        
        Args:
            sources: List of source channel candidates
            amount_needed: How much we need to move
            
        Returns:
            (channel_id, channel_info) tuple or None
        """
        best_source = None
        best_score = -1
        
        for channel_id, info, outbound_ratio in sources:
            spendable = info.get("spendable_sats", 0)
            
            # Check if has enough liquidity
            if spendable < amount_needed:
                continue
            
            # Calculate score (higher = better source)
            # Prefer channels with:
            # - Higher outbound ratio (more excess)
            # - Lower fees (not wasting valuable outbound)
            fee_ppm = info.get("fee_ppm", 1000)
            
            # Score: ratio bonus - fee penalty
            score = (outbound_ratio * 100) - (fee_ppm / 100)
            
            # Bonus if channel is sink (filling up)
            state = self.database.get_channel_state(channel_id)
            if state and state.get("state") == "sink":
                score += 20
            
            if score > best_score:
                best_score = score
                best_source = (channel_id, info)
        
        return best_source
    
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
            # Step 1: Unmanage from clboss
            self.clboss.ensure_unmanaged_for_channel(
                candidate.from_channel, candidate.from_peer_id,
                ClbossTags.REBALANCE, self.database
            )
            self.clboss.ensure_unmanaged_for_channel(
                candidate.to_channel, candidate.to_peer_id,
                ClbossTags.REBALANCE, self.database
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
                self.plugin.log(f"Rebalance failed: {error}", level='warning')
                
        except Exception as e:
            result["message"] = f"Error: {str(e)}"
            self.plugin.log(f"Error executing rebalance: {e}", level='error')
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
            # Call circular via RPC with our calculated maxppm constraint
            # This is the KEY integration point - we pass maxppm, not maxfeepercent
            response = self.plugin.rpc.circular(
                candidate.from_channel,      # outgoing_scid
                candidate.to_channel,        # incoming_scid  
                candidate.amount_msat,       # amount in msat
                candidate.max_fee_ppm,       # maxppm - THE CRITICAL CONSTRAINT
                3                            # retry_count
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
                level='warning'
            )
        
        # Calculate EV even for manual
        outbound_fee_ppm = to_info.get("fee_ppm", 0)
        inbound_fee_ppm = self._estimate_inbound_fee(to_info.get("peer_id", ""))
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm
        
        # Convert to msat
        amount_msat = amount_sats * 1000
        
        # Calculate max budget if not provided
        if max_fee_sats is None:
            max_fee_sats = (spread_ppm * amount_sats) // 1_000_000
        
        max_budget_msat = max_fee_sats * 1000
        
        # Calculate max fee PPM for circular
        if amount_msat > 0:
            max_fee_ppm = int((max_budget_msat / amount_msat) * 1_000_000)
        else:
            max_fee_ppm = 0
        
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
            spread_ppm=spread_ppm,
            max_budget_sats=max_fee_sats,
            max_budget_msat=max_budget_msat,
            max_fee_ppm=max_fee_ppm,
            expected_profit_sats=max_fee_sats - (inbound_fee_ppm * amount_sats // 1_000_000),
            liquidity_ratio=to_info.get("spendable_sats", 0) / max(to_info.get("capacity", 1), 1),
            dest_flow_state=dest_flow_state
        )
        
        # Warn if negative EV
        if spread_ppm <= 0:
            self.plugin.log(
                f"Warning: Manual rebalance has negative EV (spread={spread_ppm} PPM)",
                level='warning'
            )
        
        return self.execute_rebalance(candidate)
    
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
                spendable_msat = channel.get("spendable_msat", 0)
                receivable_msat = channel.get("receivable_msat", 0)
                total_msat = channel.get("total_msat", 0)
                
                # Handle different CLN versions
                if total_msat == 0:
                    total_msat = spendable_msat + receivable_msat
                
                channels[channel_id] = {
                    "channel_id": channel_id,
                    "peer_id": channel.get("peer_id", ""),
                    "capacity": total_msat // 1000,
                    "spendable_sats": spendable_msat // 1000,
                    "receivable_sats": receivable_msat // 1000,
                    "fee_base_msat": channel.get("fee_base_msat", 0),
                    "fee_ppm": channel.get("fee_proportional_millionths", 0)
                }
                
        except RpcError as e:
            self.plugin.log(f"Error getting channel balances: {e}", level='error')
        
        return channels
    
    def _is_pending(self, channel_id: str, timeout: int = 600) -> bool:
        """
        Check if a channel has a pending rebalance.
        
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
