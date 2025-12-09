"""
EV-Based Rebalancer module for cl-revenue-ops

MODULE 3: EV-Based Rebalancing (Profit-Aware with Opportunity Cost)

This module implements Expected Value (EV) based rebalancing decisions.
Unlike clboss which often makes negative EV rebalances, this module only
triggers rebalances when the math shows positive expected profit.

Architecture Pattern: "Strategist, Manager, and Driver"
- STRATEGIST (EVRebalancer): Calculates EV, determines IF and HOW MUCH to rebalance
- MANAGER (JobManager): Manages lifecycle of background sling jobs
- DRIVER (Sling plugin): Actually executes the payments in the background

Phase 4: Async Job Queue
- Decouples decision-making from execution
- Allows concurrent rebalancing attempts
- Uses sling-job (background) instead of sling-once (blocking)
"""

import time
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple, TYPE_CHECKING
from enum import Enum

from pyln.client import Plugin, RpcError

from .config import Config
from .database import Database
from .clboss_manager import ClbossManager, ClbossTags
from .metrics import PrometheusExporter, MetricNames, METRIC_HELP

if TYPE_CHECKING:
    from .profitability_analyzer import ChannelProfitabilityAnalyzer


class JobStatus(Enum):
    """Status of a sling background job."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    STOPPED = "stopped"


@dataclass
class RebalanceCandidate:
    """A candidate for rebalancing."""
    from_channel: str
    to_channel: str
    from_peer_id: str
    to_peer_id: str
    amount_sats: int
    amount_msat: int
    outbound_fee_ppm: int
    inbound_fee_ppm: int
    source_fee_ppm: int
    weighted_opp_cost_ppm: int
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


@dataclass
class ActiveJob:
    """Tracks an active sling background job."""
    scid: str                      # Target channel SCID (colon format for sling)
    scid_normalized: str           # Original SCID format (for our tracking)
    from_scid: str                 # Source channel SCID
    start_time: int                # Unix timestamp when job started
    candidate: RebalanceCandidate  # Original candidate data
    rebalance_id: int              # Database record ID
    target_amount_sats: int        # Total amount we want to rebalance
    initial_local_sats: int        # Local balance when job started
    max_fee_ppm: int               # Max fee rate for this job
    status: JobStatus = JobStatus.PENDING


class JobManager:
    """
    Manages the lifecycle of Sling background rebalancing jobs.
    
    Responsibilities:
    - Start new sling-job workers
    - Monitor job progress via sling-stats
    - Stop jobs on success, timeout, or error
    - Record results to database
    
    Key Design Decision:
    We use sling-job for TACTICAL rebalancing (one-off moves), not permanent
    pegging. As soon as any successful payment is detected or timeout is reached,
    we DELETE the job to prevent infinite spending.
    """
    
    # Default timeout: 2 hours (configurable)
    DEFAULT_JOB_TIMEOUT_SECONDS = 7200
    
    def __init__(self, plugin: Plugin, config: Config, database: Database,
                 metrics_exporter: Optional[PrometheusExporter] = None):
        self.plugin = plugin
        self.config = config
        self.database = database
        self.metrics = metrics_exporter
        
        # Active jobs indexed by target channel SCID (normalized format)
        self._active_jobs: Dict[str, ActiveJob] = {}
        
        # Configurable settings
        self.job_timeout_seconds = getattr(config, 'sling_job_timeout_seconds', 
                                           self.DEFAULT_JOB_TIMEOUT_SECONDS)
        self.max_concurrent_jobs = getattr(config, 'max_concurrent_jobs', 5)
        
        # Chunk size for sling rebalances (sats per attempt)
        self.chunk_size_sats = getattr(config, 'sling_chunk_size_sats', 500000)
    
    @property
    def active_job_count(self) -> int:
        """Returns the number of currently active jobs."""
        return len(self._active_jobs)
    
    @property
    def active_channels(self) -> List[str]:
        """Returns list of channel SCIDs with active jobs."""
        return list(self._active_jobs.keys())
    
    def has_active_job(self, channel_id: str) -> bool:
        """Check if a channel has an active rebalance job."""
        normalized = self._normalize_scid(channel_id)
        return normalized in self._active_jobs
    
    def slots_available(self) -> int:
        """Returns number of available job slots."""
        return max(0, self.max_concurrent_jobs - self.active_job_count)
    
    def _normalize_scid(self, scid: str) -> str:
        """Normalize SCID to consistent format (with 'x' separators)."""
        return scid.replace(':', 'x')
    
    def _to_sling_scid(self, scid: str) -> str:
        """Convert SCID to sling's expected colon format."""
        return scid.replace('x', ':')
    
    def _get_channel_local_balance(self, channel_id: str) -> int:
        """Get current local balance of a channel in sats."""
        try:
            listfunds = self.plugin.rpc.listfunds()
            normalized = self._normalize_scid(channel_id)
            
            for channel in listfunds.get("channels", []):
                scid = channel.get("short_channel_id", "")
                if self._normalize_scid(scid) == normalized:
                    our_amount_msat = channel.get("our_amount_msat", 0)
                    if isinstance(our_amount_msat, str):
                        our_amount_msat = int(our_amount_msat.replace("msat", ""))
                    return our_amount_msat // 1000
        except Exception as e:
            self.plugin.log(f"Error getting channel balance: {e}", level='debug')
        return 0
    
    def start_job(self, candidate: RebalanceCandidate, rebalance_id: int) -> Dict[str, Any]:
        """
        Start a new sling-job for the given candidate.
        
        sling-job creates a persistent background worker that will keep
        attempting to rebalance until stopped or target is reached.
        
        Args:
            candidate: The rebalance candidate with all parameters
            rebalance_id: Database record ID for this rebalance attempt
            
        Returns:
            Dict with 'success' bool and 'message' or 'error'
        """
        normalized_scid = self._normalize_scid(candidate.to_channel)
        
        # Check if job already exists
        if normalized_scid in self._active_jobs:
            return {"success": False, "error": "Job already exists for this channel"}
        
        # Check slot availability
        if self.active_job_count >= self.max_concurrent_jobs:
            return {"success": False, "error": "No job slots available"}
        
        # Convert SCIDs to sling format (colon-separated)
        to_scid = self._to_sling_scid(candidate.to_channel)
        from_scid = self._to_sling_scid(candidate.from_channel)
        
        # Get initial balance for progress tracking
        initial_balance = self._get_channel_local_balance(candidate.to_channel)
        
        # Calculate chunk size (amount per rebalance attempt)
        chunk_size = min(candidate.amount_sats, self.chunk_size_sats)
        
        try:
            # Build candidates JSON array
            candidates_json = json.dumps([from_scid])
            
            self.plugin.log(
                f"Starting sling-job: {to_scid} <- {from_scid}, "
                f"amount={chunk_size}, maxppm={candidate.max_fee_ppm}, "
                f"target_total={candidate.amount_sats}"
            )
            
            # Start sling-job with keyword arguments
            # sling-job -k scid=X direction=pull amount=Y maxppm=Z candidates='["A"]'
            self.plugin.rpc.call("sling-job", {
                "scid": to_scid,
                "direction": "pull",
                "amount": chunk_size,
                "maxppm": candidate.max_fee_ppm,
                "candidates": candidates_json,
                "target": 0.5  # Stop when 50% balance reached (we'll stop earlier ourselves)
            })
            
            # Start the job (sling-job only creates it, sling-go starts it)
            try:
                self.plugin.rpc.call("sling-go", {"scid": to_scid})
            except RpcError as e:
                # sling-go might fail if job auto-started, that's OK
                if "already running" not in str(e).lower():
                    self.plugin.log(f"sling-go warning: {e}", level='debug')
            
            # Track the job
            job = ActiveJob(
                scid=to_scid,
                scid_normalized=normalized_scid,
                from_scid=from_scid,
                start_time=int(time.time()),
                candidate=candidate,
                rebalance_id=rebalance_id,
                target_amount_sats=candidate.amount_sats,
                initial_local_sats=initial_balance,
                max_fee_ppm=candidate.max_fee_ppm,
                status=JobStatus.RUNNING
            )
            self._active_jobs[normalized_scid] = job
            
            self.plugin.log(f"Sling job started for {to_scid}, tracking as {normalized_scid}")
            
            return {"success": True, "message": f"Job started for {to_scid}"}
            
        except RpcError as e:
            error_msg = str(e)
            self.plugin.log(f"Failed to start sling-job: {error_msg}", level='warn')
            return {"success": False, "error": f"Sling RPC error: {error_msg}"}
        except Exception as e:
            self.plugin.log(f"Error starting sling-job: {e}", level='error')
            return {"success": False, "error": str(e)}
    
    def stop_job(self, channel_id: str, reason: str = "manual") -> bool:
        """
        Stop and delete a sling job.
        
        Args:
            channel_id: Channel SCID (any format)
            reason: Why the job is being stopped (for logging)
            
        Returns:
            True if job was stopped, False if not found or error
        """
        normalized = self._normalize_scid(channel_id)
        job = self._active_jobs.get(normalized)
        
        if not job:
            return False
        
        try:
            # First stop the job gracefully
            try:
                self.plugin.rpc.call("sling-stop", {"scid": job.scid})
            except RpcError:
                pass  # May already be stopped
            
            # Then delete it to prevent restart
            try:
                self.plugin.rpc.call("sling-deletejob", {
                    "job": job.scid,
                    "delete_stats": False  # Keep stats for analysis
                })
            except RpcError as e:
                self.plugin.log(f"sling-deletejob warning: {e}", level='debug')
            
            self.plugin.log(f"Stopped sling job {job.scid} (reason: {reason})")
            
        except Exception as e:
            self.plugin.log(f"Error stopping job {job.scid}: {e}", level='warn')
        
        # Remove from tracking regardless
        del self._active_jobs[normalized]
        return True
    
    def monitor_jobs(self) -> Dict[str, Any]:
        """
        Monitor all active jobs and handle completed/failed/timed-out ones.
        
        This should be called periodically (e.g., every rebalance interval).
        
        Returns:
            Summary dict with counts of various outcomes
        """
        summary = {
            "checked": 0,
            "completed": 0,
            "failed": 0,
            "timed_out": 0,
            "still_running": 0
        }
        
        if not self._active_jobs:
            return summary
        
        # Get current time for timeout checks
        now = int(time.time())
        
        # Get sling stats for all jobs
        sling_stats = self._get_sling_stats()
        
        # Copy keys to avoid modifying dict during iteration
        job_scids = list(self._active_jobs.keys())
        
        for normalized_scid in job_scids:
            job = self._active_jobs.get(normalized_scid)
            if not job:
                continue
                
            summary["checked"] += 1
            
            # Check timeout first
            elapsed = now - job.start_time
            if elapsed > self.job_timeout_seconds:
                self._handle_job_timeout(job)
                summary["timed_out"] += 1
                continue
            
            # Check current channel balance for progress
            current_balance = self._get_channel_local_balance(job.scid_normalized)
            amount_transferred = current_balance - job.initial_local_sats
            
            # Get job-specific stats from sling
            job_stats = sling_stats.get(job.scid, {})
            
            # Check for success: any positive transfer means we've achieved something
            if amount_transferred > 0:
                self._handle_job_success(job, amount_transferred, job_stats)
                summary["completed"] += 1
                continue
            
            # Check for sling-reported errors
            if self._check_job_error(job, job_stats):
                self._handle_job_failure(job, job_stats)
                summary["failed"] += 1
                continue
            
            # Job still running
            summary["still_running"] += 1
            self.plugin.log(
                f"Job {job.scid} running: {elapsed}s elapsed, "
                f"transferred={amount_transferred} sats",
                level='debug'
            )
        
        return summary
    
    def _get_sling_stats(self) -> Dict[str, Dict[str, Any]]:
        """Query sling-stats for all jobs and return as dict keyed by SCID."""
        stats = {}
        try:
            # sling-stats with json=true returns structured data
            result = self.plugin.rpc.call("sling-stats", {"json": True})
            
            if isinstance(result, dict):
                # Result might be keyed by SCID or be a list
                if "jobs" in result:
                    for job in result["jobs"]:
                        scid = job.get("scid", "")
                        if scid:
                            stats[scid] = job
                else:
                    # Assume dict is already keyed by SCID
                    stats = result
            elif isinstance(result, list):
                for job in result:
                    scid = job.get("scid", "")
                    if scid:
                        stats[scid] = job
                        
        except RpcError as e:
            self.plugin.log(f"sling-stats error: {e}", level='debug')
        except Exception as e:
            self.plugin.log(f"Error getting sling stats: {e}", level='debug')
        
        return stats
    
    def _check_job_error(self, job: ActiveJob, stats: Dict[str, Any]) -> bool:
        """Check if sling reports an error state for this job."""
        # Check for explicit error status
        status = stats.get("status", "").lower()
        if status in ("error", "failed", "stopped"):
            return True
        
        # Check for high consecutive failure count
        consecutive_failures = stats.get("consecutive_failures", 0)
        if consecutive_failures >= 10:
            return True
        
        return False
    
    def _handle_job_success(self, job: ActiveJob, amount_transferred: int, 
                            stats: Dict[str, Any]) -> None:
        """Handle a successfully completed job."""
        # Calculate actual fee paid (from sling stats if available)
        fee_sats = stats.get("fee_total_sats", 0)
        if not fee_sats:
            fee_msat = stats.get("fee_total_msat", 0)
            fee_sats = fee_msat // 1000 if fee_msat else 0
        
        # Estimate fee from amount if sling doesn't report it
        if fee_sats == 0 and amount_transferred > 0:
            # Use a conservative estimate based on max_fee_ppm
            fee_sats = (amount_transferred * job.max_fee_ppm) // 1_000_000
        
        # Calculate actual profit
        expected_profit = job.candidate.expected_profit_sats
        actual_profit = expected_profit - fee_sats
        
        self.plugin.log(
            f"Rebalance SUCCESS: {job.scid} filled with {amount_transferred} sats. "
            f"Fee: {fee_sats} sats, Profit: {actual_profit} sats"
        )
        
        # Update database
        self.database.update_rebalance_result(
            job.rebalance_id, 
            'success', 
            fee_sats, 
            actual_profit
        )
        self.database.reset_failure_count(job.scid_normalized)
        
        # Update metrics
        if self.metrics:
            self.metrics.inc_counter(
                MetricNames.REBALANCE_COST_TOTAL_SATS, 
                fee_sats, 
                {"channel_id": job.scid_normalized}
            )
        
        # Stop the job
        self.stop_job(job.scid_normalized, reason="success")
    
    def _handle_job_failure(self, job: ActiveJob, stats: Dict[str, Any]) -> None:
        """Handle a failed job."""
        error_msg = stats.get("last_error", "Unknown error from sling")
        
        self.plugin.log(
            f"Rebalance FAILED: {job.scid} - {error_msg}",
            level='warn'
        )
        
        # Update database
        self.database.update_rebalance_result(
            job.rebalance_id,
            'failed',
            error_message=error_msg
        )
        self.database.increment_failure_count(job.scid_normalized)
        
        # Stop the job
        self.stop_job(job.scid_normalized, reason="failure")
    
    def _handle_job_timeout(self, job: ActiveJob) -> None:
        """Handle a timed-out job."""
        elapsed_hours = (int(time.time()) - job.start_time) / 3600
        
        # Check if any progress was made
        current_balance = self._get_channel_local_balance(job.scid_normalized)
        amount_transferred = current_balance - job.initial_local_sats
        
        if amount_transferred > 0:
            # Partial success - still record the progress
            self.plugin.log(
                f"Rebalance TIMEOUT (partial): {job.scid} after {elapsed_hours:.1f}h. "
                f"Transferred {amount_transferred} sats before timeout."
            )
            self.database.update_rebalance_result(
                job.rebalance_id,
                'partial',
                fee_paid_sats=0,  # Unknown actual fee
                actual_profit_sats=0
            )
        else:
            self.plugin.log(
                f"Rebalance TIMEOUT: {job.scid} after {elapsed_hours:.1f}h with no progress",
                level='warn'
            )
            self.database.update_rebalance_result(
                job.rebalance_id,
                'timeout',
                error_message=f"Timeout after {elapsed_hours:.1f} hours"
            )
            self.database.increment_failure_count(job.scid_normalized)
        
        # Stop the job
        self.stop_job(job.scid_normalized, reason="timeout")
    
    def stop_all_jobs(self, reason: str = "shutdown") -> int:
        """Stop all active jobs. Returns count of jobs stopped."""
        count = 0
        for scid in list(self._active_jobs.keys()):
            if self.stop_job(scid, reason=reason):
                count += 1
        return count
    
    def get_job_status(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """Get status info for a specific job."""
        normalized = self._normalize_scid(channel_id)
        job = self._active_jobs.get(normalized)
        
        if not job:
            return None
        
        elapsed = int(time.time()) - job.start_time
        current_balance = self._get_channel_local_balance(normalized)
        transferred = current_balance - job.initial_local_sats
        
        return {
            "scid": job.scid,
            "from_scid": job.from_scid,
            "status": job.status.value,
            "elapsed_seconds": elapsed,
            "target_amount_sats": job.target_amount_sats,
            "transferred_sats": transferred,
            "progress_pct": round(transferred / job.target_amount_sats * 100, 1) if job.target_amount_sats > 0 else 0,
            "max_fee_ppm": job.max_fee_ppm
        }
    
    def get_all_jobs_status(self) -> List[Dict[str, Any]]:
        """Get status info for all active jobs."""
        return [
            self.get_job_status(scid) 
            for scid in self._active_jobs.keys()
            if self.get_job_status(scid)
        ]


class EVRebalancer:
    """
    Expected Value based rebalancer with async job queue support.
    
    This class acts as the "Strategist" - it calculates EV and determines
    IF and HOW MUCH to rebalance. The actual execution is delegated to
    the JobManager which manages sling background jobs.
    """
    
    def __init__(self, plugin: Plugin, config: Config, database: Database,
                 clboss_manager: ClbossManager,
                 metrics_exporter: Optional[PrometheusExporter] = None):
        self.plugin = plugin
        self.config = config
        self.database = database
        self.clboss = clboss_manager
        self.metrics = metrics_exporter
        self._pending: Dict[str, int] = {}
        self._our_node_id: Optional[str] = None
        self._profitability_analyzer: Optional['ChannelProfitabilityAnalyzer'] = None
        
        # Initialize job manager for async execution
        self.job_manager = JobManager(plugin, config, database, metrics_exporter)
    
    def _get_our_node_id(self) -> str:
        if self._our_node_id is None:
            try:
                info = self.plugin.rpc.getinfo()
                self._our_node_id = info.get("id", "")
            except Exception as e:
                self.plugin.log(f"Error getting our node ID: {e}", level='error')
                self._our_node_id = ""
        return self._our_node_id
    
    def set_profitability_analyzer(self, analyzer: 'ChannelProfitabilityAnalyzer') -> None:
        self._profitability_analyzer = analyzer
    
    def find_rebalance_candidates(self) -> List[RebalanceCandidate]:
        """
        Find channels that would benefit from rebalancing.
        
        This method:
        1. First monitors existing jobs to clean up finished ones
        2. Filters out channels with active jobs
        3. Respects max concurrent job limit
        4. Returns prioritized list of candidates
        """
        candidates = []
        
        # First, monitor existing jobs and clean up finished ones
        if self.job_manager.active_job_count > 0:
            monitor_result = self.job_manager.monitor_jobs()
            self.plugin.log(
                f"Job monitor: {monitor_result['checked']} checked, "
                f"{monitor_result['completed']} completed, "
                f"{monitor_result['failed']} failed, "
                f"{monitor_result['timed_out']} timed out, "
                f"{monitor_result['still_running']} running"
            )
        
        # Check if we have slots available
        available_slots = self.job_manager.slots_available()
        if available_slots <= 0:
            self.plugin.log(
                f"No rebalance slots available ({self.job_manager.active_job_count}/"
                f"{self.job_manager.max_concurrent_jobs} jobs active)"
            )
            return candidates
        
        # Check capital controls
        if not self._check_capital_controls():
            return candidates
        
        channels = self._get_channels_with_balances()
        if not channels:
            return candidates
        
        # Get set of channels with active jobs
        active_channels = set(self.job_manager.active_channels)
        
        depleted_channels = []
        source_channels = []
        
        for channel_id, info in channels.items():
            capacity = info.get("capacity", 0)
            spendable = info.get("spendable_sats", 0)
            if capacity == 0: 
                continue
            
            outbound_ratio = spendable / capacity
            
            # Skip channels with active jobs
            normalized = channel_id.replace(':', 'x')
            if normalized in active_channels:
                continue
            
            if outbound_ratio < self.config.low_liquidity_threshold:
                depleted_channels.append((channel_id, info, outbound_ratio))
            elif outbound_ratio > self.config.high_liquidity_threshold:
                source_channels.append((channel_id, info, outbound_ratio))
        
        if not depleted_channels or not source_channels:
            return candidates
            
        self.plugin.log(
            f"Found {len(depleted_channels)} depleted and {len(source_channels)} source channels "
            f"(excluding {len(active_channels)} with active jobs)"
        )
        
        for dest_id, dest_info, dest_ratio in depleted_channels:
            if self._is_pending_with_backoff(dest_id): 
                continue
            
            last_rebalance = self.database.get_last_rebalance_time(dest_id)
            if last_rebalance:
                cooldown = self.config.rebalance_cooldown_hours * 3600
                if int(time.time()) - last_rebalance < cooldown: 
                    continue
            
            candidate = self._analyze_rebalance_ev(dest_id, dest_info, dest_ratio, source_channels)
            if candidate:
                candidates.append(candidate)
                
                # Stop if we have enough candidates to fill available slots
                if len(candidates) >= available_slots:
                    break
        
        # Sort by priority
        def sort_key(c):
            dest_state = self.database.get_channel_state(c.to_channel)
            flow_state = dest_state.get("state", "balanced") if dest_state else "balanced"
            priority = 2 if flow_state == "source" else 1
            return (priority, c.expected_profit_sats)
        
        candidates.sort(key=sort_key, reverse=True)
        
        # Limit to available slots
        return candidates[:available_slots]

    def _analyze_rebalance_ev(self, dest_channel: str, dest_info: Dict[str, Any],
                              dest_ratio: float,
                              sources: List[Tuple[str, Dict[str, Any], float]]) -> Optional[RebalanceCandidate]:
        """Analyze expected value of rebalancing a channel."""
        dest_state = self.database.get_channel_state(dest_channel)
        dest_flow_state = dest_state.get("state", "unknown") if dest_state else "unknown"
        
        if dest_flow_state == "sink": 
            return None
        
        # Check profitability logic
        if self._profitability_analyzer:
            try:
                prof = self._profitability_analyzer.analyze_channel(dest_channel)
                if prof and prof.classification.value == "zombie": 
                    return None
                if prof and prof.classification.value == "underwater" and prof.marginal_roi <= 0:
                    return None
            except Exception: 
                pass

        capacity = dest_info.get("capacity", 0)
        spendable = dest_info.get("spendable_sats", 0)
        
        # Dynamic targeting
        if dest_flow_state == "source": 
            target_ratio = 0.85
        elif dest_flow_state == "sink": 
            target_ratio = 0.15
        else: 
            target_ratio = 0.50
        
        target_spendable = int(capacity * target_ratio)
        amount_needed = target_spendable - spendable
        if amount_needed <= 0: 
            return None
        
        rebalance_amount = max(self.config.rebalance_min_amount, 
                             min(self.config.rebalance_max_amount, amount_needed))
        amount_msat = rebalance_amount * 1000
        
        outbound_fee_ppm = dest_info.get("fee_ppm", 0)
        inbound_fee_ppm = self._estimate_inbound_fee(dest_info.get("peer_id", ""))
        
        best_source = self._select_source_channel(sources, rebalance_amount, dest_channel)
        if not best_source: 
            return None
        source_id, source_info = best_source
        
        source_fee_ppm = source_info.get("fee_ppm", 0)
        source_capacity = source_info.get("capacity", 1)
        source_turnover_rate = self._calculate_turnover_rate(source_id, source_capacity)
        
        turnover_weight = min(1.0, source_turnover_rate * 7)
        weighted_opp_cost = int(source_fee_ppm * turnover_weight)
        spread_ppm = outbound_fee_ppm - inbound_fee_ppm - weighted_opp_cost
        
        if spread_ppm <= 0: 
            return None
        
        max_budget_sats = (spread_ppm * rebalance_amount) // 1_000_000
        max_budget_msat = max_budget_sats * 1000
        
        # Kelly logic
        if self.config.enable_kelly:
            reputation = self.database.get_peer_reputation(dest_info.get("peer_id", ""))
            p = reputation.get('score', 0.5)
            cost_ppm = inbound_fee_ppm + weighted_opp_cost
            b = outbound_fee_ppm / cost_ppm if cost_ppm > 0 else float('inf')
            kelly_f = p - (1 - p) / b if b > 0 else -1.0
            kelly_safe = min(kelly_f * self.config.kelly_fraction, 1.0)
            
            if kelly_safe <= 0: 
                return None
            max_budget_sats = int(max_budget_sats * kelly_safe)
            max_budget_msat = max_budget_sats * 1000

        if amount_msat > 0:
            effective_max_fee_ppm = inbound_fee_ppm + (spread_ppm // 2)
            max_fee_ppm = max(1, min(effective_max_fee_ppm, spread_ppm + inbound_fee_ppm))
        else:
            max_fee_ppm = 0
            
        if max_fee_ppm <= 0: 
            return None
        
        dest_turnover_rate = self._calculate_turnover_rate(dest_channel, capacity)
        cooldown_days = self.config.rebalance_cooldown_hours / 24.0
        expected_utilization = max(min(dest_turnover_rate * cooldown_days, 1.0), 0.05)
        
        expected_income = (rebalance_amount * expected_utilization * outbound_fee_ppm) // 1_000_000
        expected_source_loss = (rebalance_amount * expected_utilization * source_fee_ppm * turnover_weight) // 1_000_000
        expected_profit = expected_income - max_budget_sats - expected_source_loss
        
        if expected_profit < self.config.rebalance_min_profit: 
            return None
        
        return RebalanceCandidate(
            from_channel=source_id, to_channel=dest_channel,
            from_peer_id=source_info.get("peer_id", ""), to_peer_id=dest_info.get("peer_id", ""),
            amount_sats=rebalance_amount, amount_msat=amount_msat,
            outbound_fee_ppm=outbound_fee_ppm, inbound_fee_ppm=inbound_fee_ppm,
            source_fee_ppm=source_fee_ppm, weighted_opp_cost_ppm=weighted_opp_cost,
            spread_ppm=spread_ppm, max_budget_sats=max_budget_sats,
            max_budget_msat=max_budget_msat, max_fee_ppm=max_fee_ppm,
            expected_profit_sats=expected_profit, liquidity_ratio=dest_ratio,
            dest_flow_state=dest_flow_state, dest_turnover_rate=dest_turnover_rate,
            source_turnover_rate=source_turnover_rate
        )

    def _calculate_turnover_rate(self, channel_id: str, capacity: int) -> float:
        if capacity <= 0: 
            return 0.0
        try:
            state = self.database.get_channel_state(channel_id)
            if not state: 
                return 0.05
            volume = (state.get("sats_in", 0) + state.get("sats_out", 0)) / max(self.config.flow_window_days, 1)
            return max(0.0001, min(1.0, volume / capacity))
        except Exception: 
            return 0.05

    def _estimate_inbound_fee(self, peer_id: str, amount_msat: int = 100000000) -> int:
        last_hop = self._get_last_hop_fee(peer_id)
        if last_hop is not None:
            return last_hop + self.config.inbound_fee_estimate_ppm
        
        hist_fee = self._get_historical_inbound_fee(peer_id)
        if hist_fee: 
            return hist_fee
        
        route_fee = self._get_route_fee_estimate(peer_id, amount_msat)
        if route_fee: 
            return route_fee
        
        return 1000

    def _get_last_hop_fee(self, peer_id: str) -> Optional[int]:
        try:
            our_id = self._get_our_node_id()
            if not our_id: 
                return None
            channels = self.plugin.rpc.listchannels(source=peer_id)
            for ch in channels.get("channels", []):
                if ch.get("destination") == our_id:
                    return ch.get("fee_per_millionth", 0) + (ch.get("base_fee_millisatoshi", 0) // 1000)
        except Exception: 
            pass
        return None

    def _get_route_fee_estimate(self, peer_id: str, amount_msat: int) -> Optional[int]:
        try:
            route = self.plugin.rpc.getroute(id=peer_id, amount_msat=amount_msat, riskfactor=10, maxhops=6)
            if route.get("route"):
                first_hop = route["route"][0].get("amount_msat", amount_msat)
                if isinstance(first_hop, str): 
                    first_hop = int(first_hop.replace("msat", ""))
                return int(((first_hop - amount_msat) / amount_msat) * 1_000_000)
        except Exception: 
            pass
        return None

    def _get_historical_inbound_fee(self, peer_id: str) -> Optional[int]:
        try:
            hist = self.database.get_rebalance_history_by_peer(peer_id)
            if not hist: 
                return None
            total_ppm, count = 0, 0
            for r in hist:
                if r.get("status") == "success" and r.get("amount_msat", 0) > 0:
                    total_ppm += int((r["fee_paid_msat"] / r["amount_msat"]) * 1_000_000)
                    count += 1
            if count > 0: 
                return total_ppm // count
        except Exception: 
            pass
        return None

    def _select_source_channel(self, sources, amount_needed, dest_channel=None):
        best_source, best_score = None, -float('inf')
        peers = self._get_peer_connection_status()
        
        # Also exclude sources with active jobs
        active_channels = set(self.job_manager.active_channels)
        
        for cid, info, ratio in sources:
            # Skip if this source has an active job
            normalized = cid.replace(':', 'x')
            if normalized in active_channels:
                continue
                
            if info.get("spendable_sats", 0) < amount_needed: 
                continue
            pid = info.get("peer_id", "")
            if pid and pid in peers and not peers[pid].get("connected"): 
                continue
            
            fee_ppm = info.get("fee_ppm", 1000)
            score = (ratio * 50) - (fee_ppm / 10)
            
            state = self.database.get_channel_state(cid)
            if state and state.get("state") == "sink": 
                score += 100
            elif state and state.get("state") == "balanced": 
                score += 20
            
            if score > best_score:
                best_score = score
                best_source = (cid, info)
        return best_source

    def _get_peer_connection_status(self) -> Dict:
        status = {}
        try:
            for p in self.plugin.rpc.listpeers().get("peers", []):
                status[p.get("id")] = {"connected": p.get("connected", False)}
        except Exception: 
            pass
        return status

    def _get_channels_with_balances(self) -> Dict[str, Dict[str, Any]]:
        """Get all channels with their current balances and fee info."""
        channels = {}
        try:
            listfunds = self.plugin.rpc.listfunds()
            listpeers = self.plugin.rpc.listpeers()
            
            # Build peer info map
            peer_info = {}
            for peer in listpeers.get("peers", []):
                peer_id = peer.get("id")
                for ch in peer.get("channels", []):
                    scid = ch.get("short_channel_id")
                    if scid and ch.get("state") == "CHANNELD_NORMAL":
                        peer_info[scid] = {
                            "peer_id": peer_id,
                            "fee_ppm": ch.get("fee_proportional_millionths", 0),
                            "base_fee_msat": ch.get("fee_base_msat", 0),
                            "htlcs": len(ch.get("htlcs", []))
                        }
            
            # Get balances from listfunds
            for channel in listfunds.get("channels", []):
                if channel.get("state") != "CHANNELD_NORMAL":
                    continue
                    
                scid = channel.get("short_channel_id", "")
                if not scid:
                    continue
                
                our_amount_msat = channel.get("our_amount_msat", 0)
                if isinstance(our_amount_msat, str):
                    our_amount_msat = int(our_amount_msat.replace("msat", ""))
                
                amount_msat = channel.get("amount_msat", 0)
                if isinstance(amount_msat, str):
                    amount_msat = int(amount_msat.replace("msat", ""))
                
                info = peer_info.get(scid, {})
                channels[scid] = {
                    "capacity": amount_msat // 1000,
                    "spendable_sats": our_amount_msat // 1000,
                    "peer_id": info.get("peer_id", channel.get("peer_id", "")),
                    "fee_ppm": info.get("fee_ppm", 0),
                    "base_fee_msat": info.get("base_fee_msat", 0),
                    "htlcs": info.get("htlcs", 0)
                }
                
        except Exception as e:
            self.plugin.log(f"Error getting channel balances: {e}", level='error')
        
        return channels

    def execute_rebalance(self, candidate: RebalanceCandidate) -> Dict[str, Any]:
        """
        Execute a rebalance for the given candidate.
        
        For sling: Starts an async background job
        For circular: Executes synchronously (legacy behavior)
        """
        result = {"success": False, "candidate": candidate.to_dict(), "message": ""}
        self._pending[candidate.to_channel] = int(time.time())
        
        try:
            # Ensure channels are unmanaged from clboss
            self.clboss.ensure_unmanaged_for_channel(
                candidate.from_channel, candidate.from_peer_id, 
                ClbossTags.FEE_AND_BALANCE, self.database
            )
            self.clboss.ensure_unmanaged_for_channel(
                candidate.to_channel, candidate.to_peer_id, 
                ClbossTags.FEE_AND_BALANCE, self.database
            )
            
            # Record rebalance attempt in database
            rebalance_id = self.database.record_rebalance(
                candidate.from_channel, candidate.to_channel, candidate.amount_sats,
                candidate.max_budget_sats, candidate.expected_profit_sats, 'pending'
            )
            
            if self.config.dry_run:
                self.plugin.log(f"[DRY RUN] Would rebalance {candidate.amount_sats} sats "
                              f"from {candidate.from_channel} to {candidate.to_channel}")
                self.database.update_rebalance_result(
                    rebalance_id, 'success', 0, candidate.expected_profit_sats
                )
                return {"success": True, "message": "Dry run", "rebalance_id": rebalance_id}

            # Route to appropriate rebalancer
            if self.config.rebalancer_plugin == 'sling':
                # Async execution via JobManager
                res = self.job_manager.start_job(candidate, rebalance_id)
                
                if res.get("success"):
                    # Update DB status to pending_async
                    self.database.update_rebalance_result(rebalance_id, 'pending_async')
                    result.update({
                        "success": True, 
                        "message": "Async job started",
                        "rebalance_id": rebalance_id
                    })
                    self.plugin.log(
                        f"Rebalance job queued: {candidate.to_channel} "
                        f"(job #{self.job_manager.active_job_count})"
                    )
                else:
                    error = res.get("error", "Failed to start job")
                    self.database.update_rebalance_result(
                        rebalance_id, 'failed', error_message=error
                    )
                    result["message"] = f"Failed: {error}"
                    self.plugin.log(f"Failed to start rebalance job: {error}", level='warn')
                    
            elif self.config.rebalancer_plugin == 'circular':
                # Synchronous execution (legacy)
                res = self._execute_circular(candidate)
                
                if res.get("success"):
                    fee = res.get("fee_sats", 0)
                    self.database.update_rebalance_result(
                        rebalance_id, 'success', fee, candidate.expected_profit_sats - fee
                    )
                    self.database.reset_failure_count(candidate.to_channel)
                    if self.metrics:
                        self.metrics.inc_counter(
                            MetricNames.REBALANCE_COST_TOTAL_SATS, 
                            fee, 
                            {"channel_id": candidate.to_channel}
                        )
                    result.update({
                        "success": True, 
                        "actual_fee_sats": fee, 
                        "message": "Success",
                        "rebalance_id": rebalance_id
                    })
                    self.plugin.log(
                        f"Rebalance SUCCESS: {candidate.to_channel} filled. Fee: {fee} sats."
                    )
                else:
                    error = res.get("error", "Unknown error")
                    self.database.update_rebalance_result(
                        rebalance_id, 'failed', error_message=error
                    )
                    self.database.increment_failure_count(candidate.to_channel)
                    result["message"] = f"Failed: {error}"
                    self.plugin.log(f"Rebalance FAILED: {error}", level='warn')
            else:
                error = f"Unknown rebalancer: {self.config.rebalancer_plugin}"
                self.database.update_rebalance_result(rebalance_id, 'failed', error_message=error)
                result["message"] = error

        except Exception as e:
            result["message"] = str(e)
            self.plugin.log(f"Execution error: {e}", level='error')
        
        return result

    def _execute_circular(self, candidate: RebalanceCandidate) -> Dict[str, Any]:
        """
        Execute circular rebalance (synchronous).
        Signature: circular outscid inscid amount maxppm attempts maxhops
        """
        result = {"success": False}
        max_attempts = 3
        max_hops = 10
        
        # Normalize SCIDs to colon format
        out_scid = candidate.from_channel.replace('x', ':')
        in_scid = candidate.to_channel.replace('x', ':')
        
        self.plugin.log(
            f"Executing circular: {out_scid} -> {in_scid}, max_ppm={candidate.max_fee_ppm}"
        )
        
        for attempt in range(max_attempts):
            try:
                response = self.plugin.rpc.circular(
                    out_scid, 
                    in_scid, 
                    candidate.amount_msat, 
                    candidate.max_fee_ppm, 
                    1,          # attempts per call
                    max_hops
                )
                
                if response.get("status") == "success":
                    fee_msat = response.get("fee", 0)
                    return {"success": True, "fee_sats": fee_msat // 1000, "fee_msat": fee_msat}
                
                self.plugin.log(
                    f"Circular attempt {attempt+1} failed: {response.get('message')}", 
                    level='debug'
                )
                time.sleep(1)
            except Exception as e:
                self.plugin.log(f"Circular RPC error: {e}", level='debug')
        
        return {"error": "Circular rebalance failed after retries"}

    def manual_rebalance(self, from_channel: str, to_channel: str, 
                         amount_sats: int, max_fee_sats: int = None) -> Dict[str, Any]:
        """Execute a manual rebalance between two channels."""
        channels = self._get_channels_with_balances()
        if from_channel not in channels or to_channel not in channels:
            return {"error": "Channels not found"}
            
        f_info = channels[from_channel]
        t_info = channels[to_channel]
        
        fee_ppm = t_info.get("fee_ppm", 0)
        src_ppm = f_info.get("fee_ppm", 0)
        est_in = self._estimate_inbound_fee(t_info.get("peer_id"))
        
        if max_fee_sats is None:
            max_fee_sats = int(amount_sats * (fee_ppm - est_in - src_ppm) / 1e6)
            if max_fee_sats < 0: 
                max_fee_sats = 100
            
        cand = RebalanceCandidate(
            from_channel, to_channel, f_info.get("peer_id"), t_info.get("peer_id"),
            amount_sats, amount_sats * 1000, fee_ppm, est_in, src_ppm,
            0, 0, max_fee_sats, max_fee_sats * 1000, 
            int(max_fee_sats * 1e6 / amount_sats) if amount_sats > 0 else 0,
            0, 0.5, "manual", 0, 0
        )
        return self.execute_rebalance(cand)

    def _check_capital_controls(self) -> bool:
        """Check if capital controls allow rebalancing."""
        try:
            listfunds = self.plugin.rpc.listfunds()
            onchain_sats = 0
            for output in listfunds.get("outputs", []):
                if output.get("status") == "confirmed":
                    amount_msat = output.get("amount_msat", 0)
                    if isinstance(amount_msat, str): 
                        amount_msat = int(amount_msat.replace("msat", ""))
                    onchain_sats += amount_msat // 1000
            
            channel_spendable_sats = 0
            for channel in listfunds.get("channels", []):
                if channel.get("state") != "CHANNELD_NORMAL": 
                    continue
                our_amount_msat = channel.get("our_amount_msat", 0)
                if isinstance(our_amount_msat, str): 
                    our_amount_msat = int(our_amount_msat.replace("msat", ""))
                spendable = our_amount_msat // 1000
                if spendable > 0: 
                    channel_spendable_sats += spendable
            
            total_reserve = onchain_sats + channel_spendable_sats
            if total_reserve < self.config.min_wallet_reserve:
                self.plugin.log(
                    f"CAPITAL CONTROL: Wallet reserve {total_reserve} < "
                    f"{self.config.min_wallet_reserve}", 
                    level='warning'
                )
                return False
                
            fees_spent_24h = self.database.get_total_rebalance_fees(int(time.time()) - 86400)
            if fees_spent_24h >= self.config.daily_budget_sats:
                self.plugin.log(
                    f"CAPITAL CONTROL: Daily budget exceeded "
                    f"({fees_spent_24h} >= {self.config.daily_budget_sats})", 
                    level='warning'
                )
                return False
                
        except Exception as e:
            self.plugin.log(f"Error checking capital controls: {e}", level='error')
        return True 
    
    def _is_pending_with_backoff(self, channel_id: str) -> bool:
        """Check if channel has a pending operation with exponential backoff."""
        # Also check job manager for active jobs
        if self.job_manager.has_active_job(channel_id):
            return True
            
        pending_time = self._pending.get(channel_id, 0)
        if pending_time == 0: 
            return False
        
        failure_count, _ = self.database.get_failure_count(channel_id)
        base_cooldown = 600
        cooldown = base_cooldown * (2 ** min(failure_count, 4))
        
        if int(time.time()) - pending_time > cooldown:
            del self._pending[channel_id]
            return False
        return True
    
    # =========================================================================
    # Job Management API (exposed for RPC commands)
    # =========================================================================
    
    def get_active_jobs(self) -> List[Dict[str, Any]]:
        """Get status of all active rebalance jobs."""
        return self.job_manager.get_all_jobs_status()
    
    def stop_rebalance_job(self, channel_id: str) -> Dict[str, Any]:
        """Manually stop a rebalance job."""
        if self.job_manager.stop_job(channel_id, reason="manual"):
            return {"success": True, "message": f"Stopped job for {channel_id}"}
        return {"success": False, "error": f"No active job for {channel_id}"}
    
    def stop_all_rebalance_jobs(self) -> Dict[str, Any]:
        """Stop all active rebalance jobs."""
        count = self.job_manager.stop_all_jobs(reason="manual_stop_all")
        return {"success": True, "stopped": count}
