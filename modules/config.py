"""
Configuration module for cl-revenue-ops

Contains the Config dataclass that holds all tunable parameters
for the Revenue Operations plugin.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class Config:
    """
    Configuration container for the Revenue Operations plugin.
    
    All values can be set via plugin options at startup.
    """
    
    # Database path
    db_path: str = '~/.lightning/revenue_ops.db'
    
    # Timer intervals (in seconds)
    flow_interval: int = 3600      # 1 hour
    fee_interval: int = 1800       # 30 minutes
    rebalance_interval: int = 900  # 15 minutes
    
    # Flow analysis parameters
    target_flow: int = 100000      # Target sats routed per day per channel
    flow_window_days: int = 7      # Days to analyze for flow calculation
    
    # Flow ratio thresholds for classification
    source_threshold: float = 0.5   # FlowRatio > 0.5 = Source (draining)
    sink_threshold: float = -0.5    # FlowRatio < -0.5 = Sink (filling)
    
    # Fee parameters
    min_fee_ppm: int = 10          # Floor fee in PPM
    max_fee_ppm: int = 5000        # Ceiling fee in PPM
    base_fee_msat: int = 0         # Base fee (we focus on PPM)
    
    # PID Controller gains
    pid_kp: float = 0.5            # Proportional gain
    pid_ki: float = 0.1            # Integral gain
    pid_kd: float = 0.05           # Derivative gain
    
    # PID limits to prevent runaway
    pid_integral_max: float = 1000.0   # Max accumulated integral
    pid_output_max: float = 500.0      # Max single adjustment step
    
    # Rebalancing parameters
    rebalance_min_profit: int = 10     # Min profit in sats to trigger
    rebalance_max_amount: int = 5000000  # Max rebalance amount in sats
    rebalance_min_amount: int = 50000    # Min rebalance amount in sats
    low_liquidity_threshold: float = 0.2  # Below 20% = low outbound
    high_liquidity_threshold: float = 0.8 # Above 80% = high outbound
    rebalance_cooldown_hours: int = 24   # Don't re-rebalance same channel for 24h
    inbound_fee_estimate_ppm: int = 500  # Network routing cost estimate in PPM
    
    # clboss integration
    clboss_enabled: bool = True    # Whether to use clboss-unmanage
    clboss_unmanage_duration_hours: int = 24  # Keep unmanaged after rebalance
    
    # Rebalancer plugin selection
    rebalancer_plugin: str = 'circular'  # 'circular' or 'sling'
    
    # Profitability tracking
    estimated_open_cost_sats: int = 5000  # Estimated on-chain fee for channel open
    
    # Global Capital Controls
    daily_budget_sats: int = 5000          # Max rebalancing fees per 24h period
    min_wallet_reserve: int = 1_000_000    # Min sats (on-chain + receivable) before ABORT
    
    # HTLC Congestion threshold
    htlc_congestion_threshold: float = 0.8  # Mark channel as CONGESTED if >80% HTLC slots used
    
    # Reputation-weighted volume
    enable_reputation: bool = True  # If True, weight volume by peer success rate
    reputation_decay: float = 0.98  # Decay factor per flow_interval (default hourly)
                                     # 0.98^24 ≈ 0.61, meaning old data loses ~40% weight daily
    
    # Prometheus Metrics (Phase 2: Observability)
    enable_prometheus: bool = False  # If True, start Prometheus metrics exporter (disabled by default)
    prometheus_port: int = 9800      # Port for Prometheus HTTP server
    
    # Kelly Criterion Position Sizing (Phase 4: Risk Management)
    enable_kelly: bool = False       # If True, scale rebalance budget by Kelly fraction
    kelly_fraction: float = 0.5      # Multiplier for Kelly fraction ("Half Kelly" is standard)
                                      # Full Kelly (1.0) maximizes growth but has high volatility
                                      # Half Kelly (0.5) reduces volatility drag significantly
    
    # Safety flags
    dry_run: bool = False          # If True, log but don't execute


# Default chain cost assumptions for fee floor calculation
class ChainCostDefaults:
    """
    Default assumptions for calculating the economic fee floor.
    
    The floor is calculated as:
    floor_ppm = (channel_open_cost + channel_close_cost) / estimated_lifetime_volume * 1_000_000
    
    This ensures we never charge less than what it costs us to maintain the channel.
    """
    
    # Estimated on-chain costs in sats
    CHANNEL_OPEN_COST_SATS: int = 5000      # ~$3-5 at typical fee rates
    CHANNEL_CLOSE_COST_SATS: int = 3000     # Usually cheaper than open
    
    # Estimated channel lifetime
    CHANNEL_LIFETIME_DAYS: int = 365        # 1 year average
    
    # Estimated routing volume per day (conservative)
    DAILY_VOLUME_SATS: int = 1000000        # 1M sats/day
    
    @classmethod
    def calculate_floor_ppm(cls, capacity_sats: int) -> int:
        """
        Calculate the economic floor fee for a channel.
        
        Args:
            capacity_sats: Channel capacity in satoshis
            
        Returns:
            Minimum fee in PPM that covers channel costs
        """
        total_chain_cost = cls.CHANNEL_OPEN_COST_SATS + cls.CHANNEL_CLOSE_COST_SATS
        estimated_lifetime_volume = cls.DAILY_VOLUME_SATS * cls.CHANNEL_LIFETIME_DAYS
        
        # Calculate minimum fee to break even
        # floor_ppm = cost / volume * 1_000_000
        if estimated_lifetime_volume > 0:
            floor_ppm = (total_chain_cost / estimated_lifetime_volume) * 1_000_000
            return max(1, int(floor_ppm))
        return 1


# Liquidity bucket definitions for fee tiers
class LiquidityBuckets:
    """
    Define liquidity buckets for tiered fee strategies.
    
    Different liquidity levels warrant different fee approaches:
    - Very low outbound: High fees (scarce resource)
    - Low outbound: Above average fees
    - Balanced: Target fees
    - High outbound: Below average fees  
    - Very high outbound: Low fees (encourage usage)
    """
    
    VERY_LOW = 0.1    # < 10% outbound
    LOW = 0.25        # 10-25% outbound
    BALANCED_LOW = 0.4   # 25-40% outbound
    BALANCED_HIGH = 0.6  # 40-60% outbound (ideal)
    HIGH = 0.75       # 60-75% outbound
    VERY_HIGH = 0.9   # > 75% outbound
    
    @classmethod
    def get_bucket(cls, outbound_ratio: float) -> str:
        """
        Classify a channel by its outbound liquidity ratio.
        
        Args:
            outbound_ratio: outbound_sats / capacity_sats
            
        Returns:
            Bucket name string
        """
        if outbound_ratio < cls.VERY_LOW:
            return "very_low"
        elif outbound_ratio < cls.LOW:
            return "low"
        elif outbound_ratio < cls.BALANCED_LOW:
            return "balanced_low"
        elif outbound_ratio < cls.BALANCED_HIGH:
            return "balanced"
        elif outbound_ratio < cls.HIGH:
            return "balanced_high"
        elif outbound_ratio < cls.VERY_HIGH:
            return "high"
        else:
            return "very_high"
    
    @classmethod
    def get_fee_multiplier(cls, bucket: str) -> float:
        """
        Get fee multiplier for a liquidity bucket.
        
        Args:
            bucket: Bucket name from get_bucket()
            
        Returns:
            Multiplier to apply to base fee
        """
        multipliers = {
            "very_low": 3.0,      # Triple fees when nearly depleted
            "low": 2.0,           # Double fees when low
            "balanced_low": 1.25, # Slightly above average
            "balanced": 1.0,      # Target fee
            "balanced_high": 0.85,# Slightly below average
            "high": 0.7,          # Reduced fees to encourage routing
            "very_high": 0.5      # Half fees when overloaded
        }
        return multipliers.get(bucket, 1.0)
