"""
cl-revenue-ops modules package

This package contains the core modules for the Revenue Operations plugin:
- flow_analysis: Sink/Source detection and flow metrics
- fee_controller: PID-based dynamic fee adjustment
- rebalancer: EV-based profit-aware rebalancing
- clboss_manager: Interface for clboss unmanage commands
- config: Configuration and constants
- database: SQLite storage layer
"""

from .flow_analysis import FlowAnalyzer, ChannelState, FlowMetrics
from .fee_controller import PIDFeeController
from .rebalancer import EVRebalancer, RebalanceCandidate
from .clboss_manager import ClbossManager
from .config import Config
from .database import Database

__all__ = [
    'FlowAnalyzer',
    'ChannelState', 
    'FlowMetrics',
    'PIDFeeController',
    'EVRebalancer',
    'RebalanceCandidate',
    'ClbossManager',
    'Config',
    'Database'
]
