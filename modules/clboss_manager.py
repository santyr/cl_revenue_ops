"""
Clboss Manager module for cl-revenue-ops

Handles interaction with clboss for the Manager-Override pattern.
Before making fee or liquidity changes, we must unmanage the peer
from clboss to prevent conflicts.

MANAGER-OVERRIDE PATTERN:
-------------------------
clboss is excellent at:
- Channel creation and peer selection
- Monitoring node reliability
- Basic fee adjustments

But we want to override it for:
- Fee setting (we use PID controller with economic principles)
- Rebalancing decisions (we use EV-based profit analysis)

The pattern:
1. Check if clboss is managing the peer
2. Call clboss-unmanage for the specific tag (lnfee, rebalance)
3. Make our changes
4. Track what we've unmanaged so we can re-enable if needed
"""

from typing import Dict, Any, Optional, List
from pyln.client import Plugin, RpcError


# Tags used by clboss that we may want to override
class ClbossTags:
    """
    Clboss management tags.
    
    These correspond to different aspects clboss manages:
    - lnfee: Fee management
    - rebalance: Liquidity rebalancing
    """
    FEE = "lnfee"
    REBALANCE = "rebalance"
    
    ALL = [FEE, REBALANCE]


class ClbossManager:
    """
    Manager for clboss interaction.
    
    Provides methods to safely unmanage peers from clboss before
    making changes, and to re-enable management when desired.
    """
    
    def __init__(self, plugin: Plugin, config):
        """
        Initialize the clboss manager.
        
        Args:
            plugin: Reference to the pyln Plugin
            config: Configuration object
        """
        self.plugin = plugin
        self.config = config
        self._clboss_available: Optional[bool] = None
    
    def is_clboss_available(self) -> bool:
        """
        Check if clboss is available and running.
        
        Returns:
            True if clboss commands are available
        """
        if not self.config.clboss_enabled:
            return False
        
        if self._clboss_available is not None:
            return self._clboss_available
        
        try:
            # Try to call a clboss command to see if it's available
            result = self.plugin.rpc.call("clboss-status")
            self._clboss_available = True
            self.plugin.log("clboss detected and available")
            return True
        except RpcError as e:
            if "Unknown command" in str(e) or "not found" in str(e).lower():
                self._clboss_available = False
                self.plugin.log("clboss not available - commands will be skipped")
                return False
            # Other RPC errors might mean clboss is there but had an issue
            self._clboss_available = True
            return True
        except Exception as e:
            self.plugin.log(f"Error checking clboss availability: {e}", level='warning')
            self._clboss_available = False
            return False
    
    def unmanage_for_fee(self, peer_id: str) -> Dict[str, Any]:
        """
        Unmanage a peer from clboss fee management.
        
        This MUST be called before setting fees on a channel to prevent
        clboss from reverting our changes.
        
        Args:
            peer_id: The node ID of the peer
            
        Returns:
            Result dict with status and details
        """
        return self.unmanage(peer_id, ClbossTags.FEE)
    
    def unmanage_for_rebalance(self, peer_id: str) -> Dict[str, Any]:
        """
        Unmanage a peer from clboss rebalancing.
        
        Args:
            peer_id: The node ID of the peer
            
        Returns:
            Result dict with status and details
        """
        return self.unmanage(peer_id, ClbossTags.REBALANCE)
    
    def unmanage(self, peer_id: str, tag: str) -> Dict[str, Any]:
        """
        Unmanage a peer from clboss for a specific tag.
        
        This is the core override method. It tells clboss to stop
        managing this peer for the specified aspect (fee/rebalance).
        
        Args:
            peer_id: The node ID of the peer
            tag: The management tag to disable (e.g., 'lnfee')
            
        Returns:
            Result dict with status and details
        """
        result = {
            "peer_id": peer_id,
            "tag": tag,
            "action": "unmanage",
            "success": False,
            "skipped": False,
            "message": ""
        }
        
        # Check if clboss integration is enabled
        if not self.config.clboss_enabled:
            result["skipped"] = True
            result["message"] = "clboss integration disabled in config"
            return result
        
        # Check if clboss is available
        if not self.is_clboss_available():
            result["skipped"] = True
            result["message"] = "clboss not available"
            return result
        
        # Check if we've already unmanaged this peer/tag
        # Import here to avoid circular imports
        from .database import Database
        
        try:
            # Check if already unmanaged (via plugin's database reference)
            # Note: We'll need to access the database through plugin context
            
            if self.config.dry_run:
                result["success"] = True
                result["message"] = f"[DRY RUN] Would unmanage {peer_id} for {tag}"
                self.plugin.log(result["message"])
                return result
            
            # Call clboss-unmanage with positional args: nodeid tags
            try:
                unmanage_result = self.plugin.rpc.call(
                    "clboss-unmanage",
                    [peer_id, tag]  # positional: nodeid, tags
                )
                
                result["success"] = True
                result["message"] = f"Successfully unmanaged {peer_id} for {tag}"
                result["clboss_response"] = unmanage_result
                
                self.plugin.log(f"Unmanaged peer {peer_id[:16]}... from clboss {tag} management")
                
            except RpcError as e:
                error_str = str(e)
                
                # Handle case where peer is not managed by clboss
                if "not managed" in error_str.lower() or "already unmanaged" in error_str.lower():
                    result["success"] = True
                    result["message"] = f"Peer {peer_id} already not managed by clboss for {tag}"
                else:
                    result["success"] = False
                    result["message"] = f"clboss-unmanage failed: {error_str}"
                    self.plugin.log(f"Failed to unmanage {peer_id}: {error_str}", level='warning')
                    
        except Exception as e:
            result["success"] = False
            result["message"] = f"Unexpected error: {str(e)}"
            self.plugin.log(f"Error in unmanage: {e}", level='error')
        
        return result
    
    def remanage(self, peer_id: str, tag: Optional[str] = None) -> Dict[str, Any]:
        """
        Re-enable clboss management for a peer.
        
        Use this to hand control back to clboss for a peer we previously
        unmanaged. If no tag is specified, re-enable all tags.
        
        Args:
            peer_id: The node ID of the peer
            tag: Optional specific tag to re-enable (None = all tags)
            
        Returns:
            Result dict with status and details
        """
        result = {
            "peer_id": peer_id,
            "tag": tag or "all",
            "action": "remanage",
            "success": False,
            "message": ""
        }
        
        if not self.config.clboss_enabled or not self.is_clboss_available():
            result["message"] = "clboss not available"
            return result
        
        if self.config.dry_run:
            result["success"] = True
            result["message"] = f"[DRY RUN] Would remanage {peer_id} for {tag or 'all tags'}"
            self.plugin.log(result["message"])
            return result
        
        try:
            # Call clboss-manage to re-enable management
            # clboss-manage nodeid tags (positional args)
            tags_to_manage = [tag] if tag else ClbossTags.ALL
            
            for t in tags_to_manage:
                try:
                    self.plugin.rpc.call(
                        "clboss-manage",
                        [peer_id, t]  # positional: nodeid, tags
                    )
                except RpcError as e:
                    # Ignore errors for individual tags
                    self.plugin.log(f"Could not remanage {t} for {peer_id}: {e}", level='debug')
            
            result["success"] = True
            result["message"] = f"Re-enabled clboss management for {peer_id}"
            self.plugin.log(f"Remanaged peer {peer_id[:16]}... to clboss")
            
        except Exception as e:
            result["success"] = False
            result["message"] = f"Error: {str(e)}"
            self.plugin.log(f"Error in remanage: {e}", level='error')
        
        return result
    
    def get_unmanaged_status(self) -> Dict[str, Any]:
        """
        Get the current status of clboss management overrides.
        
        Returns:
            Dict with clboss status and list of unmanaged peers
        """
        status = {
            "clboss_enabled": self.config.clboss_enabled,
            "clboss_available": self.is_clboss_available(),
            "unmanaged_peers": []
        }
        
        if not self.config.clboss_enabled:
            status["message"] = "clboss integration is disabled"
            return status
        
        if not self.is_clboss_available():
            status["message"] = "clboss is not available"
            return status
        
        try:
            # Try to get clboss status
            clboss_status = self.plugin.rpc.call("clboss-status")
            status["clboss_status"] = clboss_status
            
            # Get unmanaged list if available
            try:
                unmanaged = self.plugin.rpc.call("clboss-unmanaged-list")
                status["unmanaged_peers"] = unmanaged.get("unmanaged", [])
            except RpcError:
                # Command might not exist in older versions
                pass
                
        except Exception as e:
            status["message"] = f"Could not get clboss status: {e}"
        
        return status
    
    def is_peer_managed(self, peer_id: str, tag: str) -> bool:
        """
        Check if a peer is currently managed by clboss for a specific tag.
        
        Args:
            peer_id: The node ID of the peer
            tag: The management tag to check
            
        Returns:
            True if clboss is managing this peer/tag, False otherwise
        """
        if not self.is_clboss_available():
            return False
        
        try:
            # Try clboss-status or similar to check
            # Note: The exact method depends on clboss version
            # For now, assume managed unless we've explicitly unmanaged
            return True
        except Exception:
            return False
    
    def ensure_unmanaged_for_channel(self, channel_id: str, peer_id: str, 
                                     tag: str, database) -> bool:
        """
        Ensure a channel's peer is unmanaged before making changes.
        
        This is the main entry point for the Manager-Override pattern.
        It checks if we need to unmanage, and does so if necessary.
        
        Args:
            channel_id: The channel ID
            peer_id: The peer's node ID
            tag: The management tag (fee/rebalance)
            database: Database instance for tracking
            
        Returns:
            True if we can proceed with changes, False if blocked
        """
        # Check if already unmanaged
        if database.is_unmanaged(peer_id, tag):
            return True
        
        # Try to unmanage
        result = self.unmanage(peer_id, tag)
        
        if result["success"] or result["skipped"]:
            # Record that we unmanaged (if not skipped)
            if result["success"] and not result.get("skipped"):
                database.record_unmanage(peer_id, tag)
            return True
        
        # Failed to unmanage - log and return False
        self.plugin.log(
            f"Could not unmanage {peer_id} for {tag}: {result['message']}", 
            level='warning'
        )
        return False
