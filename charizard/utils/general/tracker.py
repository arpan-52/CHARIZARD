# charizard/utils/general/tracker.py
"""
Job and SPW/Field tracking utilities.

JobTracker keeps track of:
- Active SPWs
- Active fields
- Failed steps
- Checkpoints
"""

import os
import yaml
from datetime import datetime
from typing import List, Set, Dict, Optional, Any
from collections import defaultdict


class JobTracker:
    """
    Tracks active SPWs, fields, and job status throughout pipeline.
    
    Handles:
    - Active SPW tracking (which SPWs are still being processed)
    - Failure tracking (which SPWs failed at which step)
    - Checkpointing (resume support)
    """
    
    def __init__(self, num_spws: int, logger, checkpoint_file: str = None):
        """
        Initialize tracker.
        
        Args:
            num_spws: Number of processing SPWs
            logger: PipelineLogger
            checkpoint_file: Path to checkpoint file (optional)
        """
        self.logger = logger
        self.num_spws = num_spws
        
        # Active SPWs - starts with all
        self._active_spws: Set[str] = set(f"spw{i}" for i in range(num_spws))
        
        # Failed tracking
        self._failed_steps: Dict[str, Set[str]] = defaultdict(set)  # step -> set of failed spws
        self._failure_reasons: Dict[str, str] = {}  # spw -> reason
        
        # Checkpointing
        self.checkpoint_file = checkpoint_file or "charizard_checkpoints.yaml"
        self._checkpoints: Dict[str, Any] = {}
        self._load_checkpoints()
    
    def _load_checkpoints(self):
        """Load existing checkpoints if available"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    self._checkpoints = yaml.safe_load(f) or {}
                self.logger.info(f"Loaded checkpoints from {self.checkpoint_file}")
            except Exception as e:
                self.logger.warning(f"Failed to load checkpoints: {e}")
                self._checkpoints = {}
    
    def _save_checkpoints(self):
        """Save checkpoints to file"""
        try:
            with open(self.checkpoint_file, 'w') as f:
                yaml.dump(self._checkpoints, f, default_flow_style=False)
        except Exception as e:
            self.logger.warning(f"Failed to save checkpoints: {e}")
    
    def get_active_spws(self) -> List[str]:
        """Get list of currently active SPWs"""
        return sorted(list(self._active_spws))
    
    def get_num_active(self) -> int:
        """Get number of active SPWs"""
        return len(self._active_spws)
    
    def is_active(self, spw: str) -> bool:
        """Check if SPW is active"""
        return spw in self._active_spws
    
    def mark_failed(self, spw: str, step: str, reason: str = ""):
        """
        Mark an SPW as failed.
        
        Args:
            spw: SPW name (e.g., "spw0")
            step: Step name where it failed
            reason: Reason for failure
        """
        if spw in self._active_spws:
            self._active_spws.remove(spw)
        
        self._failed_steps[step].add(spw)
        self._failure_reasons[spw] = reason
        
        self.logger.warning(f"SPW {spw} failed at {step}: {reason}")
    
    def mark_success(self, spw: str, step: str):
        """
        Mark an SPW as successful for a step.
        
        Args:
            spw: SPW name
            step: Step name
        """
        if step not in self._checkpoints:
            self._checkpoints[step] = {'spws': {}, 'timestamp': None}
        
        self._checkpoints[step]['spws'][spw] = {
            'status': 'completed',
            'timestamp': datetime.now().isoformat()
        }
        self._checkpoints[step]['timestamp'] = datetime.now().isoformat()
        
        self._save_checkpoints()
    
    def mark_step_complete(self, step: str, successful_spws: List[str], failed_spws: List[str]):
        """
        Mark a step as complete with results.
        
        Args:
            step: Step name
            successful_spws: List of SPWs that succeeded
            failed_spws: List of SPWs that failed
        """
        for spw in successful_spws:
            self.mark_success(spw, step)
        
        for spw in failed_spws:
            self.mark_failed(spw, step)
    
    def is_step_complete(self, step: str, spw: str) -> bool:
        """Check if step is already complete for SPW"""
        if step in self._checkpoints:
            spw_data = self._checkpoints[step].get('spws', {}).get(spw)
            if spw_data and spw_data.get('status') == 'completed':
                return True
        return False
    
    def get_failed_spws(self, step: Optional[str] = None) -> List[str]:
        """
        Get list of failed SPWs.
        
        Args:
            step: If provided, get failures for specific step only
        
        Returns:
            List of failed SPW names
        """
        if step:
            return sorted(list(self._failed_steps.get(step, set())))
        
        # All failures across all steps
        all_failed = set()
        for spws in self._failed_steps.values():
            all_failed.update(spws)
        return sorted(list(all_failed))
    
    def get_failure_reason(self, spw: str) -> str:
        """Get failure reason for SPW"""
        return self._failure_reasons.get(spw, "Unknown")
    
    def get_summary(self) -> Dict[str, Any]:
        """Get tracker summary"""
        return {
            'total_spws': self.num_spws,
            'active_spws': len(self._active_spws),
            'failed_spws': len(self._failure_reasons),
            'active': self.get_active_spws(),
            'failed': list(self._failure_reasons.keys()),
            'completed_steps': list(self._checkpoints.keys()),
        }
    
    def print_status(self):
        """Print current status"""
        summary = self.get_summary()
        self.logger.info(f"Active SPWs: {summary['active_spws']}/{summary['total_spws']}")
        if summary['failed_spws'] > 0:
            self.logger.warning(f"Failed SPWs: {summary['failed']}")
