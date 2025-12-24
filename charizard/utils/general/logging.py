# charizard/utils/general/logging.py
"""
Pipeline logging utilities.

PipelineLogger provides formatted console output and file logging.
"""

import os
import logging
from datetime import datetime
from typing import Optional


class PipelineLogger:
    """
    Logger for Charizard pipeline.
    
    Provides:
    - Formatted console output with colors
    - File logging
    - Step/substep formatting
    - Summary tracking
    """
    
    # ANSI colors
    COLORS = {
        'reset': '\033[0m',
        'bold': '\033[1m',
        'red': '\033[91m',
        'green': '\033[92m',
        'yellow': '\033[93m',
        'blue': '\033[94m',
        'magenta': '\033[95m',
        'cyan': '\033[96m',
    }
    
    def __init__(self, working_dir: str, ms_name: str):
        """
        Initialize logger.
        
        Args:
            working_dir: Working directory for log files
            ms_name: MS name for log file naming
        """
        self.working_dir = working_dir
        self.ms_name = ms_name
        
        # Create log directory
        os.makedirs(working_dir, exist_ok=True)
        
        # Log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = os.path.join(working_dir, f"charizard_{ms_name}_{timestamp}.log")
        
        # Setup file logger
        self._file_logger = logging.getLogger(f"charizard_{ms_name}")
        self._file_logger.setLevel(logging.DEBUG)
        
        # File handler
        fh = logging.FileHandler(self.log_file)
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self._file_logger.addHandler(fh)
        
        # Tracking
        self._step_count = 0
        self._errors = []
        self._warnings = []
        self._start_time = datetime.now()
    
    def _color(self, text: str, color: str) -> str:
        """Add color to text"""
        return f"{self.COLORS.get(color, '')}{text}{self.COLORS['reset']}"
    
    def _log(self, level: str, message: str, color: Optional[str] = None):
        """Log to both console and file"""
        # File
        getattr(self._file_logger, level.lower())(message)
        
        # Console
        if color:
            print(self._color(message, color))
        else:
            print(message)
    
    def banner(self, text: str):
        """Print banner"""
        width = 60
        line = "=" * width
        padded = text.center(width)
        
        print()
        print(self._color(line, 'cyan'))
        print(self._color(padded, 'cyan'))
        print(self._color(line, 'cyan'))
        print()
        
        self._file_logger.info(f"{'='*60}")
        self._file_logger.info(text)
        self._file_logger.info(f"{'='*60}")
    
    def step(self, message: str):
        """Print step header"""
        self._step_count += 1
        header = f"\n[STEP {self._step_count}] {message}"
        print(self._color(header, 'bold'))
        print(self._color("-" * 50, 'blue'))
        self._file_logger.info(f"[STEP {self._step_count}] {message}")
    
    def substep(self, message: str):
        """Print substep"""
        print(f"  → {message}")
        self._file_logger.info(f"  → {message}")
    
    def info(self, message: str):
        """Info message"""
        print(f"  {message}")
        self._file_logger.info(message)
    
    def success(self, message: str):
        """Success message"""
        print(self._color(f"  ✓ {message}", 'green'))
        self._file_logger.info(f"SUCCESS: {message}")
    
    def warning(self, message: str):
        """Warning message"""
        self._warnings.append(message)
        print(self._color(f"  ⚠ {message}", 'yellow'))
        self._file_logger.warning(message)
    
    def error(self, message: str):
        """Error message"""
        self._errors.append(message)
        print(self._color(f"  ✗ {message}", 'red'))
        self._file_logger.error(message)
    
    def debug(self, message: str):
        """Debug message (file only)"""
        self._file_logger.debug(message)
    
    def print_summary(self):
        """Print final summary"""
        duration = datetime.now() - self._start_time
        
        print()
        print(self._color("=" * 50, 'cyan'))
        print(self._color("SUMMARY", 'cyan'))
        print(self._color("=" * 50, 'cyan'))
        
        print(f"  Duration: {duration}")
        print(f"  Steps completed: {self._step_count}")
        print(f"  Warnings: {len(self._warnings)}")
        print(f"  Errors: {len(self._errors)}")
        print(f"  Log file: {self.log_file}")
        
        if self._errors:
            print(self._color("\nErrors:", 'red'))
            for err in self._errors[:5]:
                print(self._color(f"  - {err}", 'red'))
            if len(self._errors) > 5:
                print(self._color(f"  ... and {len(self._errors) - 5} more", 'red'))
        
        print()
        
        self._file_logger.info(f"SUMMARY: Duration={duration}, Steps={self._step_count}, Warnings={len(self._warnings)}, Errors={len(self._errors)}")
