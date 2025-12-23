# charizard/utils/logging.py
"""
Pipeline logging with rich console output
"""

import os
import time
from datetime import datetime
from typing import Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn


class PipelineLogger:
    """Rich console logging for pipeline"""
    
    def __init__(self, jobs_dir: str):
        self.jobs_dir = jobs_dir
        self.console = Console(record=True)
        self.start_time = datetime.now()
        self.step_times: Dict[str, float] = {}
        self._current_step: Optional[str] = None
        self._step_start: Optional[datetime] = None
        
        os.makedirs(jobs_dir, exist_ok=True)
    
    def banner(self, text: str, style: str = "cyan"):
        """Print banner"""
        if style == "success":
            self.console.print()
            self.console.print(Panel(f"[bold green]{text}[/bold green]", style="green"))
        else:
            self.console.print()
            self.console.print(Panel(f"[bold]{text}[/bold]", style=style))
    
    def step(self, name: str):
        """Start a new pipeline step"""
        if self._current_step and self._step_start:
            elapsed = (datetime.now() - self._step_start).total_seconds()
            self.step_times[self._current_step] = elapsed
        
        self._current_step = name
        self._step_start = datetime.now()
        
        self.console.print()
        self.console.print(Panel(f"[bold]{name}[/bold]", style="cyan", expand=False))
    
    def substep(self, msg: str):
        """Print substep"""
        self.console.print(f"  [dim]→[/dim] {msg}")
    
    def info(self, msg: str):
        """Print info"""
        self.console.print(f"[blue]ℹ[/blue] {msg}")
    
    def success(self, msg: str):
        """Print success with timing"""
        elapsed = datetime.now() - self.start_time
        self.console.print(f"[green]✓[/green] {msg} [dim]({self._format_duration(elapsed)})[/dim]")
    
    def warning(self, msg: str):
        """Print warning"""
        self.console.print(f"[yellow]⚠[/yellow] {msg}")
    
    def error(self, msg: str, log_file: Optional[str] = None):
        """Print error"""
        self.console.print(f"[red]✗ ERROR:[/red] {msg}")
        if log_file:
            self.console.print(f"  [dim]Check: {log_file}[/dim]")
    
    def table(self, title: str, columns: List[str], rows: List[List[str]]):
        """Print table"""
        t = Table(title=title)
        for col in columns:
            t.add_column(col)
        for row in rows:
            t.add_row(*[str(x) for x in row])
        self.console.print(t)
    
    def _format_duration(self, delta) -> str:
        """Format timedelta"""
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"
    
    def wait_with_progress(self, description: str):
        """Return a progress context manager"""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=self.console,
            transient=True
        )
    
    def print_summary(self):
        """Print execution summary"""
        if self._current_step and self._step_start:
            elapsed = (datetime.now() - self._step_start).total_seconds()
            self.step_times[self._current_step] = elapsed
        
        total_time = datetime.now() - self.start_time
        
        self.console.print()
        self.console.print(Panel("[bold]Pipeline Summary[/bold]", style="green"))
        
        if self.step_times:
            t = Table(title="Step Timings")
            t.add_column("Step", style="cyan")
            t.add_column("Duration", justify="right")
            
            for step, duration in self.step_times.items():
                t.add_row(step, self._format_duration_secs(duration))
            
            t.add_row("[bold]Total[/bold]", f"[bold]{self._format_duration(total_time)}[/bold]")
            self.console.print(t)
    
    def _format_duration_secs(self, seconds: float) -> str:
        """Format seconds"""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"
    
    def save(self):
        """Save logs"""
        log_file = os.path.join(self.jobs_dir, "charizard.log")
        html_file = os.path.join(self.jobs_dir, "charizard.html")
        
        with open(log_file, 'w') as f:
            f.write(self.console.export_text())
        
        self.console.save_html(html_file, inline_styles=True)
        self.info(f"Logs saved to {log_file}")
