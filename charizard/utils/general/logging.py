# charizard/utils/general/logging.py
"""
Professional pipeline logging with rich console output
"""

import os
from datetime import datetime
from typing import Dict, List, Optional

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn


class PipelineLogger:
    """Rich console logging for pipeline with professional style"""

    def __init__(self, jobs_dir: str):
        self.jobs_dir = jobs_dir
        self.console = Console(record=True)
        self.start_time = datetime.now()
        self.step_times: Dict[str, float] = {}
        self._current_step: Optional[str] = None
        self._step_start: Optional[datetime] = None
        self._step_logs: Dict[str, List[str]] = {}

        os.makedirs(jobs_dir, exist_ok=True)

    # ----------------------------
    # Banner
    # ----------------------------
    def banner(self, text: str, style: str = "cyan"):
        """Print a banner panel"""
        self.console.print()
        self.console.print(Panel(f"[bold]{text}[/bold]", style=style, expand=True))

    # ----------------------------
    # Steps / Substeps
    # ----------------------------
    def step(self, name: str):
        """Start a new pipeline step"""
        # Save previous step timing
        if self._current_step and self._step_start:
            elapsed = (datetime.now() - self._step_start).total_seconds()
            self.step_times[self._current_step] = elapsed

        self._current_step = name
        self._step_start = datetime.now()
        self._step_logs[name] = []

        self.console.print()
        self.console.print(Panel(f"[bold]{name}[/bold]", style="cyan", expand=True))

    def substep(self, msg: str):
        """Print a substep"""
        if self._current_step:
            self._step_logs[self._current_step].append(msg)
        self.console.print(f"    {msg}")  # 4-space indent for clarity

    # ----------------------------
    # Messages
    # ----------------------------
    def info(self, msg: str):
        """Print info message"""
        self.console.print(f"[bold blue]INFO[/bold blue]: {msg}")

    def success(self, msg: str):
        """Print success message with elapsed time"""
        elapsed = datetime.now() - self.start_time
        self.console.print(f"[bold green]SUCCESS[/bold green]: {msg} "
                           f"[dim](Elapsed: {self._format_duration(elapsed)})[/dim]")

    def warning(self, msg: str):
        """Print warning message"""
        self.console.print(f"[bold yellow]WARNING[/bold yellow]: {msg}")

    def error(self, msg: str, log_file: Optional[str] = None):
        """Print error message"""
        self.console.print(f"[bold red]ERROR[/bold red]: {msg}")
        if log_file:
            self.console.print(f"  [dim]Check log file: {log_file}[/dim]")

    # ----------------------------
    # Tables
    # ----------------------------
    def table(self, title: str, columns: List[str], rows: List[List[str]]):
        """Print a table with full width"""
        t = Table(title=title, expand=True, show_lines=True)
        for col in columns:
            t.add_column(col, overflow="fold")
        for row in rows:
            t.add_row(*[str(x) for x in row])
        self.console.print(t)

    # ----------------------------
    # Progress context
    # ----------------------------
    def wait_with_progress(self, description: str):
        """Return a progress context manager for long-running steps"""
        return Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            TimeElapsedColumn(),
            console=self.console,
            transient=True,
        )

    # ----------------------------
    # Summary
    # ----------------------------
    def print_summary(self):
        """Print pipeline execution summary"""
        # Save current step timing
        if self._current_step and self._step_start:
            elapsed = (datetime.now() - self._step_start).total_seconds()
            self.step_times[self._current_step] = elapsed

        total_time = datetime.now() - self.start_time

        self.console.print()
        self.console.print(Panel("[bold]Pipeline Summary[/bold]", style="green", expand=True))

        if self.step_times:
            t = Table(title="Step Timings", expand=True)
            t.add_column("Step", style="cyan")
            t.add_column("Duration", justify="right", style="magenta")
            t.add_column("Substeps", style="white")

            for step, duration in self.step_times.items():
                logs = self._step_logs.get(step, [])
                substeps_str = "\n".join(logs) if logs else "-"
                t.add_row(step, self._format_duration_secs(duration), substeps_str)

            t.add_row("[bold]Total[/bold]", f"[bold]{self._format_duration(total_time)}[/bold]", "-")
            self.console.print(t)

    # ----------------------------
    # Duration formatting
    # ----------------------------
    def _format_duration(self, delta) -> str:
        total_seconds = int(delta.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def _format_duration_secs(self, seconds: float) -> str:
        hours, remainder = divmod(int(seconds), 3600)
        minutes, secs = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {secs}s"
        elif minutes > 0:
            return f"{minutes}m {secs}s"
        return f"{secs}s"

    # ----------------------------
    # Save logs
    # ----------------------------
    def save(self):
        """Save logs to plain text and HTML"""
        log_file = os.path.join(self.jobs_dir, "charizard.log")
        html_file = os.path.join(self.jobs_dir, "charizard.html")

        # Plain text log
        with open(log_file, 'w') as f:
            f.write(self.console.export_text())

        # HTML log
        self.console.save_html(html_file, inline_styles=True)
        self.info(f"Logs saved to {log_file} and {html_file}")
