# charizard/utils/container.py
"""
Container utilities for udocker integration.
Handles setup and command prefixing for all job submissions.
"""

import glob
import os
import shutil
import subprocess
from typing import Optional

DEFAULT_IMAGE = "apal52/charizard-pipeline:latest"
DEFAULT_NAME = "charizard"


def build_udocker_prefix(config) -> str:
    """
    Build the udocker run prefix for a job submission command.

    Automatically mounts working_dir and the MS directory.
    Additional volumes can be specified in config.container.volumes.

    Args:
        config: PipelineConfig with container settings

    Returns:
        str like: "udocker run --volume=X:X --volume=Y:Y <name>"
    """
    container = config.container
    name = container.get('name', DEFAULT_NAME)

    volumes = []

    # Always mount working dir
    wd = os.path.abspath(config.working_dir)
    volumes.append(f"--volume={wd}:{wd}")

    # Mount MS directory if different
    ms_dir = os.path.dirname(os.path.abspath(config.ms_path))
    if ms_dir and ms_dir != wd:
        volumes.append(f"--volume={ms_dir}:{ms_dir}")

    # User-specified extra volumes (format: "/src:/dst" or "/path")
    for v in container.get('volumes', []):
        if v:
            if ':' in v:
                volumes.append(f"--volume={v}")
            else:
                volumes.append(f"--volume={v}:{v}")

    return f"udocker run --workdir={wd} {' '.join(volumes)} {name}"


def setup_container(image: str = DEFAULT_IMAGE,
                    name: str = DEFAULT_NAME,
                    cuda_lib_path: Optional[str] = None) -> bool:
    """
    One-time setup: pull image, create container, configure NVIDIA, test GPU.
    Prints the config snippet to add to pokedex.yaml at the end.

    Args:
        image:         Docker image to pull (default: apal52/charizard-pipeline:latest)
        name:          Container name for udocker (default: charizard)
        cuda_lib_path: Path to host CUDA libs if auto-detect fails (e.g. /usr/lib)

    Returns:
        True if setup succeeded
    """
    from rich.console import Console
    from rich.panel import Panel

    console = Console()
    console.print(Panel("[bold cyan]CHARIZARD — Container Setup[/bold cyan]"))

    # 1. Pull image
    console.print(f"\n[bold]1.[/bold] Pulling [cyan]{image}[/cyan] ...")
    ret = subprocess.run(["udocker", "pull", image], check=False)
    if ret.returncode != 0:
        console.print("[red]ERROR: Failed to pull image. Check image name and network.[/red]")
        return False
    console.print("[green]  Pull OK[/green]")

    # 2. Create container
    console.print(f"\n[bold]2.[/bold] Creating container [cyan]{name}[/cyan] ...")
    ret = subprocess.run(["udocker", "create", f"--name={name}", image], check=False)
    if ret.returncode != 0:
        console.print("[yellow]  Note: container may already exist — continuing[/yellow]")

    # 3. NVIDIA setup
    console.print(f"\n[bold]3.[/bold] Configuring NVIDIA GPU support ...")
    subprocess.run(["udocker", "setup", "--nvidia", "--force", name], check=False)

    # 4. GPU test
    console.print(f"\n[bold]4.[/bold] Testing GPU ...")
    gpu_ok = _test_gpu(name)

    if not gpu_ok:
        if cuda_lib_path:
            console.print(f"[yellow]  GPU test failed — injecting libs from {cuda_lib_path}[/yellow]")
            _inject_cuda_libs(name, cuda_lib_path, console)
            gpu_ok = _test_gpu(name)
        else:
            console.print(
                "[yellow]  GPU test failed — if you have CUDA libs at a non-standard path,\n"
                "  re-run with --cuda-lib-path /path/to/libs[/yellow]"
            )

    if gpu_ok:
        console.print("[green]  GPU: OK[/green]")
    else:
        console.print("[yellow]  GPU: NOT available — catboss (GPU flagging) will not work.[/yellow]")

    # 5. Print config snippet
    _print_config_snippet(image, name, cuda_lib_path, gpu_ok, console)
    return True


# ── Helpers ──────────────────────────────────────────────────────────────────

def _test_gpu(name: str) -> bool:
    """Run a quick cupy GPU test inside the container."""
    try:
        ret = subprocess.run(
            ["udocker", "run", name,
             "/opt/envs/312data/bin/python", "-c",
             "import cupy; cupy.cuda.runtime.getDeviceCount(); print('GPU_OK')"],
            capture_output=True, text=True, timeout=60
        )
        return "GPU_OK" in ret.stdout
    except Exception:
        return False


def _find_container_root(name: str) -> Optional[str]:
    """Locate the container's ROOT filesystem inside ~/.udocker."""
    containers_dir = os.path.expanduser("~/.udocker/containers")

    # Direct name match
    direct = os.path.join(containers_dir, name, "ROOT")
    if os.path.exists(direct):
        return direct

    # UUID-named directories — check container.json for our name
    for d in os.listdir(containers_dir):
        root = os.path.join(containers_dir, d, "ROOT")
        meta = os.path.join(containers_dir, d, "container.json")
        if os.path.exists(root) and os.path.exists(meta):
            with open(meta) as f:
                if name in f.read():
                    return root

    return None


def _inject_cuda_libs(name: str, cuda_lib_path: str, console) -> None:
    """Copy host CUDA driver libs into the container filesystem."""
    root = _find_container_root(name)
    if not root:
        console.print("[red]  Could not find container ROOT — skipping lib injection[/red]")
        return

    dest = os.path.join(root, "usr/lib")
    os.makedirs(dest, exist_ok=True)

    copied = 0
    for pattern in ["libcuda.so*", "libnvidia*.so*", "libnvcuvid*.so*"]:
        for lib in glob.glob(os.path.join(cuda_lib_path, pattern)):
            shutil.copy2(lib, os.path.join(dest, os.path.basename(lib)))
            console.print(f"  [dim]Copied {os.path.basename(lib)}[/dim]")
            copied += 1

    if copied == 0:
        console.print(f"[yellow]  No CUDA libs found at {cuda_lib_path}[/yellow]")
    else:
        console.print(f"[green]  Injected {copied} CUDA libs[/green]")


def _print_config_snippet(image, name, cuda_lib_path, gpu_ok, console) -> None:
    """Print the pokedex.yaml snippet the user needs to add."""
    from rich.panel import Panel

    lines = [
        "environment:",
        "  container:",
        f"    image: {image}",
        f"    name: {name}",
    ]
    if cuda_lib_path:
        lines.append(f"    cuda_lib_path: {cuda_lib_path}")
    lines.append("    volumes: []  # add extra mounts if needed, e.g. /scratch:/scratch")

    snippet = "\n".join(lines)
    gpu_status = "[green]GPU ready[/green]" if gpu_ok else "[yellow]GPU unavailable[/yellow]"

    console.print(Panel(
        f"{gpu_status}\n\n"
        f"Add this to your [bold]pokedex.yaml[/bold] under [bold]environment:[/bold]\n\n"
        + snippet,
        title="[bold green]Setup Complete[/bold green]"
    ))
