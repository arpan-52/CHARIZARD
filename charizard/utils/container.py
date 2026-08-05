# charizard/utils/container.py
"""
Container utilities for udocker integration.
Handles setup and command prefixing for all job submissions.
"""

import glob
import os
import shutil
import subprocess
import sys
from typing import Optional

DEFAULT_IMAGE = "apal52/charizard-pipeline:latest"
DEFAULT_NAME = "charizard"

# Thread-limit variables that must be forwarded into the container.
#
# udocker does NOT inherit the host environment. `export NUMBA_NUM_THREADS=8`
# written into the generated job script stops dead at the container boundary,
# and every library inside then sizes its pool from the NODE core count (64 on
# bhima) even though PBS confined the job to ncpus=8. Measured on bhima04:
# 209 threads per catboss process on an 8-core cpuset, 25% CPU, node loadavg
# 193, and catboss's Prep stage at 1092 s. Forwarding these takes it to 13 s.
#
# Deliberately NOT udocker's --hostenv: that drags the host PATH and
# LD_LIBRARY_PATH in too and breaks the container's bundled stacks.
THREAD_ENV_VARS = (
    "OMP_NUM_THREADS",
    "NUMBA_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)


def _mount_pairs(config) -> list:
    """(host_path, container_path) for every bind mount a job needs.

    Single source of truth for both the --volume flags and the mountpoints that
    must be pre-created inside the container ROOT (see ensure_mountpoints).
    """
    pairs = []

    # Always mount working dir
    wd = os.path.abspath(config.working_dir)
    pairs.append((wd, wd))

    # Mount MS directory if different
    ms_dir = os.path.dirname(os.path.abspath(config.ms_path))
    if ms_dir and ms_dir != wd:
        pairs.append((ms_dir, ms_dir))

    # User-specified extra volumes (format: "/src:/dst" or "/path")
    for v in (config.container.get('volumes') or []):
        if not v:
            continue
        if ':' in v:
            src, dst = v.split(':', 1)
            pairs.append((src, dst))
        else:
            pairs.append((v, v))

    return pairs


def ensure_mountpoints(config, logger=None) -> None:
    """Pre-create the bind mountpoints inside the container ROOT.

    THIS PREVENTS CONCURRENT JOBS FROM DESTROYING EACH OTHER'S WORKING DIRECTORY.

    udocker creates each --volume target as a real directory inside the shared
    container ROOT, and removes it (walking up its parents) when the job that
    created it exits. Every charizard job runs the SAME container name, so a
    whole SPW fan-out shares one ROOT. The first job to finish therefore deletes
    ROOT/<working_dir> - which is also every sibling's --workdir, hence its CWD.
    Their CWD becomes unlinked, getcwd() returns ENOENT, and casacore aborts
    with

        (/code/casa/OS/Path.cc : 407) Failed AlwaysAssert getcwd(temp, 1024)

    after which every flag write fails with [Errno 2]. It is silent, partial and
    duration-dependent: the longest-running job loses the most.

    udocker's MountPoint.create() returns early WITHOUT registering a mountpoint
    that already exists, and delete() only removes registered ones - so simply
    pre-creating these directories takes udocker out of the loop entirely.

    Measured on bhima04, two concurrent jobs, only the mountpoint owner differing:
        udocker-created mountpoint : 91 getcwd failures, MS unreadable
        pre-created mountpoint     :  0 failures, MS readable throughout

    Not passing an absolute MS path instead: casacore calls getcwd() regardless
    of whether the path is relative, so that fixes nothing (verified).
    """
    name = config.container.get('name', DEFAULT_NAME)
    root = _find_container_root(name)
    if not root:
        if logger:
            logger.warning(
                f"Container '{name}' not found - cannot pre-create mountpoints. "
                "Concurrent jobs may fail with 'Failed AlwaysAssert getcwd'. "
                "Run 'charizard setup env' first.")
        return

    for _, cont_path in _mount_pairs(config):
        target = os.path.join(root, cont_path.lstrip('/'))
        try:
            os.makedirs(target, exist_ok=True)
        except OSError as e:
            if logger:
                logger.warning(f"Could not pre-create mountpoint {target}: {e}")


def build_udocker_prefix(config) -> str:
    """
    Build the udocker run prefix for a job submission command.

    Automatically mounts working_dir and the MS directory, and forwards the
    scheduler's thread-limit env vars into the container (see THREAD_ENV_VARS -
    udocker does not inherit them, and without this every library inside sizes
    its thread pool from the node core count rather than the job's cpuset).

    Additional volumes can be specified in config.container.volumes, and extra
    environment variables in config.container.env.

    The result is meant to be embedded in a shell script: it contains
    ${VAR:+...} expansions that are resolved by bash at job run time.

    Args:
        config: PipelineConfig with container settings

    Returns:
        str like: "udocker run --workdir=W --volume=X:X
                   ${OMP_NUM_THREADS:+--env=OMP_NUM_THREADS=$OMP_NUM_THREADS} <name>"
    """
    container = config.container
    name = container.get('name', DEFAULT_NAME)

    wd = os.path.abspath(config.working_dir)
    volumes = [f"--volume={h}:{c}" for h, c in _mount_pairs(config)]

    # Forward thread limits set by the scheduler (housekeeper exports these into
    # the job script before this command runs). ${VAR:+...} expands to nothing
    # when VAR is unset, so this is a no-op unless the scheduler actually set it.
    env_flags = [f"${{{v}:+--env={v}=${v}}}" for v in THREAD_ENV_VARS]

    # Explicit extras from pokedex.yaml: environment.container.env
    for k, v in (container.get('env') or {}).items():
        env_flags.append(f"--env={k}={v}")

    return (f"udocker run --workdir={wd} {' '.join(volumes)} "
            f"{' '.join(env_flags)} {name}")


# ── GPU / NVIDIA driver plumbing ─────────────────────────────────────────────
#
# udocker's own `setup --nvidia` is not trustworthy on multilib clusters. On a
# host that ships both trees it takes /usr/lib first, and on bhima that is the
# 32-bit driver sitting next to the real one:
#
#     /usr/lib/libcuda.so.590.48.01     ELF 32-bit   21 MB   <- udocker took this
#     /usr/lib64/libcuda.so.590.48.01   ELF 64-bit   98 MB   <- the correct one
#
# The container then loads a 32-bit libcuda into a 64-bit process, cupy fails to
# initialise, and catboss reports "Mode: CPU only" with no error anyone sees. So
# we run udocker's setup for the bind plumbing, then place the libraries
# ourselves with an explicit ELF-class check.

# Driver libraries the container needs. These come from the HOST driver and must
# match the running kernel module - they are deliberately not in the image.
NVIDIA_LIB_PATTERNS = (
    "libcuda.so*",                  # the driver API itself
    "libnvidia-ml.so*",             # NVML, what nvidia-smi and jax probe with
    "libnvidia-nvvm*.so*",          # NVVM, needed by numba's CUDA target
    "libnvidia-ptxjitcompiler.so*",  # PTX JIT - without it kernels never compile
    "libnvcuvid.so*",
    "libnvidia-fatbinaryloader.so*",
)

# Where they must land inside the image. The base is nvidia/cuda:*-ubuntu22.04,
# so glibc looks in the multiarch directory. udocker's own injection targets
# /usr/lib, which is on the search path but loses to any stale copy in the
# multiarch dir - place them where the loader looks first.
CONTAINER_LIB_DIR = "usr/lib/x86_64-linux-gnu"

# Ordered best-first. /usr/lib64 leads deliberately: see the note above.
CUDA_LIB_SEARCH_PATH = (
    "/usr/lib64",
    "/usr/lib/x86_64-linux-gnu",
    "/usr/lib",
    "/lib64",
    "/lib/x86_64-linux-gnu",
    "/usr/local/cuda/compat",
)


def _elf_is_64bit(path: str) -> bool:
    """True if path is a 64-bit ELF. e_ident[EI_CLASS] == 2 means ELFCLASS64."""
    try:
        with open(path, "rb") as f:
            ident = f.read(5)
        return len(ident) == 5 and ident[:4] == b"\x7fELF" and ident[4] == 2
    except OSError:
        return False


def _detect_cuda_lib_dirs() -> list:
    """Host directories holding a 64-bit libcuda, best candidate first.

    32-bit copies are rejected outright rather than ranked lower - injecting one
    is worse than injecting nothing, because the failure is silent.
    """
    found = []
    seen = set()

    def consider(directory: str) -> None:
        if not directory or not os.path.isdir(directory):
            return
        # Dedupe by real path: many distros make /usr/lib64 and /lib64 symlinks
        # to /usr/lib, and without this the same tree is injected and probed
        # once per alias - three ~100 MB copies and three probe runs for nothing.
        key = os.path.realpath(directory)
        if key in seen:
            return
        for cand in glob.glob(os.path.join(directory, "libcuda.so*")):
            real = os.path.realpath(cand)
            if os.path.isfile(real) and _elf_is_64bit(real):
                seen.add(key)
                found.append(directory)
                return

    # ldconfig is authoritative on a properly configured host.
    try:
        out = subprocess.run(["ldconfig", "-p"], capture_output=True,
                             text=True, timeout=15).stdout
        for line in out.splitlines():
            if "libcuda.so" in line and "=>" in line:
                consider(os.path.dirname(line.split("=>")[-1].strip()))
    except Exception:
        pass

    for d in CUDA_LIB_SEARCH_PATH:
        consider(d)

    return found


def _host_has_gpu() -> bool:
    """Whether this machine can actually drive a GPU right now.

    Deliberately checks the device node too: on a login node nvidia-smi may be
    installed while no device exists, and on a compute node the reverse.
    """
    if glob.glob("/dev/nvidia[0-9]*"):
        return True
    if shutil.which("nvidia-smi"):
        try:
            ret = subprocess.run(["nvidia-smi", "-L"], capture_output=True,
                                 text=True, timeout=30)
            return ret.returncode == 0 and "GPU" in ret.stdout
        except Exception:
            return False
    return False


def _find_container_root(name: str) -> Optional[str]:
    """Locate the container's ROOT filesystem.

    Honors $UDOCKER_DIR (used on HPC clusters where udocker lives on shared
    scratch, e.g. /lustre/.../udocker), falling back to ~/.udocker.
    """
    udocker_dir = os.environ.get("UDOCKER_DIR") or os.path.expanduser("~/.udocker")
    containers_dir = os.path.join(udocker_dir, "containers")
    if not os.path.isdir(containers_dir):
        return None

    direct = os.path.join(containers_dir, name, "ROOT")
    if os.path.exists(direct):
        return direct

    # UUID-named directories - check container.json for our name
    for d in os.listdir(containers_dir):
        root = os.path.join(containers_dir, d, "ROOT")
        meta = os.path.join(containers_dir, d, "container.json")
        if os.path.exists(root) and os.path.exists(meta):
            try:
                with open(meta) as f:
                    if name in f.read():
                        return root
            except OSError:
                continue
    return None


def _inject_nvidia(name: str, src_dir: str, console) -> int:
    """Copy host driver libs into the container, preserving the symlink chain.

    Real files are copied first, then symlinks are recreated as links rather
    than dereferenced. That matters: the loader resolves a DT_NEEDED of
    `libcuda.so.1`, so a copy named `libcuda.so.590.48.01` alone is invisible to
    it, and dereferencing the chain would waste ~100 MB per duplicate.
    """
    root = _find_container_root(name)
    if not root:
        console.print("[red]  Could not find container ROOT - is the container created?[/red]")
        return 0

    dest = os.path.join(root, CONTAINER_LIB_DIR)
    os.makedirs(dest, exist_ok=True)

    reals, links = [], []
    for pattern in NVIDIA_LIB_PATTERNS:
        for src in glob.glob(os.path.join(src_dir, pattern)):
            (links if os.path.islink(src) else reals).append(src)

    copied = 0
    for src in reals:
        if not _elf_is_64bit(src):
            console.print(f"  [yellow]skipped {os.path.basename(src)} (32-bit)[/yellow]")
            continue
        shutil.copy2(src, os.path.join(dest, os.path.basename(src)))
        copied += 1

    linked = 0
    for src in links:
        target = os.readlink(src)
        # Only same-directory links are meaningful once relocated.
        target = os.path.basename(target)
        dst = os.path.join(dest, os.path.basename(src))
        if os.path.basename(src) == target:
            continue
        # Never leave a dangling link behind: if the target was skipped (32-bit,
        # say), a link named libcuda.so.1 pointing at nothing is worse than no
        # link at all - it shadows whatever else might have satisfied the loader.
        if not os.path.exists(os.path.join(dest, target)):
            continue
        try:
            if os.path.lexists(dst):
                os.remove(dst)
            os.symlink(target, dst)
            linked += 1
        except OSError as e:
            console.print(f"  [yellow]could not link {os.path.basename(src)}: {e}[/yellow]")

    # Safety net: some hosts ship only the versioned file with no SONAME link.
    # Without libcuda.so.1 nothing loads, so synthesise the chain.
    for stem in ("libcuda.so", "libnvidia-ml.so"):
        versioned = sorted(glob.glob(os.path.join(dest, stem + ".*.*")))
        soname = os.path.join(dest, stem + ".1")
        if versioned and not os.path.lexists(soname):
            os.symlink(os.path.basename(versioned[-1]), soname)
            linked += 1
        plain = os.path.join(dest, stem)
        if os.path.lexists(soname) and not os.path.lexists(plain):
            os.symlink(os.path.basename(soname), plain)
            linked += 1

    # nvidia-smi is not a library but every diagnostic anyone runs starts with it.
    smi = shutil.which("nvidia-smi")
    if smi:
        bindir = os.path.join(root, "usr/bin")
        os.makedirs(bindir, exist_ok=True)
        try:
            shutil.copy2(smi, os.path.join(bindir, "nvidia-smi"))
        except OSError:
            pass

    console.print(f"[green]  Injected {copied} libraries and {linked} symlinks "
                  f"from {src_dir}[/green]")
    return copied


# Staged probe. Each stage maps to a failure that has actually shipped, and they
# are ordered so the first FAIL names the real cause instead of a symptom.
_GPU_PROBE = r"""
import sys
def stage(label, fn):
    try:
        print("  %-22s OK   %s" % (label, fn() or ""))
        return True
    except Exception as e:
        print("  %-22s FAIL %s: %s" % (label, type(e).__name__, str(e)[:160]))
        return False

def _driver():
    import ctypes
    ctypes.CDLL("libcuda.so.1")
    return "libcuda.so.1 loads"

def _cupy():
    import cupy
    n = cupy.cuda.runtime.getDeviceCount()
    assert n > 0, "no CUDA devices visible"
    a = cupy.arange(1024, dtype=cupy.float32)
    assert float((a * 2).sum()) > 0
    return "cupy %s, %d device(s)" % (cupy.__version__, n)

def _jax():
    import jax
    d = jax.devices()
    assert any(x.platform == "gpu" for x in d), "jax sees no GPU: %r" % (d,)
    return str(d)

def _numba():
    from numba import cuda
    import numpy as np
    assert cuda.is_available(), "numba.cuda.is_available() is False"
    @cuda.jit
    def k(x):
        i = cuda.grid(1)
        if i < x.size:
            x[i] += 1.0
    a = np.zeros(64, dtype=np.float32)
    d = cuda.to_device(a)
    k[1, 64](d)
    assert d.copy_to_host()[0] == 1.0
    return "kernel compiled and ran"

def _catboss():
    from catboss.pooh.methods import sumthreshold as st
    src = open(st.__file__).read()
    assert "max(1, int(M * 0.3))" not in src, \
        "2-arg max present; numba >=0.66 cannot compile it -> silent CPU fallback"
    return "CUDA kernels clean"

ok = True
for label, fn in [("driver (libcuda)", _driver), ("cupy", _cupy), ("jax", _jax),
                  ("numba cuda kernel", _numba), ("catboss kernels", _catboss)]:
    ok &= stage(label, fn)
print("GPU_PROBE_RESULT=%s" % ("PASS" if ok else "FAIL"))
"""


def _verify_gpu(name: str, console, verbose: bool = True) -> bool:
    """Run the staged GPU probe inside the container."""
    try:
        ret = subprocess.run(
            ["udocker", "run", name, "/opt/envs/312data/bin/python", "-c", _GPU_PROBE],
            capture_output=True, text=True, timeout=600
        )
    except Exception as e:
        console.print(f"[red]  Could not run the GPU probe: {e}[/red]")
        return False

    if verbose:
        for line in ret.stdout.splitlines():
            if line.startswith("  ") or "GPU_PROBE_RESULT" in line:
                console.print(f"  [dim]{line.strip()}[/dim]" if "OK" in line else f"  {line.strip()}")
    return "GPU_PROBE_RESULT=PASS" in ret.stdout


def _ask(console, question: str, default):
    """Prompt with a default. Falls back to the default when not a terminal."""
    if not sys.stdin.isatty():
        return default
    from rich.prompt import Confirm, Prompt
    if isinstance(default, bool):
        return Confirm.ask(f"  {question}", default=default)
    return Prompt.ask(f"  {question}", default=str(default))


def setup_container(image: str = DEFAULT_IMAGE,
                    name: str = DEFAULT_NAME,
                    cuda_lib_path: Optional[str] = None,
                    gpu: str = "auto",
                    skip_pull: bool = False,
                    gpu_only: bool = False,
                    udocker_dir: Optional[str] = None,
                    interactive: bool = False) -> bool:
    """
    One-time setup: pull image, create container, wire up the GPU, verify.

    Args:
        image:         Docker image to pull
        name:          Container name for udocker
        cuda_lib_path: Host directory holding the NVIDIA driver libs. Auto-
                       detected when omitted; pass it when the host keeps them
                       somewhere unusual, or to override a bad guess.
        gpu:           "auto" (enable if this host has a GPU), "on" (require it,
                       fail the setup if it does not work), or "off".
        skip_pull:     Reuse an already-pulled image instead of fetching it.
        gpu_only:      Only redo the GPU step against an existing container.
        udocker_dir:   Sets $UDOCKER_DIR for this run and everything it calls.
        interactive:   Prompt for the options instead of taking the defaults.

    Split-node clusters: some sites have no single node with both outbound
    network and a GPU driver (bhima is one - the login node has network and no
    driver, bhima04 has a driver and no network). Run it in two passes:

        # on the node with network
        charizard setup env --no-gpu
        # in a GPU job, against the container that already exists
        charizard setup env --gpu-only --gpu on

    Returns:
        True if setup succeeded. With gpu="on" a GPU that does not verify is a
        failure; with "auto" it is a warning.
    """
    from rich.console import Console
    from rich.panel import Panel

    console = Console()
    console.print(Panel("[bold cyan]CHARIZARD - Container Setup[/bold cyan]"))

    if udocker_dir:
        os.environ["UDOCKER_DIR"] = os.path.abspath(os.path.expanduser(udocker_dir))
    active_udocker_dir = os.environ.get("UDOCKER_DIR") or os.path.expanduser("~/.udocker")

    host_gpu = _host_has_gpu()

    # ── Interactive: ask rather than guess ──────────────────────────────────
    if interactive:
        console.print("\n[bold]Options[/bold] (Enter accepts the default)")
        image = _ask(console, "Image", image)
        name = _ask(console, "Container name", name)
        udocker_dir = _ask(console, "UDOCKER_DIR", active_udocker_dir)
        os.environ["UDOCKER_DIR"] = os.path.abspath(os.path.expanduser(str(udocker_dir)))
        active_udocker_dir = os.environ["UDOCKER_DIR"]
        skip_pull = not _ask(console, "Pull the image now (no on an offline node)", not skip_pull)

        console.print(f"\n  [dim]This host {'has' if host_gpu else 'does NOT have'} "
                      f"a usable GPU right now.[/dim]")
        gpu = "on" if _ask(console, "Enable GPU support", host_gpu) else "off"

        if gpu == "on":
            detected = _detect_cuda_lib_dirs()
            if detected:
                console.print("  [dim]64-bit driver libraries found in: "
                              + ", ".join(detected) + "[/dim]")
            else:
                console.print("  [yellow]No 64-bit libcuda found on the usual paths.[/yellow]")
            cuda_lib_path = _ask(console, "Driver library directory",
                                 cuda_lib_path or (detected[0] if detected else "/usr/lib64"))

    console.print(f"\n[dim]UDOCKER_DIR = {active_udocker_dir}[/dim]")

    # ── 1. Pull ─────────────────────────────────────────────────────────────
    if gpu_only or skip_pull:
        console.print(f"\n[bold]1.[/bold] Skipping pull "
                      f"({'--gpu-only' if gpu_only else '--skip-pull'})")
    else:
        console.print(f"\n[bold]1.[/bold] Pulling [cyan]{image}[/cyan] ...")
        if subprocess.run(["udocker", "pull", image], check=False).returncode != 0:
            console.print("[red]ERROR: Failed to pull image. Check the image name and network.[/red]")
            console.print("[yellow]  On a node without outbound network, pull elsewhere and "
                          "re-run with --skip-pull.[/yellow]")
            return False
        console.print("[green]  Pull OK[/green]")

    # ── 2. Create ───────────────────────────────────────────────────────────
    if gpu_only:
        if not _find_container_root(name):
            console.print(f"[red]ERROR: --gpu-only needs an existing container named "
                          f"'{name}' under {active_udocker_dir}.[/red]")
            return False
        console.print(f"\n[bold]2.[/bold] Reusing existing container [cyan]{name}[/cyan]")
    else:
        console.print(f"\n[bold]2.[/bold] Creating container [cyan]{name}[/cyan] ...")
        if subprocess.run(["udocker", "rm", name],
                          capture_output=True, check=False).returncode == 0:
            console.print(f"[yellow]  Removed existing container '{name}'[/yellow]")
        if subprocess.run(["udocker", "create", f"--name={name}", image],
                          check=False).returncode != 0:
            console.print("[red]ERROR: Failed to create container.[/red]")
            return False
        console.print("[green]  Container created[/green]")

    # ── 3. GPU ──────────────────────────────────────────────────────────────
    want_gpu = {"on": True, "off": False}.get(str(gpu).lower(), host_gpu)
    gpu_ok = False

    if not want_gpu:
        reason = "--no-gpu" if str(gpu).lower() == "off" else "no GPU detected on this host"
        console.print(f"\n[bold]3.[/bold] GPU support: [yellow]skipped ({reason})[/yellow]")
        console.print("  [dim]catboss will flag on CPU. Re-run with --gpu-only --gpu on "
                      "from a GPU node to enable it.[/dim]")
    else:
        console.print(f"\n[bold]3.[/bold] Configuring GPU support ...")
        if not host_gpu:
            console.print("[yellow]  Warning: no GPU visible on this host - the libraries "
                          "will be staged but cannot be verified here.[/yellow]")

        # udocker's own setup does the bind plumbing; we place the libraries.
        subprocess.run(["udocker", "setup", "--nvidia", "--force", name],
                       capture_output=True, check=False)

        injected = 0
        sources = [cuda_lib_path] if cuda_lib_path else _detect_cuda_lib_dirs()
        if not sources:
            console.print("[red]  No 64-bit libcuda found.[/red]")
            console.print("  [yellow]Searched: " + ", ".join(CUDA_LIB_SEARCH_PATH) +
                          "\n  Pass the right directory with --cuda-lib-path.[/yellow]")
        else:
            for src in sources:
                if not os.path.isdir(src):
                    console.print(f"[yellow]  {src} does not exist - skipping[/yellow]")
                    continue
                console.print(f"  Using driver libraries from [cyan]{src}[/cyan]")
                n_here = _inject_nvidia(name, src, console)
                injected += n_here
                if n_here == 0:
                    # Nothing usable here (an all-32-bit tree, say). Try the next
                    # candidate rather than giving up - on a multilib host the
                    # first hit is often the wrong architecture.
                    continue
                if not host_gpu:
                    break          # staged; nothing on this host to verify against
                console.print("\n[bold]4.[/bold] Verifying GPU inside the container ...")
                if _verify_gpu(name, console):
                    gpu_ok = True
                    break
                console.print(f"[yellow]  Verification failed with libs from {src}[/yellow]")

        if gpu_ok:
            console.print("[green]  GPU: OK[/green]")
        elif injected and not host_gpu:
            # Only honest when libraries really were copied. A host with no
            # driver has nothing to stage in the first place - the libs come
            # from the host, not the image.
            console.print("[yellow]  GPU: libraries staged but UNVERIFIED (no GPU on this host).\n"
                          "  Re-run '--gpu-only --gpu' from a GPU node to confirm.[/yellow]")
        else:
            console.print("[red]  GPU: NOT working - catboss will fall back to CPU flagging.[/red]")
            if not host_gpu:
                console.print("  [yellow]This host has no GPU driver, so there is nothing to "
                              "copy. Run this on the GPU node:\n"
                              "    charizard setup env --gpu-only --gpu[/yellow]")

        # --gpu means "GPU support verified working". Anything less is a failure,
        # including the case where there was no driver here to stage.
        if str(gpu).lower() == "on" and not gpu_ok:
            _print_config_snippet(image, name, cuda_lib_path, gpu_ok, console,
                                  active_udocker_dir)
            return False

    _print_config_snippet(image, name, cuda_lib_path, gpu_ok, console, active_udocker_dir)
    return True


def _print_config_snippet(image, name, cuda_lib_path, gpu_ok, console,
                          udocker_dir=None) -> None:
    """Print the pokedex.yaml snippet the user needs to add."""
    from rich.panel import Panel

    lines = [
        "environment:",
        "  container:",
        f"    image: {image}",
        f"    name: {name}",
        "    volumes: []  # add extra mounts if needed, e.g. /scratch:/scratch",
    ]
    if udocker_dir:
        lines += [
            "  shell_preamble: |",
            "    source ~/.bashrc",
            f"    export UDOCKER_DIR={udocker_dir}",
        ]

    snippet = "\n".join(lines)
    if gpu_ok:
        status = "[green]GPU ready[/green] - set `flagging: {use_gpu: true}` in your flow"
    else:
        status = "[yellow]GPU unavailable[/yellow] - keep `use_gpu: false`, or re-run with --gpu-only"

    console.print(Panel(
        f"{status}\n\n"
        f"Add this to your [bold]pokedex.yaml[/bold] under [bold]environment:[/bold]\n\n"
        + snippet,
        title="[bold green]Setup Complete[/bold green]"
    ))
