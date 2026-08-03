#!/usr/bin/env python3
"""
Tests for `charizard setup env` GPU wiring.

Run: python tests/test_container_gpu.py     (no pytest needed, exits non-zero on failure)

These are regression tests for bugs that cost real cluster time, not coverage
padding. The central one is the bhima multilib trap: a host carrying both a
32-bit and a 64-bit NVIDIA driver, where udocker's own `setup --nvidia` picks
the 32-bit one, cupy fails to initialise, and catboss reports "Mode: CPU only"
with nothing in any log to say why.
"""

import glob
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rich.console import Console

from charizard.utils import container as C

console = Console(quiet=True)
FAILURES = []


def check(label, cond, extra=""):
    if cond:
        print("  PASS  %s %s" % (label, extra))
    else:
        FAILURES.append(label)
        print("  FAIL  %s %s" % (label, extra))


def _fake_elf32(path):
    """A believable 32-bit shared object: correct magic, ELFCLASS32."""
    with open(path, "wb") as f:
        f.write(b"\x7fELF\x01" + b"\x00" * 123)


def _host_libcuda():
    """A real 64-bit libcuda to copy around, or None on a driverless host."""
    for cand in ("/usr/lib/libcuda.so.1", "/usr/lib64/libcuda.so.1",
                 "/usr/lib/x86_64-linux-gnu/libcuda.so.1"):
        if os.path.exists(cand):
            return os.path.realpath(cand)
    return None


def test_elf_class(tmp):
    print("\n[ELF class detection]")
    real = _host_libcuda()
    if real:
        check("real libcuda reads as 64-bit", C._elf_is_64bit(real), real)
    else:
        print("  SKIP  no host driver to test against")

    f32 = os.path.join(tmp, "libcuda.so.1.2.3")
    _fake_elf32(f32)
    check("32-bit ELF rejected", not C._elf_is_64bit(f32))

    notelf = os.path.join(tmp, "notanelf.so")
    open(notelf, "wb").write(b"hello world")
    check("non-ELF rejected", not C._elf_is_64bit(notelf))
    check("missing file rejected", not C._elf_is_64bit(os.path.join(tmp, "nope")))


def test_multilib_trap(tmp):
    """The bhima case: 32-bit in /usr/lib, 64-bit in /usr/lib64."""
    print("\n[multilib trap]")
    real = _host_libcuda()
    if not real:
        print("  SKIP  needs a host driver to build the fixture")
        return

    lib32 = os.path.join(tmp, "ml/usr/lib")
    lib64 = os.path.join(tmp, "ml/usr/lib64")
    os.makedirs(lib32)
    os.makedirs(lib64)
    _fake_elf32(os.path.join(lib32, "libcuda.so.590.48.01"))
    os.symlink("libcuda.so.590.48.01", os.path.join(lib32, "libcuda.so.1"))
    shutil.copy(real, os.path.join(lib64, "libcuda.so.590.48.01"))
    os.symlink("libcuda.so.590.48.01", os.path.join(lib64, "libcuda.so.1"))
    os.symlink("libcuda.so.1", os.path.join(lib64, "libcuda.so"))

    saved_path, saved_run = C.CUDA_LIB_SEARCH_PATH, subprocess.run
    C.CUDA_LIB_SEARCH_PATH = (lib64, lib32)
    # Force the directory scan rather than trusting the host's ldconfig.
    C.subprocess.run = lambda *a, **k: (_ for _ in ()).throw(OSError("no ldconfig"))
    try:
        dirs = C._detect_cuda_lib_dirs()
    finally:
        C.CUDA_LIB_SEARCH_PATH, C.subprocess.run = saved_path, saved_run

    check("32-bit directory excluded entirely", lib32 not in dirs, str(dirs))
    check("64-bit directory found", lib64 in dirs)

    # Even ranked first, the 32-bit tree must not stop the search.
    udk = os.path.join(tmp, "ml/udocker")
    root = os.path.join(udk, "containers", "c", "ROOT")
    os.makedirs(root)
    os.environ["UDOCKER_DIR"] = udk

    chosen = None
    for src in (lib32, lib64):                      # deliberately wrong order
        if C._inject_nvidia("c", src, console) == 0:
            continue
        chosen = src
        break
    check("falls through 32-bit dir to the 64-bit one", chosen == lib64, str(chosen))

    dest = os.path.join(root, C.CONTAINER_LIB_DIR)
    blob = os.path.join(dest, "libcuda.so.590.48.01")
    check("injected library is 64-bit", os.path.isfile(blob) and C._elf_is_64bit(blob))


def test_symlink_chain(tmp):
    """The loader resolves DT_NEEDED libcuda.so.1 - the chain must survive."""
    print("\n[symlink chain]")
    real = _host_libcuda()
    if not real:
        print("  SKIP  needs a host driver")
        return

    src = os.path.join(tmp, "sc/lib")
    os.makedirs(src)
    shutil.copy(real, os.path.join(src, "libcuda.so.590.48.01"))
    os.symlink("libcuda.so.590.48.01", os.path.join(src, "libcuda.so.1"))
    os.symlink("libcuda.so.1", os.path.join(src, "libcuda.so"))

    udk = os.path.join(tmp, "sc/udocker")
    root = os.path.join(udk, "containers", "c", "ROOT")
    os.makedirs(root)
    os.environ["UDOCKER_DIR"] = udk
    check("container ROOT located", C._find_container_root("c") == root)

    C._inject_nvidia("c", src, console)
    dest = os.path.join(root, C.CONTAINER_LIB_DIR)
    soname = os.path.join(dest, "libcuda.so.1")
    plain = os.path.join(dest, "libcuda.so")
    blob = os.path.join(dest, "libcuda.so.590.48.01")

    check("versioned file copied as a real file",
          os.path.isfile(blob) and not os.path.islink(blob))
    check("libcuda.so.1 stays a SYMLINK", os.path.islink(soname))
    check("libcuda.so -> .so.1", os.path.islink(plain))
    check("chain resolves to the real file",
          os.path.realpath(plain) == os.path.realpath(blob))
    check("no duplicated ~100 MB blob",
          sum(1 for f in glob.glob(dest + "/libcuda.so*") if not os.path.islink(f)) == 1)


def test_no_dangling_links(tmp):
    """A link whose target was skipped is worse than no link at all."""
    print("\n[dangling links]")
    src = os.path.join(tmp, "dl/lib")
    os.makedirs(src)
    _fake_elf32(os.path.join(src, "libcuda.so.590.48.01"))
    os.symlink("libcuda.so.590.48.01", os.path.join(src, "libcuda.so.1"))

    udk = os.path.join(tmp, "dl/udocker")
    root = os.path.join(udk, "containers", "c", "ROOT")
    os.makedirs(root)
    os.environ["UDOCKER_DIR"] = udk

    n = C._inject_nvidia("c", src, console)
    check("all-32-bit source injects nothing", n == 0, "copied=%d" % n)
    dest = os.path.join(root, C.CONTAINER_LIB_DIR)
    dangling = [f for f in glob.glob(dest + "/*")
                if os.path.islink(f) and not os.path.exists(f)]
    check("no dangling symlinks left behind", not dangling, str(dangling))


def _quiet_setup(**kw):
    """setup_container builds its own Console, so mute it at the stdout level."""
    import contextlib
    import io
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        return C.setup_container(**kw)


def test_gpu_flag_contract(tmp):
    """--gpu means "verified working"; auto is advisory. Nothing in between."""
    print("\n[--gpu / --no-gpu contract]")
    udk = os.path.join(tmp, "fc/udocker")
    os.makedirs(os.path.join(udk, "containers", "c1", "ROOT"))
    os.environ["UDOCKER_DIR"] = udk

    saved = (C._host_has_gpu, C._detect_cuda_lib_dirs,
             C._inject_nvidia, C._verify_gpu, C.subprocess.run)
    C.subprocess.run = lambda *a, **k: type(
        "R", (), {"returncode": 0, "stdout": "", "stderr": ""})()
    try:
        # A host with no driver has nothing to stage, so --gpu cannot succeed.
        C._host_has_gpu = lambda: False
        C._detect_cuda_lib_dirs = lambda: []
        check("--gpu on a driverless host fails",
              _quiet_setup(name="c1", gpu_only=True, gpu="on") is False)
        check("--no-gpu on a driverless host succeeds",
              _quiet_setup(name="c1", gpu_only=True, gpu="off") is True)
        check("auto on a driverless host succeeds (advisory)",
              _quiet_setup(name="c1", gpu_only=True, gpu="auto") is True)

        C._host_has_gpu = lambda: True
        C._detect_cuda_lib_dirs = lambda: ["/usr/lib"]
        C._inject_nvidia = lambda *a, **k: 5
        C._verify_gpu = lambda *a, **k: False
        check("--gpu with a failing probe fails",
              _quiet_setup(name="c1", gpu_only=True, gpu="on") is False)
        check("auto with a failing probe succeeds (advisory)",
              _quiet_setup(name="c1", gpu_only=True, gpu="auto") is True)

        C._verify_gpu = lambda *a, **k: True
        check("--gpu with a passing probe succeeds",
              _quiet_setup(name="c1", gpu_only=True, gpu="on") is True)
    finally:
        (C._host_has_gpu, C._detect_cuda_lib_dirs,
         C._inject_nvidia, C._verify_gpu, C.subprocess.run) = saved


def main():
    tmp = tempfile.mkdtemp(prefix="charizard_gputest_")
    saved_udocker_dir = os.environ.get("UDOCKER_DIR")
    try:
        test_elf_class(tmp)
        test_multilib_trap(tmp)
        test_symlink_chain(tmp)
        test_no_dangling_links(tmp)
        test_gpu_flag_contract(tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        if saved_udocker_dir is None:
            os.environ.pop("UDOCKER_DIR", None)
        else:
            os.environ["UDOCKER_DIR"] = saved_udocker_dir

    print()
    if FAILURES:
        print("FAILED: %s" % ", ".join(FAILURES))
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
