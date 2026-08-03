#!/usr/bin/env python3
# charizard/main.py
"""
CHARIZARD - Radio Interferometry Calibration Pipeline
Entry point - parse args, define whitelist, call charizard()
"""

import argparse
import os
import sys

from .charizard import charizard
from .utils.general.config_parser import parse_config
from .utils.general.logging import PipelineLogger
from .utils.container import setup_container, DEFAULT_IMAGE, DEFAULT_NAME


# =============================================================================
# ERROR WHITELIST - passed to housekeeper for log checking
# =============================================================================

ERROR_WHITELIST = [
    # CASA harmless errors
    "the only error message you will receive",
    "prterun has exited",
    "[TerminalIPythonApp] ERROR | Failed to create history session",
    "sqlite3.OperationalError: database is locked",
    "getcell::TIME   Exception Reported: TableProxy::getCell: no such row",
    "TableProxy::getCell: no such row",
    "Error '0:0' does not overlap",
    "warnings.warn(errors[info][0], RuntimeWarning)",
    "Leap second table TAI_UTC seems out-of-date",
    "Until the table is updated (see the CASA documentation or your system admin)",
    "times and coordinates derived from UTC could be wrong by 1s or more.",
    "no auto update is possible on this measurespath by this user",
    "measurespath must exist as a directory and it must be owned by the user",
    "The expected casa data was not found at measurespath",
    "The expected measures data was not found at measurespath",
    "visit https://casadocs.readthedocs.io",
    
    # QuartiCal/Numba warnings
    "NumbaPendingDeprecationWarning",
    "Code using Numba extension API maybe depending on 'old_style' error-capturing",
    "which is deprecated and will be replaced by 'new_style'",
    "See details at https://numba.readthedocs.io/en/latest/reference/deprecation.html",
    "Exception origin:",
    "if mode.literal_value == 4:",
    "NumbaWarning",
    "warnings.warn(errors.NumbaWarning",
    
    # Catboss/GPU errors - often non-fatal
    "'NoneType' object has no attribute 'synchronize'",
    "Error processing baseline",
    "CUDA error",
    "cudaErrorNoDevice",
    "cuBLAS error",
    "GPU memory allocation failed",
    
    # CuPy warnings
    "CuPy may not function correctly because multiple CuPy packages are installed",
    "cupy, cupy-cuda12x",
    "Follow these steps to resolve this issue:",
    "pip uninstall <package_name>",
    "conda uninstall cupy",
    "Install the appropriate CuPy package",
    "Refer to the Installation Guide for detailed instructions",
    "https://docs.cupy.dev/en/stable/install.html",
    
    # CASA warnings
    "WARN",
    "WARNING",
    "FutureWarning",
    "DeprecationWarning",
    "UserWarning",
    "RuntimeWarning",
    "PendingDeprecationWarning",
    
    # Common HPC/cluster warnings
    "module load",
    "module: command not found",
    "PBS: job killed: walltime",
    "slurmstepd: error: Exceeded job memory limit",
    "mpirun: command not found",
    "which: no mpirun in",
    
    # Python/package warnings
    "site-packages",
    "/usr/lib/python",
    "import warnings",
    "warnings.filterwarnings",
    "FutureWarning: Passing",
    
    # Foresight warnings
    "No sources found in catalog",
    "Empty source list",
    "Warning: Low S/N sources",
    
    # CrystalBall warnings  
    "Warning: Model component",
    "No MODEL_DATA column found",
    "Creating MODEL_DATA column",
    "NumbaDeprecationWarning: numba.generated_jit is deprecated",

    # General computation warnings
    "divide by zero encountered",
    "invalid value encountered",
    "overflow encountered",
    "underflow encountered",
    "NaN values detected",
    "Empty array passed",
    
    # Nimki warnings
    "nimki",
    "Nimki",
    

    # Concat 
    "Exception Reported: Table DataManager error: Internal error: StManIndArray::get/put shapes not conforming",
    "RuntimeWarning: Number of calls to function has reached maxfev = 1400.",
    "p, success = leastsq(errorfunction, p_ini)"

]


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        prog="charizard",
        description="CHARIZARD - Radio Interferometry Calibration Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  charizard setup env                       # pull, create, GPU if present\n"
            "  charizard setup env -i                    # ask for every option\n"
            "  charizard setup env --gpu                 # require a working GPU\n"
            "  charizard setup env --no-gpu              # CPU-only\n"
            "  charizard setup env --cuda-lib-path /usr/lib64\n"
            "  charizard setup env --gpu-only --gpu      # GPU step only, on a GPU node\n"
            "  charizard run pokedex.yaml\n"
            "  charizard run pokedex.yaml -s scheduler.yaml\n"
            "\n"
            "Clusters where no single node has both network and a GPU driver:\n"
            "  charizard setup env --no-gpu              # on the login node\n"
            "  charizard setup env --gpu-only --gpu      # inside a GPU job\n"
        )
    )

    subparsers = parser.add_subparsers(dest="command", metavar="<command>")
    subparsers.required = True

    # ── setup env ──────────────────────────────────────────────────────────
    setup_parser = subparsers.add_parser(
        "setup",
        help="Setup the compute container environment"
    )
    setup_sub = setup_parser.add_subparsers(dest="setup_target", metavar="<target>")
    setup_sub.required = True

    env_parser = setup_sub.add_parser(
        "env",
        help="Pull image, create container, configure GPU"
    )
    env_parser.add_argument(
        "--image",
        default=DEFAULT_IMAGE,
        help=f"Docker image to pull (default: {DEFAULT_IMAGE})"
    )
    env_parser.add_argument(
        "--name",
        default=DEFAULT_NAME,
        help=f"Container name for udocker (default: {DEFAULT_NAME})"
    )
    env_parser.add_argument(
        "--cuda-lib-path",
        default=None,
        metavar="PATH",
        help="Host dir holding the NVIDIA driver libs (auto-detected if omitted). "
             "Use it to override a bad guess, e.g. /usr/lib64"
    )
    env_parser.add_argument(
        "--udocker-dir",
        default=None,
        metavar="PATH",
        help="Sets $UDOCKER_DIR for the setup (default: $UDOCKER_DIR or ~/.udocker)"
    )

    # GPU is a tri-state, not a bool: "auto" must stay distinguishable from an
    # explicit --gpu so that a GPU that fails to verify can be a hard error only
    # when the user actually asked for one.
    gpu_group = env_parser.add_mutually_exclusive_group()
    gpu_group.add_argument(
        "--gpu",
        dest="gpu", action="store_const", const="on",
        help="Require GPU support; setup FAILS if the GPU does not verify"
    )
    gpu_group.add_argument(
        "--no-gpu",
        dest="gpu", action="store_const", const="off",
        help="Skip GPU setup entirely (CPU-only flagging)"
    )
    env_parser.set_defaults(gpu="auto")

    env_parser.add_argument(
        "--skip-pull",
        action="store_true",
        help="Reuse an already-pulled image (for nodes with no outbound network)"
    )
    env_parser.add_argument(
        "--gpu-only",
        action="store_true",
        help="Only redo the GPU step against an existing container. Use from a "
             "GPU node when the node that has network has no driver."
    )
    env_parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Prompt for each option instead of taking the defaults"
    )

    # ── run ────────────────────────────────────────────────────────────────
    run_parser = subparsers.add_parser(
        "run",
        help="Run the calibration pipeline"
    )
    run_parser.add_argument(
        "config",
        help="Pokedex config file (YAML)"
    )
    run_parser.add_argument(
        "--scheduler_config", "-s",
        help="Scheduler config file for housekeeper (YAML)"
    )
    run_parser.add_argument(
        "--models", "-m",
        help="User models file with custom polcal models (YAML)"
    )

    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()

    if args.command == "setup":
        # charizard setup env [--image ...] [--name ...] [--cuda-lib-path ...]
        ok = setup_container(
            image=args.image,
            name=args.name,
            cuda_lib_path=args.cuda_lib_path,
            gpu=args.gpu,
            skip_pull=args.skip_pull,
            gpu_only=args.gpu_only,
            udocker_dir=args.udocker_dir,
            interactive=args.interactive,
        )
        sys.exit(0 if ok else 1)

    # charizard run pokedex.yaml
    config = parse_config(args.config, models_path=args.models)

    # All job scripts and outputs are created relative to CWD, and the
    # container only mounts working_dir - so run from there regardless of
    # where the user launched charizard.
    os.makedirs(config.working_dir, exist_ok=True)
    os.chdir(config.working_dir)

    logger = PipelineLogger(config.working_dir)
    logger.banner("CHARIZARD PIPELINE")

    logger.info(f"Config: {args.config}")
    if args.scheduler_config:
        logger.info(f"Scheduler config: {args.scheduler_config}")
    if args.models:
        logger.info(f"User models: {args.models}")
    logger.info(f"MS: {config.ms_path}")
    logger.info(f"Working directory: {config.working_dir}")
    if config.container:
        logger.info(f"Container: {config.container.get('name', DEFAULT_NAME)}")

    try:
        success = charizard(config, logger, args.scheduler_config, whitelist=ERROR_WHITELIST)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        success = False
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        success = False

    logger.print_summary()
    logger.save()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
