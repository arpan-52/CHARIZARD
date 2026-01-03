#!/usr/bin/env python3
# charizard/main.py
"""
CHARIZARD - Radio Interferometry Calibration Pipeline
Entry point - parse args, define whitelist, call charizard()
"""

import argparse
import sys

from .charizard import charizard
from .utils.general.config_parser import parse_config
from .utils.general.logging import PipelineLogger


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
    "NumbaDeprecationWarning: numba.generated_jit is deprecated"
    
    # General computation warnings
    "divide by zero encountered",
    "invalid value encountered",
    "overflow encountered",
    "underflow encountered",
    "NaN values detected",
    "Empty array passed",
    
    # Nami warnings
    "nami",
    "Nami",
    

    # Concat 
    "Exception Reported: Table DataManager error: Internal error: StManIndArray::get/put shapes not conforming",
    "RuntimeWarning: Number of calls to function has reached maxfev = 1400.",
    "p, success = leastsq(errorfunction, p_ini)"

]


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description="CHARIZARD - Radio Interferometry Calibration Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "config",
        help="Pokedex config file (YAML)"
    )
    
    parser.add_argument(
        "--scheduler_config", "-s",
        help="Scheduler config file for housekeeper (YAML)"
    )
    
    parser.add_argument(
        "--models", "-m",
        help="User models file with custom polcal models (YAML)"
    )
    
    return parser.parse_args()


def main():
    """Main entry point"""
    args = parse_args()
    
    # Parse config (with optional user models)
    config = parse_config(args.config, models_path=args.models)
    
    # Setup logger
    logger = PipelineLogger(config.working_dir)
    logger.banner("CHARIZARD PIPELINE")
    
    logger.info(f"Config: {args.config}")
    if args.scheduler_config:
        logger.info(f"Scheduler config: {args.scheduler_config}")
    if args.models:
        logger.info(f"User models: {args.models}")
    logger.info(f"MS: {config.ms_path}")
    logger.info(f"Working directory: {config.working_dir}")
    
    # Scheduler config
    scheduler_config = args.scheduler_config
    
    # Run pipeline
    try:
        success = charizard(config, logger, scheduler_config, whitelist=ERROR_WHITELIST)
    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        success = False
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        success = False
    
    # Summary and save
    logger.print_summary()
    logger.save()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
