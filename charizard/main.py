#!/usr/bin/env python3
"""
Charizard - Radio Interferometry Calibration Pipeline

Usage:
    charizard --config pokedex.yaml --scheduler_config scheduler.yaml
"""

import argparse
import os
import sys
import yaml
from datetime import datetime

from housekeeper import Housekeeper

from charizard.stager import run_stager
from charizard.rfi_remover import run_rfi_removal
from charizard.calibration import run_calibration
from charizard.selfcal import run_selfcal
from charizard.utils.logging import PipelineLogger


def load_config(config_path):
    """Load pipeline configuration from YAML"""
    if not os.path.exists(config_path):
        print(f"Error: Config file not found: {config_path}")
        sys.exit(1)
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def main():
    parser = argparse.ArgumentParser(
        description='Charizard - Radio Interferometry Calibration Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    charizard --config pokedex.yaml --scheduler_config scheduler.yaml
    charizard --config pokedex.yaml --scheduler_config scheduler.yaml --start-from calibration
        """
    )
    parser.add_argument('--config', '-c', required=True, 
                        help='Path to pokedex.yaml pipeline config')
    parser.add_argument('--scheduler_config', '-s', required=True,
                        help='Path to scheduler_config.yaml for housekeeper')
    parser.add_argument('--models', '-m', default=None,
                        help='Path to user models.yaml (optional)')
    parser.add_argument('--start-from', choices=['stager', 'flagging', 'calibration', 'selfcal'],
                        default='stager', help='Start from specific stage')
    parser.add_argument('--stop-after', choices=['stager', 'flagging', 'calibration', 'selfcal'],
                        default='selfcal', help='Stop after specific stage')
    parser.add_argument('--jobs-dir', default=None,
                        help='Directory for job files (default: <ms_name>_processing)')
    
    args = parser.parse_args()

    # Load configs
    config = load_config(args.config)
    
    # Determine jobs directory
    ms_name = config['data']['ms']
    ms_basename = os.path.basename(ms_name).replace('.ms', '')
    jobs_dir = args.jobs_dir or f"./{ms_basename}_jobs"
    
    # Initialize housekeeper with scheduler config
    hk = Housekeeper(config=args.scheduler_config, jobs_dir=jobs_dir)
    
    # Initialize logger
    logger = PipelineLogger(jobs_dir)
    
    logger.banner("CHARIZARD PIPELINE")
    logger.info(f"Pipeline config: {args.config}")
    logger.info(f"Scheduler config: {args.scheduler_config}")
    logger.info(f"MS: {ms_name}")
    logger.info(f"Jobs directory: {jobs_dir}")
    logger.info(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    stages = ['stager', 'flagging', 'calibration', 'selfcal']
    start_idx = stages.index(args.start_from)
    stop_idx = stages.index(args.stop_after)

    cal_plan = None
    active_spws = None

    try:
        # ===== STAGER =====
        if start_idx <= 0 <= stop_idx:
            result = run_stager(hk, config, logger, user_models_path=args.models)
            if not result['success']:
                logger.error(f"Stager failed: {result.get('error', 'Unknown')}")
                logger.save()
                sys.exit(1)
            cal_plan = result['cal_plan']
            active_spws = result['active_spws']

        # ===== FLAGGING =====
        if start_idx <= 1 <= stop_idx:
            if cal_plan is None:
                logger.error("No calibration plan - run stager first")
                sys.exit(1)
            result = run_rfi_removal(hk, config, cal_plan, active_spws, logger)
            if not result['success']:
                logger.error(f"Flagging failed: {result.get('error', 'Unknown')}")
                logger.save()
                sys.exit(1)
            # Update cal_plan with refant if found
            if 'refant' in result:
                cal_plan['refant'] = result['refant']

        # ===== CALIBRATION =====
        if start_idx <= 2 <= stop_idx:
            if cal_plan is None:
                logger.error("No calibration plan - run stager first")
                sys.exit(1)
            result = run_calibration(hk, config, cal_plan, active_spws, logger)
            if not result['success']:
                logger.error(f"Calibration failed: {result.get('error', 'Unknown')}")
                logger.save()
                sys.exit(1)

        # ===== SELFCAL =====
        if start_idx <= 3 <= stop_idx:
            if cal_plan is None:
                logger.error("No calibration plan - run stager first")
                sys.exit(1)
            result = run_selfcal(hk, config, cal_plan, active_spws, logger)
            if not result['success']:
                logger.error(f"Selfcal failed: {result.get('error', 'Unknown')}")
                logger.save()
                sys.exit(1)

        # ===== DONE =====
        logger.print_summary()
        logger.save()

        logger.banner("PIPELINE COMPLETED SUCCESSFULLY", style="success")

    except KeyboardInterrupt:
        logger.warning("Pipeline interrupted by user")
        hk.cancel_all()
        logger.save()
        sys.exit(130)

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        import traceback
        traceback.print_exc()
        logger.save()
        sys.exit(1)


if __name__ == '__main__':
    main()
