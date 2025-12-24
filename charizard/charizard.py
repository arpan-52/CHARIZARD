# charizard/charizard.py
"""
CHARIZARD - The Orchestrator

Calls utility functions in order, handles flow control.
Each util function does: write scripts, submit jobs, wait, return results.
This file just orchestrates and handles brotherhood logic.
"""

import os
from typing import List, Optional

from housekeeper import Housekeeper

from .utils.general.ms_utils import get_ms_info
from .utils.general.source_utils import build_calibration_plan
from .utils.general.tracker import JobTracker
from .utils.splitting_utils.splitter import run_split


def charizard(config, logger, scheduler_config: Optional[str] = None, 
              whitelist: List[str] = None) -> bool:
    """
    Run the pipeline.
    
    Args:
        config: PipelineConfig from config_parser
        logger: PipelineLogger
        scheduler_config: Path to scheduler config for housekeeper
        whitelist: Error patterns to ignore in log checking
    
    Returns:
        True if successful
    """
    whitelist = whitelist or []
    
    # Setup housekeeper
    hk = Housekeeper(
        config=scheduler_config,
        jobs_dir=os.path.join(config.working_dir, "jobs"),
        scheduler=config.environment['scheduler']
    )
    
    # Get flow config
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    setup = init_cal.get('setup', {})
    # flagging = init_cal.get('flagging', {})
    # calibration = init_cal.get('calibration', {})
    
    brotherhood = setup.get('brotherhood', True)
    
    # =========================================================================
    # ANALYZE MS
    # =========================================================================
    logger.step("Analyzing MS")
    
    ms_info = get_ms_info(config.ms_path)
    
    logger.info(f"Fields: {len(ms_info['fields'])}")
    for fid, fdata in ms_info['fields'].items():
        logger.info(f"  [{fid}] {fdata['name']}")
    logger.info(f"SPWs: {ms_info['num_spws']}, Channels/SPW: {ms_info['num_channels']}")
    logger.info(f"Antennas: {ms_info['num_antennas']}")
    logger.info(f"Central freq: {ms_info['central_freq_hz']/1e9:.3f} GHz")
    
    # Build calibration plan (auto-detect + user overrides)
    cal_plan = build_calibration_plan(ms_info, config, logger)
    
    logger.info(f"Flux cal: {cal_plan['flux_cal']}")
    logger.info(f"Phase cal: {cal_plan['phase_cal']}")
    if cal_plan.get('leakage_cal'):
        logger.info(f"Leakage cal: {cal_plan['leakage_cal']}")
    if cal_plan.get('polangle_cal'):
        logger.info(f"Pol angle cal: {cal_plan['polangle_cal']}")
    logger.info(f"Targets: {', '.join(cal_plan['targets']) if cal_plan['targets'] else 'None'}")
    
    # Initialize tracker
    tracker = JobTracker(config.target_spws, logger)
    
    # =========================================================================
    # SPLIT
    # =========================================================================
    if setup.get('make_structure', True):
        logger.step("SPLITTING")
        
        success = run_split(
            hk=hk,
            config=config,
            ms_info=ms_info,
            cal_plan=cal_plan,
            tracker=tracker,
            logger=logger,
            whitelist=whitelist
        )
        
        if not success:
            if brotherhood:
                logger.error("Split failed - brotherhood enabled, stopping pipeline")
                return False
            else:
                logger.warning("Split had failures, continuing with remaining SPWs")
        
        if not tracker.get_active_spws():
            logger.error("No active SPWs remaining!")
            return False
        
        logger.success(f"Split complete. Active SPWs: {tracker.get_active_spws()}")
    
    # =========================================================================
    # FUTURE STEPS (not implemented yet)
    # =========================================================================
    
    # if flagging.get('bad_antennas', {}).get('auto', True):
    #     run_antenna_analysis(...)
    
    # if setup.get('initialize', True):
    #     run_initial_flagging(...)
    
    # if flagging.get('rfi', True):
    #     run_rfi_flagging(...)
    
    # run_calibration(...)
    
    # if calibration.get('apply', {}).get('calibrators', True):
    #     run_applycal(..., 'calibrators')
    
    # if calibration.get('apply', {}).get('targets', True):
    #     run_applycal(..., 'targets')
    
    # =========================================================================
    # DONE (for now - only split implemented)
    # =========================================================================
    
    logger.success("Pipeline completed (split stage)!")
    return True
