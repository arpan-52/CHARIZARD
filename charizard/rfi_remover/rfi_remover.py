# charizard/rfi_remover/rfi_remover.py
"""
RFI Remover - Main orchestrator for flagging
"""

import os
import time
from typing import Dict, List, Optional
from housekeeper import Housekeeper

from .antenna_analysis import analyze_antennas, write_flag_commands, add_user_bad_antennas
from .flaggers import submit_initial_flag_jobs, submit_tfcrop_jobs, submit_rflag_jobs
from ..utils.ms_utils import remove_lock


def wait_for_jobs(hk: Housekeeper, job_ids: List[str], logger, step_name: str,
                  brotherhood: bool = True) -> bool:
    """Wait for jobs and check success"""
    if not job_ids:
        return True
    
    logger.substep(f"Waiting for {len(job_ids)} {step_name} jobs...")
    
    results = hk.wait(job_ids)
    
    failed = [jid for jid, job in results.items() if job.state.value == 'failed']
    
    if failed:
        logger.error(f"{len(failed)}/{len(job_ids)} {step_name} jobs failed")
        if brotherhood:
            return False
    
    logger.success(f"All {step_name} jobs completed")
    return True


def run_rfi_removal(hk: Housekeeper, config: Dict, cal_plan: Dict,
                    active_spws: List[str], logger) -> Dict:
    """
    Run full RFI removal:
    1. Analyze antennas (find bad + find best refant)
    2. Write flag commands
    3. Apply initial flags
    4. TFCrop on calibrators
    
    Args:
        hk: Housekeeper instance
        config: Pipeline config
        cal_plan: Calibration plan
        active_spws: List of active SPWs
        logger: PipelineLogger
    
    Returns:
        dict with success status and refant
    """
    logger.step("RFI REMOVAL & ANTENNA ANALYSIS")
    
    flow_config = config.get('flow', {}).get('initial_calibration_flagging', {})
    flag_config = flow_config.get('flagging', {})
    cal_config = flow_config.get('calibration', {})
    brotherhood = flow_config.get('setup', {}).get('brotherhood', True)
    
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    
    # Check if user provided refant
    user_refant = cal_config.get('refant')
    found_refant = None
    
    # ----- ANTENNA ANALYSIS -----
    if flag_config.get('bad_antennas', {}).get('auto', True):
        logger.substep("Analyzing antennas (bad detection + refant selection)...")
        
        # Analyze first SPW's cal.ms
        first_spw = active_spws[0]
        ms_path = f"{first_spw}/cal.ms"
        
        analysis = analyze_antennas(ms_path, n_processes=4, logger=logger)
        
        bad_antennas = analysis['bad_antennas']
        found_refant = analysis['refant']
        refant_candidates = analysis['refant_candidates']
        
        # Report
        if bad_antennas:
            logger.warning(f"Bad antennas: {', '.join(bad_antennas)}")
        
        logger.info(f"Best reference antenna: {found_refant}")
        if refant_candidates:
            logger.info(f"Refant candidates: {', '.join(refant_candidates)}")
        
        # Write flag files for each SPW
        user_bad = flag_config.get('bad_antennas', {}).get('list', [])
        all_bad = list(set(bad_antennas + user_bad))
        
        for spw in active_spws:
            flag_file = f"{spw}/flags.txt"
            write_flag_commands(
                flag_file,
                mode='w',
                bad_antennas=all_bad,
                flags_to_include=['shadow', 'autocorr', 'clip', 'quack', 'badant'],
                quack_interval=10.0
            )
    else:
        # Just write basic flags
        for spw in active_spws:
            flag_file = f"{spw}/flags.txt"
            user_bad = flag_config.get('bad_antennas', {}).get('list', [])
            write_flag_commands(
                flag_file,
                mode='w',
                bad_antennas=user_bad if user_bad else None,
                flags_to_include=['shadow', 'autocorr', 'clip', 'quack'],
                quack_interval=10.0
            )
            if user_bad:
                add_user_bad_antennas(flag_file, user_bad)
    
    # Determine final refant
    if user_refant:
        final_refant = user_refant
        logger.info(f"Using user-specified refant: {final_refant}")
    elif found_refant:
        final_refant = found_refant
        logger.info(f"Using auto-detected refant: {final_refant}")
    else:
        final_refant = None
        logger.warning("No reference antenna found!")
    
    # ----- APPLY INITIAL FLAGS -----
    logger.substep("Applying initial flags to cal.ms...")
    
    job_ids = submit_initial_flag_jobs(
        hk, config, ['cal.ms'], 'flags.txt', active_spws,
        casa_path, preamble
    )
    
    success = wait_for_jobs(hk, job_ids, logger, "initial flagging", brotherhood)
    if not success:
        return {'success': False, 'error': 'Initial flagging failed'}
    
    # ----- TFCROP ON CALIBRATORS -----
    if flag_config.get('rfi', True):
        logger.substep("Running TFCrop on calibrators...")
        
        job_ids = submit_tfcrop_jobs(
            hk, config, ['cal.ms'], active_spws,
            datacolumn='DATA', t_sigma=4.0, f_sigma=4.0,
            casa_path=casa_path, preamble=preamble, prefix='cal'
        )
        
        success = wait_for_jobs(hk, job_ids, logger, "tfcrop", brotherhood)
        if not success:
            return {'success': False, 'error': 'TFCrop failed'}
    
    # Remove locks
    for spw in active_spws:
        remove_lock(f"{spw}/cal.ms")
    
    logger.success("RFI removal completed")
    
    return {
        'success': True,
        'refant': final_refant,
        'bad_antennas': bad_antennas if 'bad_antennas' in dir() else []
    }
