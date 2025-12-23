# charizard/stager/stager.py
"""
Stager - MS analysis, calibration planning, and splitting
"""

import os
import time
from typing import Dict, List, Optional
from housekeeper import Housekeeper

from ..utils.ms_utils import get_ms_info, get_active_spws
from ..utils.source_utils import CalibratorMatcher, categorize_fields


def setup_directories(num_spw: int, logger=None):
    """Create SPW directories"""
    for i in range(num_spw):
        spw_dir = f'spw{i}'
        os.makedirs(spw_dir, exist_ok=True)
        os.makedirs(f'{spw_dir}/caltables', exist_ok=True)
        os.makedirs(f'{spw_dir}/plots', exist_ok=True)
        if logger:
            logger.substep(f"Created {spw_dir}/")


def build_calibration_plan(ms_info: Dict, field_classifications: Dict,
                           field_categories: Dict, config: Dict, logger=None) -> Dict:
    """
    Build calibration plan from MS analysis.
    
    Includes:
    - Calibrator assignments (flux, phase, leakage, polangle)
    - UV ranges per calibrator
    - Polcal models
    - Flags for what calibration steps to do
    """
    
    cal_config = config.get('flow', {}).get('initial_calibration_flagging', {}).get('calibration', {})
    pol_config = cal_config.get('pol', {})
    
    plan = {
        # MS info
        'num_corrs': ms_info['num_corrs'],
        'pol_basis': ms_info['pol_basis'],
        'num_spws': ms_info['num_spws'],
        'num_channels': ms_info['num_channels'],
        'wavelength_cm': ms_info['wavelength_cm'],
        'central_freq_hz': ms_info['central_freq_hz'],
        
        # Calibrators
        'flux_cal': field_categories['flux_cal'],
        'phase_cal': field_categories['phase_cal'],
        'leakage_cal': field_categories['leakage_cal'],
        'polangle_cal': field_categories['polangle_cal'],
        'targets': field_categories['targets'],
        'all_calibrators': field_categories['all_calibrators'],
        
        # UV ranges per calibrator (from XML based on MS frequency)
        'calibrator_uvranges': field_categories.get('calibrator_uvranges', {}),
        
        # Calibration flags
        'do_fluxscale': False,
        'do_leakage': False,
        'do_polangle': False,
        
        # Polcal models
        'leakage_model': None,
        'polangle_model': None,
        
        # Reference antenna (may be None, set by antenna analysis)
        'refant': cal_config.get('refant'),
        
        # Warnings and errors
        'warnings': [],
        'errors': []
    }
    
    # ----- FLUXSCALE -----
    # Only if phase_cal exists and is different from flux_cal
    if plan['phase_cal'] and plan['phase_cal'] != plan['flux_cal']:
        plan['do_fluxscale'] = True
        if logger:
            logger.info(f"Will do fluxscale: {plan['flux_cal']} → {plan['phase_cal']}")
    
    # ----- POLCAL -----
    if ms_info['num_corrs'] == 4:
        # Leakage calibration
        if pol_config.get('leakage', {}).get('enabled', False):
            if plan['leakage_cal']:
                plan['do_leakage'] = True
                plan['leakage_mode'] = pol_config.get('leakage', {}).get('mode', 'Df')
                
                # Check if leakage cal has a model
                leakage_info = field_classifications.get(plan['leakage_cal'], {})
                if leakage_info.get('polcal_model'):
                    plan['leakage_model'] = leakage_info['polcal_model']
                    if logger:
                        logger.info(f"Leakage cal {plan['leakage_cal']}: using polcal model")
                elif leakage_info.get('is_known_unpolarized'):
                    if logger:
                        logger.info(f"Leakage cal {plan['leakage_cal']}: known unpolarized")
                else:
                    plan['warnings'].append(f"Leakage cal {plan['leakage_cal']}: assuming unpolarized (no model)")
                    if logger:
                        logger.warning(f"Leakage cal {plan['leakage_cal']}: ASSUMING UNPOLARIZED")
            else:
                plan['warnings'].append("Leakage enabled but no leakage calibrator specified")
                if logger:
                    logger.warning("Leakage enabled but no leakage calibrator specified")
        
        # Polarization angle calibration
        if pol_config.get('angle', {}).get('enabled', False):
            if plan['polangle_cal']:
                polangle_info = field_classifications.get(plan['polangle_cal'], {})
                if polangle_info.get('polcal_model'):
                    plan['do_polangle'] = True
                    plan['polangle_model'] = polangle_info['polcal_model']
                    if logger:
                        logger.info(f"Pol angle cal {plan['polangle_cal']}: using polcal model")
                else:
                    plan['errors'].append(f"Pol angle cal {plan['polangle_cal']}: NO MODEL - cannot calibrate")
                    if logger:
                        logger.error(f"Pol angle cal {plan['polangle_cal']}: NO POLCAL MODEL")
            else:
                plan['warnings'].append("Pol angle enabled but no pol angle calibrator specified")
                if logger:
                    logger.warning("Pol angle enabled but no pol angle calibrator specified")
    
    # ----- VALIDATION -----
    if not plan['flux_cal']:
        plan['errors'].append("No flux calibrator specified or detected")
    
    # Log UV ranges
    if plan['calibrator_uvranges'] and logger:
        logger.info("UV ranges:")
        for cal, uvrange in plan['calibrator_uvranges'].items():
            logger.info(f"  {cal}: {uvrange}")
    
    return plan


def submit_split_jobs(hk: Housekeeper, config: Dict, cal_plan: Dict,
                      target_spws: int, logger) -> List[str]:
    """Submit MS splitting jobs"""
    ms_path = config['data']['ms']
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    
    calibrators = ','.join(cal_plan['all_calibrators'])
    targets = ','.join(cal_plan['targets']) if cal_plan['targets'] else ''
    
    job_ids = []
    
    for i in range(target_spws):
        spw_name = f'spw{i}'
        
        # Build split script
        casa_script = f"""
# Split calibrators
mstransform(vis='{ms_path}', outputvis='{spw_name}/cal.ms',
            field='{calibrators}', spw='{i}', datacolumn='data')
"""
        
        # Only split targets if there are any
        if targets:
            casa_script += f"""
# Split targets  
mstransform(vis='{ms_path}', outputvis='{spw_name}/src.ms',
            field='{targets}', spw='{i}', datacolumn='data')
"""
        
        script_file = f"split_{spw_name}.py"
        with open(script_file, 'w') as f:
            f.write(casa_script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"split_{spw_name}",
            job_subdir=spw_name,
            ppn=4,
            walltime="02:00:00"
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def run_stager(hk: Housekeeper, config: Dict, logger,
               user_models_path: Optional[str] = None) -> Dict:
    """
    Run stager stage:
    1. Analyze MS
    2. Classify fields by position
    3. Build calibration plan (with UV ranges)
    4. Create directories
    5. Split MS
    """
    logger.step("STAGER: MS Analysis & Splitting")
    
    ms_path = config['data']['ms']
    target_spws = config['data'].get('processing_spw', 1)
    
    # Check MS exists
    if not os.path.exists(ms_path):
        logger.error(f"MS not found: {ms_path}")
        return {'success': False, 'error': f"MS not found: {ms_path}"}
    
    # ----- ANALYZE MS -----
    logger.substep("Analyzing measurement set...")
    
    ms_info = get_ms_info(ms_path)
    
    logger.info(f"Fields: {len(ms_info['fields'])}")
    for fid, fdata in ms_info['fields'].items():
        logger.info(f"  [{fid}] {fdata['name']}")
    logger.info(f"SPWs: {ms_info['num_spws']}, Channels/SPW: {ms_info['num_channels']}")
    logger.info(f"Correlations: {ms_info['num_corrs']} ({', '.join(ms_info['corr_names'])})")
    logger.info(f"Central freq: {ms_info['central_freq_hz']/1e9:.3f} GHz ({ms_info['wavelength_cm']:.1f} cm)")
    
    # ----- CLASSIFY FIELDS BY POSITION -----
    logger.substep("Classifying fields by position (2 arcsec matching)...")
    
    matcher = CalibratorMatcher(user_models_path, logger)
    field_classifications = matcher.analyze_ms_fields(ms_info)
    field_categories = categorize_fields(field_classifications, config, logger)
    
    # ----- BUILD CALIBRATION PLAN -----
    logger.substep("Building calibration plan...")
    
    cal_plan = build_calibration_plan(ms_info, field_classifications, 
                                       field_categories, config, logger)
    
    # Summary
    logger.info(f"Flux cal: {cal_plan['flux_cal']}")
    logger.info(f"Phase cal: {cal_plan['phase_cal']}")
    logger.info(f"Leakage cal: {cal_plan['leakage_cal']}")
    logger.info(f"Pol angle cal: {cal_plan['polangle_cal']}")
    logger.info(f"Targets: {', '.join(cal_plan['targets']) if cal_plan['targets'] else 'None'}")
    logger.info(f"Do fluxscale: {cal_plan['do_fluxscale']}")
    logger.info(f"Do leakage: {cal_plan['do_leakage']}")
    logger.info(f"Do polangle: {cal_plan['do_polangle']}")
    
    # Check for errors
    if cal_plan.get('errors'):
        for err in cal_plan['errors']:
            logger.error(err)
        return {'success': False, 'cal_plan': cal_plan, 'error': 'Calibration plan has errors'}
    
    # Log warnings
    for warn in cal_plan.get('warnings', []):
        logger.warning(warn)
    
    # ----- CREATE DIRECTORIES -----
    logger.substep(f"Creating directory structure for {target_spws} SPWs...")
    setup_directories(target_spws, logger)
    
    # ----- SPLIT MS -----
    logger.substep("Submitting split jobs...")
    
    job_ids = submit_split_jobs(hk, config, cal_plan, target_spws, logger)
    
    if not job_ids:
        logger.error("No split jobs submitted")
        return {'success': False, 'error': 'No split jobs submitted'}
    
    logger.substep(f"Waiting for {len(job_ids)} split jobs...")
    results = hk.wait(job_ids)
    
    failed = [jid for jid, job in results.items() if job.state.value == 'failed']
    if failed:
        logger.error(f"{len(failed)} split jobs failed")
        return {'success': False, 'error': 'Split failed'}
    
    active_spws = get_active_spws()
    
    logger.success("Stager completed")
    
    return {
        'success': True,
        'cal_plan': cal_plan,
        'ms_info': ms_info,
        'active_spws': active_spws
    }