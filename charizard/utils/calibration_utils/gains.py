# charizard/utils/calibration_utils/gains.py
"""
Calibration - calculate gains, bandpass, polcal.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper
from ..container import build_udocker_prefix


def build_calibration_script(spw: str,
                             cal_plan: Dict,
                             refant: str,
                             cal_round: int,
                             do_polcal: bool = False) -> str:
    """
    Build CASA calibration script.
    
    Args:
        spw: SPW directory
        cal_plan: Calibration plan dict
        refant: Reference antenna
        cal_round: Calibration round number
        do_polcal: Whether to do polarization calibration
    
    Returns:
        CASA script string
    """
    flux_cal = cal_plan.get('flux_cal', '')
    phase_cal = cal_plan.get('phase_cal', '')
    leakage_cal = cal_plan.get('leakage_cal')
    polangle_cal = cal_plan.get('polangle_cal')
    uvranges = cal_plan.get('calibrator_uvranges', {})
    polcal_models = cal_plan.get('polcal_models', {})
    
    # Build lists
    amp_cal_list = [c.strip() for c in flux_cal.split(',') if c.strip()] if flux_cal else []
    phase_cal_list = [c.strip() for c in phase_cal.split(',') if c.strip()] if phase_cal else []
    
    # All calibrators for amp/phase cal
    all_cal_list = list(set(amp_cal_list + phase_cal_list))
    if do_polcal:
        if leakage_cal and leakage_cal not in all_cal_list:
            all_cal_list.append(leakage_cal)
        if polangle_cal and polangle_cal not in all_cal_list:
            all_cal_list.append(polangle_cal)
    
    # Transfer sources for fluxscale
    transfer_list = [c for c in phase_cal_list if c not in amp_cal_list]
    if do_polcal:
        if leakage_cal and leakage_cal not in amp_cal_list and leakage_cal not in transfer_list:
            transfer_list.append(leakage_cal)
        if polangle_cal and polangle_cal not in amp_cal_list and polangle_cal not in transfer_list:
            transfer_list.append(polangle_cal)
    
    do_fluxscale = len(transfer_list) > 0
    
    script = "# CASA Calibration Script\n\n"
    
    # setjy for flux calibrators
    script += "# Set flux density model\n"
    for amp_cal in amp_cal_list:
        script += f"setjy(vis='{spw}/cal.ms', field='{amp_cal}')\n"
    
    script += "\n# Delay calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""
        
        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/delays.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    gaintype='K',
    solint='inf',
    combine='scan',
    minsnr=3{uvrange_param}{append_param})
"""
    
    script += "\n# Initial phase calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""
        
        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/phase_int.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    gaintype='G',
    calmode='p',
    solint='int',
    minsnr=3{uvrange_param}{append_param})
"""
    
    script += "\n# Bandpass calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""
        
        script += f"""bandpass(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/bandpass.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    solint='inf',
    combine='scan',
    solnorm=True,
    minsnr=3,
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/phase_int.cal{cal_round}']{uvrange_param}{append_param})
"""
    
    script += "\n# Amplitude and phase calibration\n"
    for i, cal_name in enumerate(all_cal_list):
        uvrange = uvranges.get(cal_name)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""
        
        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/amp_phase.cal{cal_round}',
    field='{cal_name}',
    refant='{refant}',
    gaintype='G',
    calmode='ap',
    solint='120s',
    minsnr=3,
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}']{uvrange_param}{append_param})
"""
    
    # Fluxscale
    if do_fluxscale:
        transfer_str = ','.join(transfer_list)
        script += f"""
# Flux calibration
fluxscale(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/amp_phase.cal{cal_round}',
    fluxtable='{spw}/caltables/flux.cal{cal_round}',
    reference='{flux_cal}',
    transfer='{transfer_str}')
"""
        flux_table = f"'{spw}/caltables/flux.cal{cal_round}'"
    else:
        flux_table = f"'{spw}/caltables/amp_phase.cal{cal_round}'"
    
    # Polarization calibration
    if do_polcal and polangle_cal and polangle_cal in polcal_models:
        polcal_data = polcal_models[polangle_cal]
        
        polang_uvrange = uvranges.get(polangle_cal)
        polang_uvrange_param = f", uvrange='{polang_uvrange}'" if polang_uvrange else ""
        
        leakage_uvrange = uvranges.get(leakage_cal) if leakage_cal else None
        leakage_uvrange_param = f", uvrange='{leakage_uvrange}'" if leakage_uvrange else ""
        
        # Get model parameters
        stokes_I = polcal_data.get('stokes_I', 1.0)
        spectral_index = polcal_data.get('spectral_index', [0.0])
        reffreq = polcal_data.get('reffreq', '1.0GHz')
        pol_frac = polcal_data.get('polarization_fraction', [0.0])
        pol_angle = polcal_data.get('polarization_angle', [0.0])
        
        script += f"""
# Set polarization calibrator model
setjy(vis='{spw}/cal.ms',
    field='{polangle_cal}',
    standard='manual',
    fluxdensity=[{stokes_I}, 0, 0, 0],
    spix={spectral_index},
    reffreq="{reffreq}",
    polindex={pol_frac},
    polangle={pol_angle})

# Cross-hand delay calibration
gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/delaycross.cal{cal_round}',
    field='{polangle_cal}',
    refant='{refant}',
    gaintype='KCROSS',
    solint='inf',
    calmode='ap',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table}]{polang_uvrange_param})

# Leakage calibration
polcal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/leakage.cal{cal_round}',
    field='{leakage_cal}',
    refant='{refant}',
    poltype='Df',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table},
               '{spw}/caltables/delaycross.cal{cal_round}']{leakage_uvrange_param})

# Polarization angle calibration
polcal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/polangle.cal{cal_round}',
    field='{polangle_cal}',
    refant='{refant}',
    poltype='Xf',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table},
               '{spw}/caltables/delaycross.cal{cal_round}',
               '{spw}/caltables/leakage.cal{cal_round}']{polang_uvrange_param})
"""
    
    script += "\nprint('Calibration complete')\n"
    
    return script


def run_calibration(hk: Housekeeper,
                    config,
                    active_spws: List[str],
                    cal_plan: Dict,
                    refant: str,
                    cal_round: int,
                    logger,
                    whitelist: List[str],
                    wait: bool = True) -> Optional[List[str]]:
    """
    Run calibration.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        cal_plan: Calibration plan
        refant: Reference antenna
        cal_round: Calibration round number
        logger: Logger
        whitelist: Error whitelist
        wait: Whether to wait for completion
    
    Returns:
        List of successful SPWs or job_ids (if wait=False)
    """
    logger.substep(f"Running calibration round {cal_round}...")
    
    env = config.environment
    casa_path = env.get('casa_path', '')
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('crosscal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    # Check if doing polcal
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    calibration = init_cal.get('calibration', {})
    pol_config = calibration.get('pol', {})
    
    do_polcal = (cal_plan.get('leakage_cal') and 
                 cal_plan.get('polangle_cal') and 
                 cal_plan.get('polangle_cal') in cal_plan.get('polcal_models', {}))
    
    if do_polcal:
        logger.info("Polarization calibration enabled")
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        script = build_calibration_script(
            spw=spw,
            cal_plan=cal_plan,
            refant=refant,
            cal_round=cal_round,
            do_polcal=do_polcal
        )
        
        script_file = f"calibrate_{cal_round}_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"cal_r{cal_round}_{spw}",
            job_subdir=spw,
            ppn=ppn,
            walltime=resources.get('walltime', '12:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted calibration for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No calibration jobs submitted")
        return None
    
    if not wait:
        return job_ids
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} calibration jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    successful = []
    failed = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if log_result.success:
            # Verify caltables exist
            required = ['delays.cal', 'bandpass.cal', 'amp_phase.cal']
            missing = [t for t in required if not os.path.exists(f"{spw}/caltables/{t}{cal_round}")]
            
            if missing:
                failed.append(spw)
                logger.error(f"{spw}: Missing caltables: {missing}")
            else:
                successful.append(spw)
                logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.error(f"{spw}: FAILED")
    
    return successful if successful else None
