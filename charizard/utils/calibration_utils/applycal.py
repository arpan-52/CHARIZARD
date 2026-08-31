# charizard/utils/calibration_utils/applycal.py
"""
Apply calibration solutions.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper
from ..general.jobs import wait_and_check
from ..general.resources import submit_resources
from ..container import build_udocker_prefix
from .gains import get_fluxscale_lists


def build_applycal_script(spw: str,
                          cal_plan: Dict,
                          cal_round: int,
                          target_type: str,
                          do_polcal: bool = False,
                          do_fluxscale: bool = True) -> str:
    """
    Build CASA applycal script.

    Handles both circular and linear feeds:
    - Circular: delays, bandpass, gain, delaycross, leakage, polangle
    - Linear: bandpass, leakage, gain, delaycross, polangle

    Args:
        spw: SPW directory
        cal_plan: Calibration plan
        cal_round: Calibration round number
        target_type: 'calibrators' or 'targets'
        do_polcal: Whether polcal was done
        do_fluxscale: Whether fluxscale was done

    Returns:
        CASA script string
    """
    flux_cal = cal_plan.get('flux_cal', '')
    phase_cal = cal_plan.get('phase_cal', '')
    targets = cal_plan.get('targets', [])
    all_calibrators = cal_plan.get('all_calibrators', [])
    pol_basis = cal_plan.get('pol_basis', 'circular')

    # Build gaintable list based on feed type
    if pol_basis == 'linear':
        # Linear feeds: delays, bandpass, leakage (if polcal), gain
        gaintables = [
            f"{spw}/caltables/delays.cal{cal_round}",
            f"{spw}/caltables/bandpass.cal{cal_round}",
        ]

        if do_polcal and os.path.exists(f"{spw}/caltables/leakage.cal{cal_round}"):
            gaintables.append(f"{spw}/caltables/leakage.cal{cal_round}")

        if do_fluxscale and os.path.exists(f"{spw}/caltables/flux.cal{cal_round}"):
            gaintables.append(f"{spw}/caltables/flux.cal{cal_round}")
        else:
            gaintables.append(f"{spw}/caltables/amp_phase.cal{cal_round}")

        if do_polcal:
            for pt in (f"{spw}/caltables/delaycross.cal{cal_round}",
                       f"{spw}/caltables/polangle.cal{cal_round}"):
                if os.path.exists(pt):
                    gaintables.append(pt)
    else:
        # Circular feeds: delays, bandpass, gain, polcal tables
        gaintables = [
            f"{spw}/caltables/delays.cal{cal_round}",
            f"{spw}/caltables/bandpass.cal{cal_round}",
        ]

        if do_fluxscale and os.path.exists(f"{spw}/caltables/flux.cal{cal_round}"):
            gaintables.append(f"{spw}/caltables/flux.cal{cal_round}")
        else:
            gaintables.append(f"{spw}/caltables/amp_phase.cal{cal_round}")

        if do_polcal:
            pol_tables = [
                f"{spw}/caltables/delaycross.cal{cal_round}",
                f"{spw}/caltables/leakage.cal{cal_round}",
                f"{spw}/caltables/polangle.cal{cal_round}",
            ]
            for pt in pol_tables:
                if os.path.exists(pt):
                    gaintables.append(pt)
    
    gaintables_str = "['" + "','".join(gaintables) + "']"
    num_tables = len(gaintables)
    
    if target_type == 'calibrators':
        field_list = ','.join(all_calibrators)
        ms_path = f"{spw}/cal.ms"
        interp = "['nearest'] * " + str(num_tables)
    else:
        field_list = ','.join(targets) if targets else ''
        ms_path = f"{spw}/src.ms"
        interp = "['linear'] * " + str(num_tables)
    
    # For linear feeds the parallactic angle correction is a real X/Y rotation,
    # so it needs cross-hands to be meaningful. Without polcal the MS holds only
    # XX,YY and the rotation is undefined - match the solve and leave it off.
    parang = 'True' if (pol_basis != 'linear' or do_polcal) else 'False'

    script = f"""# Apply calibration to {target_type}
applycal(vis='{ms_path}',
    field='{field_list}',
    gaintable={gaintables_str},
    gainfield=['nearest'] * {num_tables},
    interp={interp},
    parang={parang},
    calwt=True,
    flagbackup=True)

print('Applycal complete for {target_type}')
"""
    
    return script


def run_applycal(hk: Housekeeper,
                 config,
                 active_spws: List[str],
                 cal_plan: Dict,
                 cal_round: int,
                 target_type: str,
                 logger,
                 whitelist: List[str],
                 wait: bool = True) -> Optional[List[str]]:
    """
    Apply calibration solutions.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        cal_plan: Calibration plan
        cal_round: Calibration round number
        target_type: 'calibrators' or 'targets'
        logger: Logger
        whitelist: Error whitelist
        wait: Whether to wait for completion
    
    Returns:
        List of successful SPWs or job_ids (if wait=False)
    """
    logger.substep(f"Applying calibration to {target_type}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('crosscal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    # Check polcal
    do_polcal = (cal_plan.get('leakage_cal') and 
                 cal_plan.get('polangle_cal') and 
                 cal_plan.get('polangle_cal') in cal_plan.get('polcal_models', {}))
    
    # Check fluxscale - same decision as the solve in gains.py
    _, _, transfer_list = get_fluxscale_lists(cal_plan, do_polcal)
    do_fluxscale = len(transfer_list) > 0
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # Check if target MS exists
        if target_type == 'targets':
            if not os.path.exists(f"{spw}/src.ms"):
                logger.warning(f"{spw}/src.ms not found, skipping")
                continue
        
        script = build_applycal_script(
            spw=spw,
            cal_plan=cal_plan,
            cal_round=cal_round,
            target_type=target_type,
            do_polcal=do_polcal,
            do_fluxscale=do_fluxscale
        )
        
        script_file = f"applycal_{target_type}_{cal_round}_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"apply_{target_type[0]}_{cal_round}_{spw}",
            job_subdir=spw,
            **submit_resources(resources, walltime='02:00:00', ppn=ppn)
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted applycal {target_type} for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.warning(f"No applycal jobs submitted for {target_type}")
        return active_spws if wait else []
    
    if not wait:
        return job_ids
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} applycal jobs...")
    results = wait_and_check(hk, job_ids, whitelist=whitelist, logger=logger)
    
    successful = []
    failed = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if log_result.success:
            successful.append(spw)
            logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.error(f"{spw}: FAILED")
    
    return successful if successful else None
