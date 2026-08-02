# charizard/utils/flagging_utils/initial_flagger.py
"""
Initial flagging - apply flag command files via CASA.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper
from ..general.jobs import wait_and_check
from ..general.resources import submit_resources
from ..container import build_udocker_prefix


def run_initial_flagging(hk: Housekeeper,
                         config,
                         active_spws: List[str],
                         ms_names: List[str],
                         flag_file: str,
                         logger,
                         whitelist: List[str],
                         prefix: str = '') -> Optional[List[str]]:
    """
    Apply flag commands from file to MS.
    
    Uses CASA flagdata(mode='list', inpfile=flag_file).
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        ms_names: List of MS names to flag (e.g., ['cal.ms'] or ['src.ms'])
        flag_file: Flag file name (e.g., 'badants.txt')
        logger: Logger
        whitelist: Error whitelist
        prefix: Job name prefix
    
    Returns:
        List of successful SPWs or None
    """
    logger.substep(f"Applying {flag_file} to {ms_names}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('flagging', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        casa_script = ""
        for ms in ms_names:
            ms_path = f"{spw}/{ms}"
            flag_path = f"{spw}/{flag_file}"
            
            if not os.path.exists(flag_path):
                logger.warning(f"Flag file not found: {flag_path}")
                continue
            
            casa_script += f"""
# Apply flags from {flag_file}
flagdata(
    vis='{ms_path}',
    mode='list',
    inpfile='{flag_path}'
)
print("Applied {flag_file} to {ms_path}")
"""
        
        if not casa_script:
            continue
        
        job_name = f"flag_init_{prefix}_{spw}" if prefix else f"flag_init_{spw}"
        script_file = f"{job_name}.py"
        
        with open(script_file, 'w') as f:
            f.write(casa_script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=job_name,
            job_subdir=spw,
            **submit_resources(resources, '04:00:00', ppn=ppn)
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted {job_name}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.warning("No initial flagging jobs submitted")
        return active_spws
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} initial flagging jobs...")
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
    
    if not successful:
        return None
    
    return successful
