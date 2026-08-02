# charizard/utils/ddcal_utils/concat.py
"""
MS concatenation for DDCal.
Always uses parallel correlations only (RR,LL or XX,YY).
Output: ddcal_output/{field}/combined_{field}.ms
"""

import os
import time
from typing import List, Dict, Optional

from housekeeper import Housekeeper
from ..general.jobs import wait_and_check
from ..general.resources import submit_resources
from ..container import build_udocker_prefix


def concat_ms(hk: Housekeeper,
              config,
              ms_map: Dict[str, List[str]],
              logger,
              whitelist: List[str]) -> Optional[Dict[str, str]]:
    """
    Concatenate MS files from all SPWs for each field.
    
    Uses only parallel correlations (RR,LL or XX,YY).
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        ms_map: Dict mapping field -> list of MS paths from selfcal
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Dict mapping field -> combined MS path, or None on failure
    """
    logger.substep("Concatenating MS files for DDCal...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    # Create output directory
    os.makedirs('ddcal_output', exist_ok=True)
    
    job_ids = []
    job_map = {}  # job_id -> field
    
    for field, ms_list in ms_map.items():
        if not ms_list:
            logger.warning(f"No MS files for field {field}")
            continue
        
        # Create field directory
        field_dir = f"ddcal_output/{field}"
        os.makedirs(field_dir, exist_ok=True)
        os.makedirs(f"{field_dir}/logs", exist_ok=True)
        
        output_ms = f"{field_dir}/combined_{field}.ms"
        
        # Build vis_list for CASA
        vis_list_str = ','.join([f"'{ms}'" for ms in ms_list])
        
        # CASA script - concat with only parallel correlations
        script = f'''# Concatenate MS files for {field}
# Only parallel correlations (RR,LL or XX,YY)

import os

vis_list = [{vis_list_str}]
output_ms = '{output_ms}'

# First concat all
concat(vis=vis_list, concatvis=output_ms)

print(f"Combined MS: {{output_ms}}")
'''
        
        script_file = f"concat_{field}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"concat_{field}",
            job_subdir=field_dir,
            **submit_resources(resources, '02:00:00', ppn=ppn)
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = field
            logger.info(f"Submitted concat {field}: {job.job_id}")
        
        time.sleep(0.3)
    
    if not job_ids:
        logger.error("No concat jobs submitted")
        return None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} concat jobs...")
    results = wait_and_check(hk, job_ids, whitelist=whitelist, logger=logger)
    
    # Collect results
    output_map = {}
    
    for job_id, (job, log_result) in results.items():
        field = job_map.get(job_id, 'unknown')
        output_ms = f"ddcal_output/{field}/combined_{field}.ms"
        
        if log_result.success:
            output_map[field] = output_ms
            logger.info(f"{field}: OK -> {output_ms}")
        else:
            logger.error(f"{field}: concat FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    return output_map if output_map else None
