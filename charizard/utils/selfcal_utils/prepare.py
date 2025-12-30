# charizard/utils/selfcal_utils/prepare.py
"""
Prepare MS for self-calibration.
- Split per field
- Frequency average
- Initial flagging
"""

import os
import time
from typing import List, Dict, Optional, Tuple

from housekeeper import Housekeeper


def prepare_selfcal_ms(hk: Housekeeper,
                       config,
                       active_spws: List[str],
                       targets: List[str],
                       freqbin: int,
                       logger,
                       whitelist: List[str]) -> Optional[Dict[str, List[str]]]:
    """
    Prepare MS for self-calibration.
    
    For each SPW and field:
    1. Split src.ms to {spw}/{field}/sc.ms
    2. Frequency average by freqbin
    3. Apply initial flagging (catboss)
    
    All SPW/field combinations run in parallel.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        targets: List of target field names
        freqbin: Frequency binning factor
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Dict mapping field -> list of MS paths, or None on failure
    """
    logger.substep("Preparing MS for self-calibration...")
    
    env = config.environment
    casa_path = env.get('casa_path', '')
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('selfcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}  # job_id -> (spw, field)
    
    for spw in active_spws:
        for field in targets:
            # Create directory
            field_dir = f"{spw}/{field}"
            os.makedirs(field_dir, exist_ok=True)
            os.makedirs(f"{field_dir}/selfcal-tables", exist_ok=True)
            
            # Check src.ms exists
            src_ms = f"{spw}/src.ms"
            if not os.path.exists(src_ms):
                logger.warning(f"src.ms not found for {spw}, skipping")
                continue
            
            output_ms = f"{field_dir}/sc.ms"
            
            # CASA script for split + average
            script = f'''# Split and average for selfcal
# {spw}/{field}

import os

# Split field
mstransform(
    vis='{src_ms}',
    outputvis='{output_ms}',
    field='{field}',
    datacolumn='CORRECTED',
    chanaverage=True,
    chanbin={freqbin},
    keepflags=True
)

print("Split and averaged: {output_ms}")
'''
            
            script_file = f"prep_sc_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
            
            job = hk.submit(
                command=command,
                name=f"prep_sc_{spw}_{field}",
                job_subdir=field_dir,
                ppn=ppn,
                walltime=resources.get('walltime', '02:00:00')
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (spw, field)
                logger.info(f"Submitted prep {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        logger.error("No prep jobs submitted")
        return None
    
    # Wait for all jobs
    logger.substep(f"Waiting for {len(job_ids)} prep jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Build MS map
    ms_map = {}  # field -> [ms_paths]
    
    for job_id, (job, log_result) in results.items():
        spw, field = job_map.get(job_id, ('unknown', 'unknown'))
        output_ms = f"{spw}/{field}/sc.ms"
        
        if log_result.success and os.path.exists(output_ms):
            if field not in ms_map:
                ms_map[field] = []
            ms_map[field].append(output_ms)
            logger.info(f"{spw}/{field}: OK")
        else:
            logger.error(f"{spw}/{field}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    if not ms_map:
        return None
    
    logger.info(f"Prepared {sum(len(v) for v in ms_map.values())} MS files for {len(ms_map)} fields")
    return ms_map
