# charizard/utils/ddcal_utils/foresight_runner.py
"""
Run Foresight via job submission to get known sources from TGSS-NVSS catalog.
"""

import os
import time
from typing import Dict, List

from housekeeper import Housekeeper


def run_foresight(hk: Housekeeper,
                  config,
                  fields: List[str],
                  ms_map: Dict[str, str],
                  logger,
                  whitelist: List[str],
                  imsize: int = 4096,
                  cellsize: float = 1.5) -> Dict[str, str]:
    """
    Run Foresight for each field to get known sources from TGSS-NVSS.
    
    Returns:
        Dict of field -> foresight source list path
    """
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('default', {})
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    output_files = {}
    
    for field in fields:
        if field not in ms_map:
            continue
        
        ms_path = ms_map[field]
        output_dir = f"ddcal_output/{field}"
        os.makedirs(output_dir, exist_ok=True)
        
        source_file = f"{output_dir}/foresight_sources.txt"
        mask_file = f"{output_dir}/foresight_mask.fits"
        output_files[field] = source_file
        
        script = f"""#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "Running Foresight for {field}"

foresight {ms_path} \\
    --imsize {imsize} \\
    --cellsize {cellsize} \\
    --source-types S,M \\
    -o {source_file} \\
    -m {mask_file} \\
    --output-type flux \\
    --debug

if [ -f "{source_file}" ]; then
    echo "SUCCESS: Foresight complete"
    wc -l {source_file}
else
    echo "ERROR: No source file created"
    exit 1
fi
"""
        
        script_file = f"foresight_{field}.sh"
        with open(script_file, 'w') as f:
            f.write(script)
        os.chmod(script_file, 0o755)
        
        job = hk.submit(
            command=f"bash {os.getcwd()}/{script_file}",
            name=f"foresight_{field}",
            job_subdir=output_dir,
            ppn=ppn,
            walltime=resources.get('walltime', '01:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = field
            logger.info(f"Submitted Foresight {field}: {job.job_id}")
        
        time.sleep(0.3)
    
    if not job_ids:
        logger.error("No Foresight jobs submitted")
        return {}
    
    logger.substep(f"Waiting for {len(job_ids)} Foresight jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    successful = {}
    for job_id, (job, log_result) in results.items():
        field = job_map.get(job_id, 'unknown')
        
        if log_result.success and os.path.exists(output_files.get(field, '')):
            logger.info(f"{field}: OK")
            successful[field] = output_files[field]
        else:
            logger.error(f"{field}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    return successful