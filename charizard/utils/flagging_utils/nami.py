# charizard/utils/flagging_utils/nami.py
"""
NAMI flagger interface.

CPU-based polynomial fitting flagger for post-calibration data.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper


# Hardcoded NAMI defaults
NAMI_DEFAULTS = {
    'sigma': 5.0,
    'nknots': 2,
    'timebin': 30.0,
}


def build_nami_command(ms_path: str,
                       datacolumn: str = 'CORRECTED_DATA',
                       sigma: float = None,
                       nknots: int = None,
                       timebin: float = None,
                       ncpu: int = 8,
                       field: str = None,
                       corr: str = None) -> str:
    """
    Build nami command string.
    
    Args:
        ms_path: Path to measurement set
        datacolumn: Data column to flag
        sigma: Sigma threshold
        nknots: Number of knots for spline
        timebin: Time bin in seconds
        ncpu: Number of CPUs
        field: Field selection
        corr: Correlation selection
    
    Returns:
        nami command string
    """
    sigma = sigma or NAMI_DEFAULTS['sigma']
    nknots = nknots or NAMI_DEFAULTS['nknots']
    timebin = timebin or NAMI_DEFAULTS['timebin']
    
    cmd = f"nami {ms_path}"
    cmd += f" --datacolumn {datacolumn}"
    cmd += f" --sigma {sigma}"
    cmd += f" --nknots {nknots}"
    cmd += f" --timebin {timebin}"
    cmd += f" --ncpu {ncpu}"
    
    if field:
        cmd += f" --field {field}"
    if corr:
        cmd += f" --corr {corr}"
    
    return cmd


def run_nami(hk: Housekeeper,
             config,
             active_spws: List[str],
             ms_names: List[str],
             datacolumn: str,
             logger,
             whitelist: List[str],
             sigma: float = None,
             wait: bool = True,
             prefix: str = '') -> Optional[List[str]]:
    """
    Run NAMI flagging.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        ms_names: List of MS names to flag
        datacolumn: Data column to flag
        logger: Logger
        whitelist: Error whitelist
        sigma: Sigma threshold (default from NAMI_DEFAULTS)
        wait: Whether to wait for jobs to complete
        prefix: Job name prefix
    
    Returns:
        List of successful SPWs (if wait=True) or job_ids (if wait=False)
    """
    logger.substep(f"Running NAMI on {ms_names}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('flagging', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        commands = []
        
        for ms in ms_names:
            ms_path = f"{spw}/{ms}"
            
            if not os.path.exists(ms_path):
                continue
            
            cmd = build_nami_command(
                ms_path=ms_path,
                datacolumn=datacolumn,
                sigma=sigma,
                nknots=NAMI_DEFAULTS['nknots'],
                timebin=NAMI_DEFAULTS['timebin'],
                ncpu=ppn
            )
            commands.append(cmd)
        
        if not commands:
            continue
        
        job_name = f"nami_{prefix}_{spw}" if prefix else f"nami_{spw}"
        batch_script = '\n'.join(commands)
        
        command = f"""cd {os.getcwd()}
{preamble}
{batch_script}
"""
        
        job = hk.submit(
            command=command,
            name=job_name,
            job_subdir=spw,
            ppn=ppn,
            walltime=resources.get('walltime', '04:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted {job_name}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.warning("No NAMI jobs submitted")
        return active_spws if wait else []
    
    if not wait:
        return job_ids
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} NAMI jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    successful = []
    failed = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if log_result.success:
            successful.append(spw)
            logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.warning(f"{spw}: FAILED (nami)")
    
    return successful if successful else None
