# charizard/utils/flagging_utils/catboss.py
"""
Catboss RFI flagger interface.

Replaces tfcrop and rflag with GPU-accelerated flagging.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper

from ..container import build_udocker_prefix


# Hardcoded catboss defaults
# NEVER more than 4 combinations (1,2,4,8)
# NEVER below 5 sigma
CATBOSS_DEFAULTS = {
    'initial': {
        'combinations': '1,2',
        'sigma': 6.0,
        'rho': 1.5,
        'poly_degree': 5,
        'deviation_threshold': 5.0,
    },
    'postcal': {
        'combinations': '1,2,4,8',
        'sigma': 5.0,
        'rho': 1.5,
        'poly_degree': 5,
        'deviation_threshold': 5.0,
    },
    'final': {
        'combinations': '1,2,4,8',
        'sigma': 5.0,
        'rho': 1.5,
        'poly_degree': 5,
        'deviation_threshold': 5.0,
    },
    'residual': {
        'combinations': '1,2,4,8',
        'sigma': 5.0,
        'rho': 1.5,
        'poly_degree': 5,
        'deviation_threshold': 5.0,
    }
}


def build_catboss_command(ms_path: str,
                          stage: str = 'initial',
                          datacolumn: str = 'DATA',
                          max_memory: float = 0.8,
                          use_gpu: bool = True) -> str:
    """
    Build catboss pooh command string.

    Args:
        ms_path:    Path to measurement set
        stage:      Stage name ('initial', 'postcal', 'final', 'residual')
        datacolumn: Data column to flag
        max_memory: Maximum memory fraction (default 0.8)
        use_gpu:    Use GPU mode (default True)

    Returns:
        catboss pooh command string
    """
    defaults = CATBOSS_DEFAULTS.get(stage, CATBOSS_DEFAULTS['initial'])
    mode = 'gpu' if use_gpu else 'cpu'

    cmd = f"catboss pooh {ms_path}"
    cmd += f" --method sumthreshold"
    cmd += f" --combinations {defaults['combinations']}"
    cmd += f" --sigma {defaults['sigma']}"
    cmd += f" --rho {defaults['rho']}"
    cmd += f" --poly-order {defaults['poly_degree']}"
    cmd += f" --deviation-threshold {defaults['deviation_threshold']}"
    cmd += f" --datacolumn {datacolumn}"
    cmd += f" --apply-flags"
    cmd += f" --propagate-flags"
    cmd += f" --mode {mode}"
    cmd += f" --max-memory {max_memory}"
    cmd += f" --verbose"

    return cmd


def run_catboss(hk: Housekeeper,
                config,
                active_spws: List[str],
                ms_names: List[str],
                stage: str,
                datacolumn: str,
                logger,
                whitelist: List[str],
                wait: bool = True,
                prefix: str = '') -> Optional[List[str]]:
    """
    Run catboss RFI flagging.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        ms_names: List of MS names to flag
        stage: Stage name ('initial', 'postcal', 'final')
        datacolumn: Data column to flag
        logger: Logger
        whitelist: Error whitelist
        wait: Whether to wait for jobs to complete
        prefix: Job name prefix
    
    Returns:
        List of successful SPWs (if wait=True) or job_ids (if wait=False)
    """
    logger.substep(f"Running catboss ({stage}) on {ms_names}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    
    # Check if GPU - flagging.use_gpu
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    flagging_config = init_cal.get('flagging', {})
    use_gpu = flagging_config.get('use_gpu', False)
    
    # Get resources
    if use_gpu:
        resources = config.resources.get('gpu', config.resources.get('flagging', {}))
    else:
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
            
            cmd = build_catboss_command(
                ms_path=ms_path,
                stage=stage,
                datacolumn=datacolumn,
                max_memory=0.8,
                use_gpu=use_gpu
            )
            commands.append(cmd)

        if not commands:
            continue

        job_name = f"catboss_{stage}_{prefix}_{spw}" if prefix else f"catboss_{stage}_{spw}"
        udocker = build_udocker_prefix(config)
        batch_script = f"\n{udocker} ".join(commands)

        command = f"""cd {os.getcwd()}
{preamble}
{udocker} {batch_script}
"""

        job = hk.submit(
            command=command,
            name=job_name,
            job_subdir=spw,
            ppn=ppn,
            walltime=resources.get('walltime', '08:00:00'),
            gpu=use_gpu
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted {job_name}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.warning("No catboss jobs submitted")
        return active_spws if wait else []
    
    if not wait:
        # Return job IDs for parallel execution
        return job_ids
    
    # Wait for jobs - housekeeper handles whitelist
    logger.substep(f"Waiting for {len(job_ids)} catboss jobs...")
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
            logger.error(f"{spw}: FAILED (catboss)")
    
    # Return successful SPWs, or None if nothing succeeded
    return successful if successful else None
