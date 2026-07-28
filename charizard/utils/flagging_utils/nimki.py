# charizard/utils/flagging_utils/nimki.py
"""
NIMKI flagger interface.

CPU-based UV-domain Gabor flagger (catboss nimki) for post-calibration data.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper
from ..general.jobs import wait_and_check
from ..general.resources import submit_resources

from ..container import build_udocker_prefix


# Hardcoded nimki defaults (matches catboss nimki defaults)
NIMKI_DEFAULTS = {
    'sigma': 5.0,
    'n_components': 5,  # Gabor components
    'timebin': 30.0,    # minutes
}


def build_nimki_command(ms_path: str,
                        datacolumn: str = 'CORRECTED_DATA',
                        sigma: float = None,
                        n_components: int = None,
                        timebin: float = None,
                        ncpu: int = 8,
                        field: str = None,
                        corr: str = None) -> str:
    """
    Build catboss nimki command string.

    Args:
        ms_path:      Path to measurement set
        datacolumn:   Data column to flag
        sigma:        Sigma threshold (default 5.0)
        n_components: Number of Gabor components (default 5)
        timebin:      Time bin in minutes (default 30.0)
        ncpu:         Number of CPUs (0 = all)
        field:        Field selection
        corr:         Correlation selection

    Returns:
        catboss nimki command string
    """
    sigma = sigma or NIMKI_DEFAULTS['sigma']
    n_components = n_components or NIMKI_DEFAULTS['n_components']
    timebin = timebin or NIMKI_DEFAULTS['timebin']

    cmd = f"catboss nimki {ms_path}"
    cmd += f" --datacolumn {datacolumn}"
    cmd += f" --sigma {sigma}"
    cmd += f" --n-components {n_components}"
    cmd += f" --timebin {timebin}"
    cmd += f" --ncpu {ncpu}"
    cmd += f" --apply-flags"

    if field:
        cmd += f" --field {field}"
    if corr:
        cmd += f" --corr {corr}"

    return cmd


def run_nimki(hk: Housekeeper,
              config,
              active_spws: List[str],
              ms_names: List[str],
              datacolumn: str,
              logger,
              whitelist: List[str],
              sigma: float = None,
              n_components: int = None,
              timebin: float = None,
              wait: bool = True,
              prefix: str = '') -> Optional[List[str]]:
    """
    Run nimki flagging (catboss nimki).

    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        ms_names: List of MS names to flag
        datacolumn: Data column to flag
        logger: Logger
        whitelist: Error whitelist
        sigma: Sigma threshold (default 5.0)
        n_components: Gabor components (default 5)
        timebin: Time bin in minutes (default 30.0)
        wait: Whether to wait for jobs to complete
        prefix: Job name prefix

    Returns:
        List of successful SPWs (if wait=True) or job_ids (if wait=False)
    """
    logger.substep(f"Running nimki on {ms_names}...")

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

            cmd = build_nimki_command(
                ms_path=ms_path,
                datacolumn=datacolumn,
                sigma=sigma,
                n_components=n_components,
                timebin=timebin,
                ncpu=ppn
            )
            commands.append(cmd)

        if not commands:
            continue

        job_name = f"nimki_{prefix}_{spw}" if prefix else f"nimki_{spw}"
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
            **submit_resources(resources, '04:00:00', ppn=ppn)
        )

        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted {job_name}: {job.job_id}")

        time.sleep(0.5)

    if not job_ids:
        logger.warning("No nimki jobs submitted")
        return active_spws if wait else []

    if not wait:
        return job_ids

    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} nimki jobs...")
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
            logger.warning(f"{spw}: FAILED (nimki)")

    return successful if successful else None
