# charizard/utils/selfcal_utils/imaging.py
"""
Imaging utilities using WSClean.
"""

import os
import time
from typing import List, Dict, Optional, Tuple

from housekeeper import Housekeeper
from ..container import build_udocker_prefix


def build_wsclean_command(ms_list: List[str],
                          output_name: str,
                          imsize: int,
                          cellsize: str,
                          niter: int,
                          datacolumn: str = 'DATA',
                          threshold: float = 0.0001,
                          use_mask: bool = False,
                          mask_path: str = None,
                          stokes: str = 'I',
                          save_source_list: bool = False) -> str:
    """
    Build wsclean command.
    
    Args:
        ms_list: List of MS paths to image together
        output_name: Output image name prefix
        imsize: Image size in pixels
        cellsize: Cell size (e.g., '1asec')
        niter: Number of clean iterations (0 for dirty)
        datacolumn: Data column to image
        threshold: Clean threshold in Jy
        use_mask: Whether to use a mask
        mask_path: Path to FITS mask
        stokes: Stokes parameter (I, Q, U, V)
        save_source_list: Whether to save wsclean source list
    
    Returns:
        wsclean command string
    """
    ms_str = ' '.join(ms_list)
    
    cmd = f"""wsclean \\
    -name {output_name} \\
    -weight briggs 0.0 \\
    -super-weight 1.0 \\
    -size {imsize} {imsize} \\
    -scale {cellsize} \\
    -channels-out 4 \\
    -pol {stokes} \\
    -data-column {datacolumn} \\
    -niter {niter} \\
    -auto-mask 7 \\
    -auto-threshold 3 \\
    -gain 0.1 \\
    -mgain 0.7 \\
    -join-channels \\
    -multiscale \\
    -no-negative \\
    -multiscale-scale-bias 0.6 \\
    -fit-spectral-pol 3 \\
    -fit-beam \\
    -padding 1.3"""
    
    if niter > 0:
        cmd += f" \\\n    -threshold {threshold}"
    
    if use_mask and mask_path and os.path.exists(mask_path):
        cmd += f" \\\n    -fits-mask {mask_path}"
    
    if save_source_list:
        cmd += f" \\\n    -save-source-list"
    
    cmd += f" \\\n    {ms_str}"
    
    return cmd


def run_wsclean(hk: Housekeeper,
                config,
                ms_map: Dict[str, List[str]],
                niter: int,
                prefix: str,
                logger,
                whitelist: List[str],
                datacolumn: str = 'DATA',
                use_masks: bool = False,
                stokes: str = 'I',
                save_source_list: bool = False) -> Optional[Dict[str, str]]:
    """
    Run wsclean for all fields in parallel.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        ms_map: Dict mapping field -> list of MS paths
        niter: Number of clean iterations
        prefix: Output prefix (e.g., 'dirty', 'selfcal_p0')
        logger: Logger
        whitelist: Error whitelist
        datacolumn: Data column to image
        use_masks: Whether to use masks
        stokes: Stokes parameter (I, Q, U, V)
        save_source_list: Whether to save wsclean source list
    
    Returns:
        Dict mapping field -> image path, or None on failure
    """
    logger.substep(f"Running wsclean ({prefix}, niter={niter}, stokes={stokes})...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('imaging', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    # Get imaging params
    flow = config.flow
    selfcal_config = flow.get('imaging_selfcal', {}).get('selfcal', {})
    imaging_config = selfcal_config.get('imaging', {})
    clean_config = selfcal_config.get('clean', {})
    
    imsize = imaging_config.get('imsize', 4096)
    cellsize = imaging_config.get('cellsize', '1asec')
    threshold = clean_config.get('threshold', 0.001)
    
    # Create images directory
    os.makedirs('images', exist_ok=True)
    
    job_ids = []
    job_map = {}  # job_id -> field
    
    for field, ms_list in ms_map.items():
        if not ms_list:
            continue
        
        # Field output directory
        field_dir = f"images/{field}"
        os.makedirs(field_dir, exist_ok=True)
        
        output_name = f"{field_dir}/{prefix}_{field}"
        
        # Check for mask
        mask_path = f"{field_dir}/mask.fits"
        
        # Build command
        cmd = build_wsclean_command(
            ms_list=ms_list,
            output_name=output_name,
            imsize=imsize,
            cellsize=cellsize,
            niter=niter,
            datacolumn=datacolumn,
            threshold=threshold,
            use_mask=use_masks,
            mask_path=mask_path,
            stokes=stokes,
            save_source_list=save_source_list
        )
        
        script_file = f"wsclean_{prefix}_{field}.sh"
        with open(script_file, 'w') as f:
            f.write(f"#!/bin/bash\ncd {os.getcwd()}\n{preamble}\n{cmd}\n")
        os.chmod(script_file, 0o755)

        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} {cmd}
"""
        
        job = hk.submit(
            command=command,
            name=f"wsclean_{prefix}_{field}",
            job_subdir=field_dir,
            ppn=ppn,
            walltime=resources.get('walltime', '04:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = field
            logger.info(f"Submitted wsclean {field}: {job.job_id}")
        
        time.sleep(0.3)
    
    if not job_ids:
        logger.error("No wsclean jobs submitted")
        return None
    
    # Wait for all jobs
    logger.substep(f"Waiting for {len(job_ids)} wsclean jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Collect results - just check if job succeeded, don't check files
    image_map = {}
    
    for job_id, (job, log_result) in results.items():
        field = job_map.get(job_id, 'unknown')
        image_path = f"images/{field}/{prefix}_{field}-MFS-image.fits"
        source_list_path = f"images/{field}/{prefix}_{field}-sources.txt"
        
        if log_result.success:
            image_map[field] = {
                'image': image_path,
                'source_list': source_list_path if save_source_list else None
            }
            logger.info(f"{field}: OK")
        else:
            logger.warning(f"{field}: wsclean had issues")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    return image_map if image_map else None


def run_dirty_image(hk: Housekeeper,
                    config,
                    ms_map: Dict[str, List[str]],
                    logger,
                    whitelist: List[str]) -> Optional[Dict[str, str]]:
    """
    Create dirty images for all fields.
    """
    return run_wsclean(
        hk=hk,
        config=config,
        ms_map=ms_map,
        niter=0,
        prefix='dirty',
        logger=logger,
        whitelist=whitelist,
        datacolumn='DATA',
        use_masks=False
    )
