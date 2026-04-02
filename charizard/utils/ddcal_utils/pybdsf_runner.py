# charizard/utils/ddcal_utils/pybdsf_runner.py
"""
PyBDSF source finding for DDCal.
Uses 5 sigma threshold.
"""

import os
import time
from typing import List, Dict, Optional

from housekeeper import Housekeeper
from ..container import build_udocker_prefix


def run_pybdsf(hk: Housekeeper,
               config,
               image_map: Dict[str, str],
               logger,
               whitelist: List[str]) -> Optional[Dict[str, Dict[str, str]]]:
    """
    Run PyBDSF on final selfcal images.
    
    Uses 5 sigma threshold for island detection.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        image_map: Dict mapping field -> MFS image path
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Dict mapping field -> {catalog, mask} paths, or None on failure
    """
    logger.substep("Running PyBDSF source finding...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('pybdsf', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}  # job_id -> field
    
    for field, image_path in image_map.items():
        if not os.path.exists(image_path):
            logger.warning(f"Image not found for {field}: {image_path}")
            continue
        
        field_dir = f"ddcal_output/{field}"
        os.makedirs(field_dir, exist_ok=True)
        
        # Output files
        catalog_file = f"{field_dir}/{field}_catalog.fits"
        mask_file = f"{field_dir}/{field}_mask.fits"
        
        # PyBDSF script - 5 sigma threshold
        script = f'''#!/usr/bin/env python3
# PyBDSF source finding for {field}
# 5 sigma threshold

import bdsf

img = bdsf.process_image(
    '{image_path}',
    thresh_isl=5.0,
    thresh_pix=5.0,
    rms_box=(150, 50),
    rms_map=True,
    mean_map='zero',
    ini_method='intensity',
    adaptive_rms_box=True,
    adaptive_thresh=150,
    group_by_isl=True,
    quiet=False
)

# Write catalog
img.write_catalog(
    outfile='{catalog_file}',
    format='fits',
    catalog_type='srl',
    clobber=True
)

# Write island mask
img.export_image(
    outfile='{mask_file}',
    img_type='island_mask',
    clobber=True
)

print(f"Catalog: {catalog_file}")
print(f"Mask: {mask_file}")
print(f"Found {{img.nsrc}} sources in {{img.nisl}} islands")
'''
        
        script_file = f"pybdsf_{field}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} python3 {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"pybdsf_{field}",
            job_subdir=field_dir,
            ppn=ppn,
            walltime=resources.get('walltime', '01:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = field
            logger.info(f"Submitted PyBDSF {field}: {job.job_id}")
        
        time.sleep(0.3)
    
    if not job_ids:
        logger.error("No PyBDSF jobs submitted")
        return None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} PyBDSF jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Collect results
    output_map = {}
    
    for job_id, (job, log_result) in results.items():
        field = job_map.get(job_id, 'unknown')
        catalog_file = f"ddcal_output/{field}/{field}_catalog.fits"
        mask_file = f"ddcal_output/{field}/{field}_mask.fits"
        
        if log_result.success:
            output_map[field] = {
                'catalog': catalog_file,
                'mask': mask_file,
            }
            logger.info(f"{field}: OK")
        else:
            logger.error(f"{field}: PyBDSF FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    return output_map if output_map else None
