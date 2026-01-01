# charizard/utils/ddcal_utils/peeling.py
"""
Sequential peeling loop using CrystalBall + QuartiCal.
Processes each region file one by one.
"""

import os
import time
from typing import List, Optional

from housekeeper import Housekeeper


def run_peeling_loop(hk: Housekeeper,
                     config,
                     field: str,
                     region_files: List[str],
                     source_list_file: str,
                     ms_path: str,
                     logger,
                     whitelist: List[str]) -> Optional[str]:
    """
    Run sequential peeling for each region file.
    
    For each region file (brightest first):
    1. CrystalBall: predict source model into MODEL_DATA
    2. QuartiCal: solve and subtract
    
    Args:
        hk: Housekeeper
        config: Pipeline config
        field: Field name
        region_files: List of region file paths (ordered brightest first)
        source_list_file: WSClean source list from final imaging
        ms_path: Path to combined MS
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Final data column name after peeling, or None if failed
    """
    if not region_files:
        logger.info(f"{field}: No region files to peel")
        return None
    
    logger.substep(f"Peeling {len(region_files)} sources for {field}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    output_dir = f"ddcal_output/{field}"
    os.makedirs(output_dir, exist_ok=True)
    
    current_data_col = "DATA"
    
    for i, region_file in enumerate(region_files):
        source_num = i + 1
        
        logger.info(f"Peeling source {source_num}/{len(region_files)}: {region_file}")
        
        # Output column for this peel
        output_col = f"PEEL_{source_num}_DATA"
        
        # Script: CrystalBall + QuartiCal
        script = f"""#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "=== Peeling source {source_num}/{len(region_files)} ==="
echo "Region file: {region_file}"
echo "Input column: {current_data_col}"
echo "Output column: {output_col}"

MS="{ms_path}"

# Step 1: CrystalBall - predict source model using the region file
echo "Running CrystalBall..."
crystalball $MS \\
    -sm {source_list_file} \\
    -w {region_file} \\
    -o MODEL_DATA \\
    -j {ppn}

if [ $? -ne 0 ]; then
    echo "ERROR: CrystalBall failed"
    exit 1
fi

# Step 2: QuartiCal - solve and subtract
echo "Running QuartiCal..."
goquartical \\
    input_ms.path=$MS \\
    input_ms.data_column={current_data_col} \\
    input_ms.time_chunk=0 \\
    input_ms.freq_chunk=0 \\
    input_model.recipe=MODEL_DATA \\
    solver.terms=[G] \\
    solver.iter_recipe=[50] \\
    output.gain_directory={output_dir}/gains_peel_{source_num} \\
    output.log_directory={output_dir}/logs_peel_{source_num} \\
    output.overwrite=True \\
    output.products=[corrected_residual] \\
    output.columns=[{output_col}] \\
    G.type=diag_complex \\
    G.time_interval=1 \\
    G.freq_interval=0

if [ $? -ne 0 ]; then
    echo "ERROR: QuartiCal failed"
    exit 1
fi

echo "SUCCESS: Peeled source {source_num}"
"""
        
        script_file = f"peel_{field}_{source_num}.sh"
        with open(script_file, 'w') as f:
            f.write(script)
        os.chmod(script_file, 0o755)
        
        # Submit job
        job = hk.submit(
            command=f"bash {os.getcwd()}/{script_file}",
            name=f"peel_{field}_{source_num}",
            job_subdir=output_dir,
            ppn=ppn,
            walltime=resources.get('walltime', '02:00:00')
        )
        
        if not job.job_id:
            logger.error(f"Failed to submit peel job for source {source_num}")
            return None
        
        logger.info(f"Submitted peel job: {job.job_id}")
        
        # Wait for this peel to complete before next (sequential!)
        results = hk.wait_and_check([job.job_id], whitelist=whitelist)
        
        job_result = results.get(job.job_id)
        if job_result:
            job_obj, log_result = job_result
            if not log_result.success:
                logger.error(f"Peel source {source_num} FAILED")
                if log_result.error_lines:
                    for err in log_result.error_lines[:3]:
                        logger.error(f"  >> {err}")
                return None
        
        logger.info(f"Peeled source {source_num}: OK")
        
        # Update data column for next iteration
        current_data_col = output_col
        
        time.sleep(1)
    
    logger.success(f"Peeling complete for {field}. Final column: {current_data_col}")
    return current_data_col