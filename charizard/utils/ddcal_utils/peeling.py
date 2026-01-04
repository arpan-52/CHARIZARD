# # charizard/utils/ddcal_utils/peeling.py
# """
# Sequential peeling loop using CrystalBall + QuartiCal (G + dE).
# Processes each region file one by one.

# For each source:
# 1. CrystalBall: predict source model into unique column
# 2. QuartiCal: solve G + dE and subtract direction
# """

# import os
# import time
# from typing import List, Optional

# from housekeeper import Housekeeper


# def run_crystalball_for_regions(hk: Housekeeper,
#                                  config,
#                                  field: str,
#                                  region_files: List[str],
#                                  source_list_file: str,
#                                  ms_path: str,
#                                  logger,
#                                  whitelist: List[str]) -> Optional[List[str]]:
#     """
#     Run CrystalBall for each region file to create MODEL columns.
#     Runs in parallel - one job per region.
    
#     Returns:
#         List of MODEL column names, or None if failed
#     """
#     logger.substep(f"Running CrystalBall for {len(region_files)} regions...")
    
#     env = config.environment
#     preamble = env.get('shell_preamble', '')
#     resources = config.resources.get('ddcal', config.resources.get('default', {}))
#     ppn = resources.get('ppn', 8)
    
#     output_dir = f"ddcal_output/{field}"
#     os.makedirs(output_dir, exist_ok=True)
    
#     job_ids = []
#     job_map = {}  # job_id -> (source_num, model_col)
#     model_columns = []
    
#     for i, region_file in enumerate(region_files):
#         source_num = i + 1
#         model_col = f"MODEL_SOURCE_{source_num}"
#         model_columns.append(model_col)
        
#         script = f"""#!/bin/bash
# cd {os.getcwd()}
# {preamble}

# echo "=== CrystalBall for source {source_num} ==="
# echo "Region file: {region_file}"
# echo "Output column: {model_col}"

# crystalball {ms_path} \\
#     -sm {source_list_file} \\
#     -w {region_file} \\
#     -o {model_col} \\
#     -j {ppn}

# if [ $? -ne 0 ]; then
#     echo "ERROR: CrystalBall failed for source {source_num}"
#     exit 1
# fi

# echo "SUCCESS: CrystalBall source {source_num} -> {model_col}"
# """
        
#         script_file = f"crystalball_{field}_{source_num}.sh"
#         with open(script_file, 'w') as f:
#             f.write(script)
#         os.chmod(script_file, 0o755)
        
#         job = hk.submit(
#             command=f"bash {os.getcwd()}/{script_file}",
#             name=f"crystalball_{field}_{source_num}",
#             job_subdir=output_dir,
#             ppn=ppn,
#             walltime=resources.get('walltime', '01:00:00')
#         )
        
#         if job.job_id:
#             job_ids.append(job.job_id)
#             job_map[job.job_id] = (source_num, model_col)
#             logger.info(f"Submitted CrystalBall {source_num}: {job.job_id}")
        
#         time.sleep(0.3)
    
#     if not job_ids:
#         logger.error("No CrystalBall jobs submitted")
#         return None
    
#     # Wait for all CrystalBall jobs (parallel)
#     logger.substep(f"Waiting for {len(job_ids)} CrystalBall jobs...")
#     results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
#     # Check results
#     failed = []
#     for job_id, (job, log_result) in results.items():
#         source_num, model_col = job_map.get(job_id, (0, ''))
#         if log_result.success:
#             logger.info(f"CrystalBall source {source_num}: OK -> {model_col}")
#         else:
#             logger.error(f"CrystalBall source {source_num}: FAILED")
#             if log_result.error_lines:
#                 for err in log_result.error_lines[:3]:
#                     logger.error(f"  >> {err}")
#             failed.append(source_num)
    
#     if failed:
#         logger.error(f"CrystalBall failed for sources: {failed}")
#         return None
    
#     return model_columns


# def run_peeling_loop(hk: Housekeeper,
#                      config,
#                      field: str,
#                      region_files: List[str],
#                      source_list_file: str,
#                      ms_path: str,
#                      logger,
#                      whitelist: List[str]) -> Optional[str]:
#     """
#     Run sequential peeling using CrystalBall + QuartiCal (G + dE).
    
#     1. Run CrystalBall for all regions (parallel) -> MODEL_SOURCE_N columns
#     2. Run QuartiCal sequentially for each source (G + dE, subtract)
    
#     Returns:
#         Final data column name after peeling, or None if failed
#     """
#     if not region_files:
#         logger.info(f"{field}: No region files to peel")
#         return None
    
#     logger.substep(f"Peeling {len(region_files)} sources for {field}...")
    
#     # Step 1: Run CrystalBall for all regions (parallel)
#     model_columns = run_crystalball_for_regions(
#         hk=hk,
#         config=config,
#         field=field,
#         region_files=region_files,
#         source_list_file=source_list_file,
#         ms_path=ms_path,
#         logger=logger,
#         whitelist=whitelist
#     )
    
#     if not model_columns:
#         logger.error("CrystalBall failed")
#         return None
    
#     # Step 2: Run QuartiCal sequentially for each source
#     env = config.environment
#     preamble = env.get('shell_preamble', '')
#     resources = config.resources.get('ddcal', config.resources.get('default', {}))
#     ppn = resources.get('ppn', 8)
    
#     # Get peeling config
#     ddcal_config = config.flow.get('dd_cal', {})
#     peeling_config = ddcal_config.get('peeling', {})
#     g_time_interval = peeling_config.get('g_time_interval', '10s')
#     g_freq_interval = peeling_config.get('g_freq_interval', 10)
#     de_time_interval = peeling_config.get('de_time_interval', '100s')
#     de_freq_interval = peeling_config.get('de_freq_interval', 100)
#     time_chunk = peeling_config.get('time_chunk', '300s')
    
#     output_dir = f"ddcal_output/{field}"
    
#     current_data_col = "DATA"
    
#     for i, model_col in enumerate(model_columns):
#         source_num = i + 1
        
#         logger.info(f"Peeling source {source_num}/{len(model_columns)}")
#         logger.info(f"  Input column: {current_data_col}")
#         logger.info(f"  Model column: {model_col}")
        
#         # Output column for this peel
#         output_col = f"PEELED_{source_num}_DATA"
        
#         # QuartiCal recipe: DATA~MODEL:MODEL means solve against MODEL, subtract MODEL
#         # input_model.recipe=CURRENT_DATA~MODEL_COL:MODEL_COL
#         recipe = f"{current_data_col}~{model_col}:{model_col}"
        
#         script = f"""#!/bin/bash
# cd {os.getcwd()}
# {preamble}

# echo "=== QuartiCal Peeling source {source_num}/{len(model_columns)} ==="
# echo "Input column: {current_data_col}"
# echo "Model column: {model_col}"
# echo "Output column: {output_col}"
# echo "Recipe: {recipe}"

# micromamba activate quartical

# goquartical \\
#     input_ms.path={ms_path} \\
#     input_ms.data_column={current_data_col} \\
#     input_ms.time_chunk={time_chunk} \\
#     input_ms.freq_chunk=0 \\
#     input_model.recipe={recipe} \\
#     solver.terms=[G,dE] \\
#     solver.iter_recipe=[25,25,10,10] \\
#     output.gain_directory={output_dir}/gains_peel_{source_num} \\
#     output.log_directory={output_dir}/logs_peel_{source_num} \\
#     output.overwrite=True \\
#     output.products=[corrected_residual] \\
#     output.columns=[{output_col}] \\
#     output.subtract_directions=[1] \\
#     G.type=diag_complex \\
#     G.time_interval={g_time_interval} \\
#     G.freq_interval={g_freq_interval} \\
#     dE.type=complex \\
#     dE.time_interval={de_time_interval} \\
#     dE.freq_interval={de_freq_interval} \\
#     dE.direction_dependent=True

# if [ $? -ne 0 ]; then
#     echo "ERROR: QuartiCal failed for source {source_num}"
#     exit 1
# fi

# echo "SUCCESS: Peeled source {source_num} -> {output_col}"
# """
        
#         script_file = f"quartical_peel_{field}_{source_num}.sh"
#         with open(script_file, 'w') as f:
#             f.write(script)
#         os.chmod(script_file, 0o755)
        
#         job = hk.submit(
#             command=f"bash {os.getcwd()}/{script_file}",
#             name=f"quartical_peel_{field}_{source_num}",
#             job_subdir=output_dir,
#             ppn=ppn,
#             walltime=resources.get('walltime', '02:00:00')
#         )
        
#         if not job.job_id:
#             logger.error(f"Failed to submit QuartiCal job for source {source_num}")
#             return None
        
#         logger.info(f"Submitted QuartiCal peel job: {job.job_id}")
        
#         # Wait for this peel to complete before next (sequential!)
#         results = hk.wait_and_check([job.job_id], whitelist=whitelist)
        
#         job_result = results.get(job.job_id)
#         if job_result:
#             job_obj, log_result = job_result
#             if not log_result.success:
#                 logger.error(f"QuartiCal peel source {source_num} FAILED")
#                 if log_result.error_lines:
#                     for err in log_result.error_lines[:3]:
#                         logger.error(f"  >> {err}")
#                 return None
        
#         logger.info(f"Peeled source {source_num}: OK -> {output_col}")
        
#         # Update data column for next iteration
#         current_data_col = output_col
        
#         time.sleep(1)
    
#     logger.success(f"Peeling complete for {field}. Final column: {current_data_col}")
#     return current_data_col









# charizard/utils/ddcal_utils/peeling.py
"""
Sequential peeling loop using CrystalBall + QuartiCal (G + dE).

1. CrystalBall: one job, runs all regions sequentially (avoid MS locking)
2. QuartiCal: sequential jobs, one per source (G + dE, subtract)
"""

import os
import time
from typing import List, Optional

from housekeeper import Housekeeper


def run_crystalball_for_regions(hk: Housekeeper,
                                 config,
                                 field: str,
                                 region_files: List[str],
                                 source_list_file: str,
                                 ms_path: str,
                                 logger,
                                 whitelist: List[str]) -> Optional[List[str]]:
    """
    Run CrystalBall for all regions in a single job (sequential).
    Avoids MS locking issues.
    
    Returns:
        List of MODEL column names, or None if failed
    """
    logger.substep(f"Running CrystalBall for {len(region_files)} regions...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    output_dir = f"ddcal_output/{field}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Build list of model columns
    model_columns = [f"MODEL_SOURCE_{i+1}" for i in range(len(region_files))]
    
    # Build single script with all CrystalBall commands
    script_lines = [
        f"#!/bin/bash",
        f"cd {os.getcwd()}",
        f"{preamble}",
        f"",
        f"echo '=== CrystalBall for {len(region_files)} sources ==='",
        f"echo 'MS: {ms_path}'",
        f"echo 'Source list: {source_list_file}'",
        f"",
    ]
    
    for i, region_file in enumerate(region_files):
        source_num = i + 1
        model_col = model_columns[i]
        
        script_lines.extend([
            f"echo ''",
            f"echo '--- Source {source_num}/{len(region_files)} ---'",
            f"echo 'Region: {region_file}'",
            f"echo 'Output column: {model_col}'",
            f"",
            f"crystalball {ms_path} \\",
            f"    -sm {source_list_file} \\",
            f"    -w {region_file} \\",
            f"    -o {model_col} \\",
            f"    -j {ppn}",
            f"",
            f"if [ $? -ne 0 ]; then",
            f"    echo 'ERROR: CrystalBall failed for source {source_num}'",
            f"    exit 1",
            f"fi",
            f"echo 'SUCCESS: Source {source_num} -> {model_col}'",
            f"",
        ])
    
    script_lines.append(f"echo 'All CrystalBall jobs complete!'")
    
    script = '\n'.join(script_lines)
    
    script_file = f"crystalball_all_{field}.sh"
    with open(script_file, 'w') as f:
        f.write(script)
    os.chmod(script_file, 0o755)
    
    # Submit single job
    job = hk.submit(
        command=f"bash {os.getcwd()}/{script_file}",
        name=f"crystalball_all_{field}",
        job_subdir=output_dir,
        ppn=ppn,
        walltime=resources.get('walltime', '02:00:00')
    )
    
    if not job.job_id:
        logger.error("Failed to submit CrystalBall job")
        return None
    
    logger.info(f"Submitted CrystalBall job: {job.job_id}")
    
    # Wait for job
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    
    job_result = results.get(job.job_id)
    if job_result:
        job_obj, log_result = job_result
        if not log_result.success:
            logger.error("CrystalBall job FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            return None
    
    logger.info(f"CrystalBall complete: {len(model_columns)} model columns created")
    return model_columns


def run_peeling_loop(hk: Housekeeper,
                     config,
                     field: str,
                     region_files: List[str],
                     source_list_file: str,
                     ms_path: str,
                     logger,
                     whitelist: List[str]) -> Optional[str]:
    """
    Run sequential peeling using CrystalBall + QuartiCal (G + dE).
    
    1. Run CrystalBall for all regions (single job, sequential)
    2. Run QuartiCal sequentially for each source (G + dE, subtract)
    
    Returns:
        Final data column name after peeling, or None if failed
    """
    if not region_files:
        logger.info(f"{field}: No region files to peel")
        return None
    
    logger.substep(f"Peeling {len(region_files)} sources for {field}...")
    
    # Step 1: Run CrystalBall for all regions (single job)
    model_columns = run_crystalball_for_regions(
        hk=hk,
        config=config,
        field=field,
        region_files=region_files,
        source_list_file=source_list_file,
        ms_path=ms_path,
        logger=logger,
        whitelist=whitelist
    )
    
    if not model_columns:
        logger.error("CrystalBall failed")
        return None
    
    # Step 2: Run QuartiCal sequentially for each source
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    # Get peeling config
    ddcal_config = config.flow.get('dd_cal', {})
    peeling_config = ddcal_config.get('peeling', {})
    g_time_interval = peeling_config.get('g_time_interval', '10s')
    g_freq_interval = peeling_config.get('g_freq_interval', 10)
    de_time_interval = peeling_config.get('de_time_interval', '100s')
    de_freq_interval = peeling_config.get('de_freq_interval', 100)
    time_chunk = peeling_config.get('time_chunk', '300s')
    
    output_dir = f"ddcal_output/{field}"
    
    current_data_col = "DATA"
    
    for i, model_col in enumerate(model_columns):
        source_num = i + 1
        
        logger.info(f"Peeling source {source_num}/{len(model_columns)}")
        logger.info(f"  Input column: {current_data_col}")
        logger.info(f"  Model column: {model_col}")
        
        # Output column for this peel
        output_col = f"PEELED_{source_num}_DATA"
        
        # QuartiCal recipe
        recipe = f"{current_data_col}~{model_col}:{model_col}"
        
        script = f"""#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "=== QuartiCal Peeling source {source_num}/{len(model_columns)} ==="
echo "Input column: {current_data_col}"
echo "Model column: {model_col}"
echo "Output column: {output_col}"
echo "Recipe: {recipe}"

micromamba activate quartical

goquartical \\
    input_ms.path={ms_path} \\
    input_ms.data_column={current_data_col} \\
    input_ms.time_chunk={time_chunk} \\
    input_ms.freq_chunk=0 \\
    input_model.recipe={recipe} \\
    solver.terms=[G,dE] \\
    solver.iter_recipe=[25,25,10,10] \\
    output.gain_directory={output_dir}/gains_peel_{source_num} \\
    output.log_directory={output_dir}/logs_peel_{source_num} \\
    output.overwrite=True \\
    output.products=[corrected_residual] \\
    output.columns=[{output_col}] \\
    output.subtract_directions=[1] \\
    G.type=diag_complex \\
    G.time_interval={g_time_interval} \\
    G.freq_interval={g_freq_interval} \\
    dE.type=complex \\
    dE.time_interval={de_time_interval} \\
    dE.freq_interval={de_freq_interval} \\
    dE.direction_dependent=True

if [ $? -ne 0 ]; then
    echo "ERROR: QuartiCal failed for source {source_num}"
    exit 1
fi

echo "SUCCESS: Peeled source {source_num} -> {output_col}"
"""
        
        script_file = f"quartical_peel_{field}_{source_num}.sh"
        with open(script_file, 'w') as f:
            f.write(script)
        os.chmod(script_file, 0o755)
        
        job = hk.submit(
            command=f"bash {os.getcwd()}/{script_file}",
            name=f"quartical_peel_{field}_{source_num}",
            job_subdir=output_dir,
            ppn=ppn,
            walltime=resources.get('walltime', '02:00:00')
        )
        
        if not job.job_id:
            logger.error(f"Failed to submit QuartiCal job for source {source_num}")
            return None
        
        logger.info(f"Submitted QuartiCal peel job: {job.job_id}")
        
        # Wait for this peel to complete before next (sequential!)
        results = hk.wait_and_check([job.job_id], whitelist=whitelist)
        
        job_result = results.get(job.job_id)
        if job_result:
            job_obj, log_result = job_result
            if not log_result.success:
                logger.error(f"QuartiCal peel source {source_num} FAILED")
                if log_result.error_lines:
                    for err in log_result.error_lines[:3]:
                        logger.error(f"  >> {err}")
                return None
        
        logger.info(f"Peeled source {source_num}: OK -> {output_col}")
        
        # Update data column for next iteration
        current_data_col = output_col
        
        time.sleep(1)
    
    logger.success(f"Peeling complete for {field}. Final column: {current_data_col}")
    return current_data_col
