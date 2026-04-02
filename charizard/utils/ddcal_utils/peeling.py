# charizard/utils/ddcal_utils/peeling.py
"""
DDCal peeling using CrystalBall + QuartiCal (G + dE).
Peels all sources in one go.
"""

import os
import time
from typing import List, Optional

from housekeeper import Housekeeper

from ..container import build_udocker_prefix


def run_ddcal_peeling(hk: Housekeeper,
                      config,
                      field: str,
                      region_files: List[str],
                      source_list_file: str,
                      ms_path: str,
                      logger,
                      whitelist: List[str]) -> Optional[str]:
    """
    Run DDCal peeling: CrystalBall + QuartiCal + Final Image.
    
    1. CrystalBall: load all sources -> MODEL_DATA, then each peel source -> MODEL_SOURCE_N
    2. QuartiCal: peel all sources in one go with G + dE
    3. WSClean: final image on PEELED_DATA
    
    Returns:
        Path to final image, or None if failed
    """
    if not region_files:
        logger.info(f"{field}: No sources to peel")
        return None
    
    num_sources = len(region_files)
    logger.substep(f"DDCal peeling {num_sources} sources for {field}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    # Get peeling config
    ddcal_config = config.flow.get('dd_cal', {})
    peeling_config = ddcal_config.get('peeling', {})
    g_time_interval = peeling_config.get('g_time_interval', '120s')
    g_freq_interval = peeling_config.get('g_freq_interval', '10MHz')
    de_time_interval = peeling_config.get('de_time_interval', '120s')
    de_freq_interval = peeling_config.get('de_freq_interval', '10MHz')
    
    # Get imaging config
    selfcal_config = config.flow.get('imaging_selfcal', {}).get('selfcal', {})
    imaging_config = selfcal_config.get('imaging', {})
    imsize = imaging_config.get('imsize', 4096)
    cellsize = imaging_config.get('cellsize', '1asec')
    niter = ddcal_config.get('final_niter', 50000)
    
    output_dir = f"ddcal_output/{field}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Build model column names
    model_columns = [f"MODEL_SOURCE_{i+1}" for i in range(num_sources)]
    
    # =========================================================================
    # STEP 1: CrystalBall - one job, all sources
    # =========================================================================
    logger.substep("Running CrystalBall...")
    
    cb_lines = [
        f"#!/bin/bash",
        f"echo '=== CrystalBall for {field} ==='",
        f"",
        f"# First: load ALL sources into MODEL_DATA",
        f"echo 'Loading all sources -> MODEL_DATA'",
        f"crystalball {ms_path} \\",
        f"    -sm {source_list_file} \\",
        f"    -o MODEL_DATA \\",
        f"    -j {ppn}",
        f"",
        f"if [ $? -ne 0 ]; then",
        f"    echo 'ERROR: CrystalBall failed for MODEL_DATA'",
        f"    exit 1",
        f"fi",
        f"echo 'SUCCESS: MODEL_DATA created'",
        f"",
    ]
    
    # Then: each peel source
    for i, region_file in enumerate(region_files):
        source_num = i + 1
        model_col = model_columns[i]
        
        cb_lines.extend([
            f"# Source {source_num}: {region_file} -> {model_col}",
            f"echo 'Source {source_num} -> {model_col}'",
            f"crystalball {ms_path} \\",
            f"    -sm {source_list_file} \\",
            f"    -w {region_file} \\",
            f"    -o {model_col} \\",
            f"    -j {ppn}",
            f"",
            f"if [ $? -ne 0 ]; then",
            f"    echo 'ERROR: CrystalBall failed for {model_col}'",
            f"    exit 1",
            f"fi",
            f"echo 'SUCCESS: {model_col} created'",
            f"",
        ])
    
    cb_lines.append(f"echo 'All CrystalBall complete!'")
    
    cb_script = '\n'.join(cb_lines)
    cb_script_file = f"crystalball_{field}.sh"
    with open(cb_script_file, 'w') as f:
        f.write(cb_script)
    os.chmod(cb_script_file, 0o755)
    
    udocker = build_udocker_prefix(config)
    job = hk.submit(
        command=f"cd {os.getcwd()}\n{preamble}\n{udocker} bash {os.getcwd()}/{cb_script_file}",
        name=f"crystalball_{field}",
        job_subdir=output_dir,
        ppn=ppn,
        walltime=resources.get('walltime', '02:00:00')
    )
    
    if not job.job_id:
        logger.error("Failed to submit CrystalBall job")
        return None
    
    logger.info(f"Submitted CrystalBall job: {job.job_id}")
    
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    job_result = results.get(job.job_id)
    if job_result:
        job_obj, log_result = job_result
        if not log_result.success:
            logger.error("CrystalBall FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            return None
    
    logger.info("CrystalBall complete")
    
    # =========================================================================
    # STEP 2: QuartiCal - peel all sources in one go
    # =========================================================================
    logger.substep("Running QuartiCal peeling...")
    
    # Build recipe: MODEL_DATA~MODEL_SOURCE_1~MODEL_SOURCE_2:MODEL_SOURCE_1:MODEL_SOURCE_2
    model_parts = '~'.join(model_columns)
    direction_parts = ':'.join(model_columns)
    recipe = f"MODEL_DATA~{model_parts}:{direction_parts}"
    
    # Build subtract_directions: [1,2,3,...]
    subtract_dirs = ','.join([str(i+1) for i in range(num_sources)])
    
    qc_script = f"""#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "=== QuartiCal Peeling for {field} ==="
echo "Recipe: {recipe}"
echo "Subtract directions: [{subtract_dirs}]"

micromamba activate quartical

# goquartical \\
#     input_ms.path={ms_path} \\
#     input_ms.data_column=DATA \\
#     input_ms.time_chunk=0 \\
#     input_ms.freq_chunk=0 \\
#     input_model.recipe={recipe} \\
#     solver.terms=[K,dE] \\
#     solver.iter_recipe=[50,50,50,50,50,50,50,50,50,50] \\
#     output.gain_directory={output_dir}/gains_peel \\
#     output.log_directory={output_dir}/logs_peel \\
#     output.overwrite=True \\
#     output.products=[corrected_residual] \\
#     output.columns=[PEELED_DATA] \\
#     output.subtract_directions=[{subtract_dirs}] \\
#     G.type=complex \\
#     G.time_interval={g_time_interval} \\
#     G.freq_interval={g_freq_interval} \\
#     dE.type=complex \\
#     dE.time_interval={de_time_interval} \\
#     dE.freq_interval={de_freq_interval} \\
#     dE.direction_dependent=True \\
#     G.direction_dependent=False \\
#     K.type=delay_and_offset \\
#     K.solve_per=antenna \\
#     K.direction_dependent=False \\
#     K.pinned_directions=[0] \\
#     K.time_interval=120 \\
#     K.freq_interval=0 \\
#     K.interp_mode=reim \\
#     K.interp_method=2dlinear \\
#     K.respect_scan_boundaries=True \\
#     K.initial_estimate=False



goquartical \\
    input_ms.path={ms_path} \\
    input_ms.data_column=DATA \\
    input_ms.time_chunk=0 \\
    input_ms.freq_chunk=0 \\
    input_model.recipe={recipe} \\
    solver.terms=[K,dE] \\
    solver.iter_recipe=[50,50,50,50,50,50,50,50,50,50] \\
    output.gain_directory={output_dir}/gains_peel \\
    output.log_directory={output_dir}/logs_peel \\
    output.overwrite=True \\
    output.products=[corrected_residual] \\
    output.columns=[PEELED_DATA] \\
    output.subtract_directions=[{subtract_dirs}] \\
    dE.type=complex \\
    dE.time_interval={de_time_interval} \\
    dE.freq_interval={de_freq_interval} \\
    dE.direction_dependent=True \\
    K.type=delay_and_offset \\
    K.solve_per=antenna \\
    K.direction_dependent=False \\
    K.pinned_directions=[0] \\
    K.time_interval=120 \\
    K.freq_interval=0 \\
    K.interp_mode=reim \\
    K.interp_method=2dlinear \\
    K.respect_scan_boundaries=True \\
    K.initial_estimate=False



if [ $? -ne 0 ]; then
    echo "ERROR: QuartiCal failed"
    exit 1
fi

echo "SUCCESS: QuartiCal peeling complete -> PEELED_DATA"
"""
    
    qc_script_file = f"quartical_peel_{field}.sh"
    with open(qc_script_file, 'w') as f:
        f.write(qc_script)
    os.chmod(qc_script_file, 0o755)
    
    job = hk.submit(
        command=f"bash {os.getcwd()}/{qc_script_file}",
        name=f"quartical_peel_{field}",
        job_subdir=output_dir,
        ppn=ppn,
        walltime=resources.get('walltime', '04:00:00')
    )
    
    if not job.job_id:
        logger.error("Failed to submit QuartiCal job")
        return None
    
    logger.info(f"Submitted QuartiCal job: {job.job_id}")
    
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    job_result = results.get(job.job_id)
    if job_result:
        job_obj, log_result = job_result
        if not log_result.success:
            logger.error("QuartiCal FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            return None
    
    logger.info("QuartiCal peeling complete")
    
    # =========================================================================
    # STEP 3: Final WSClean image
    # =========================================================================
    logger.substep("Running final WSClean image...")
    
    image_prefix = f"{output_dir}/peeled_{field}"
    
    ws_script = f"""#!/bin/bash
echo "=== Final WSClean for {field} ==="
echo "Data column: PEELED_DATA"

wsclean \\
    -name {image_prefix} \\
    -size {imsize} {imsize} \\
    -scale {cellsize} \\
    -channels-out 4 \\
    -pol I \\
    -intervals-out 1 \\
    -data-column PEELED_DATA \\
    -niter 50000 \\
    -auto-mask 7 \\
    -auto-threshold 3 \\
    -gain 0.1 \\
    -mgain 0.7 \\
    -weight briggs 0.0 \\
    -join-channels \\
    -multiscale \\
    -no-negative \\
    -multiscale-scale-bias 0.6 \\
    -fit-spectral-pol 3 \\
    -fit-beam \\
    -elliptical-beam \\
    -padding 1.3 \\
    -parallel-deconvolution 8192 \\
    -save-source-list \\
    {ms_path}

if [ ! -f "{image_prefix}-MFS-image.fits" ]; then
    echo "ERROR: WSClean failed - no output image"
    exit 1
fi

echo "SUCCESS: Final image created"
"""
    
    ws_script_file = f"wsclean_peeled_{field}.sh"
    with open(ws_script_file, 'w') as f:
        f.write(ws_script)
    os.chmod(ws_script_file, 0o755)
    
    job = hk.submit(
        command=f"cd {os.getcwd()}\n{preamble}\n{udocker} bash {os.getcwd()}/{ws_script_file}",
        name=f"wsclean_peeled_{field}",
        job_subdir=output_dir,
        ppn=config.resources.get('imaging', {}).get('ppn', 8),
        walltime='04:00:00'
    )
    
    if not job.job_id:
        logger.error("Failed to submit WSClean job")
        return None
    
    logger.info(f"Submitted WSClean job: {job.job_id}")
    
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    job_result = results.get(job.job_id)
    if job_result:
        job_obj, log_result = job_result
        if not log_result.success:
            logger.error("WSClean FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            return None
    
    final_image = f"{image_prefix}-MFS-image.fits"
    logger.success(f"DDCal complete! Final image: {final_image}")
    return final_image