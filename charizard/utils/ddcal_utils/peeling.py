# charizard/utils/ddcal_utils/peeling.py
"""
Sequential peeling loop for DDCal.

For each bright source:
1. crystalball - predict model from wsclean source list + region
2. quartical - G + dE calibration, subtract direction
3. Use peeled MS for next source

Peeling is sequential - one source at a time.
Each peel uses the output from the previous peel.
"""

import os
import time
from typing import List, Dict, Optional

from housekeeper import Housekeeper


def run_crystalball(hk: Housekeeper,
                    config,
                    field: str,
                    source_idx: int,
                    source_info: Dict,
                    source_list_file: str,
                    region_file: str,
                    ms_path: str,
                    logger,
                    whitelist: List[str]) -> Optional[str]:
    """
    Run crystalball for one source.
    
    Args:
        hk: Housekeeper
        config: Config
        field: Field name
        source_idx: Source index (0, 1, 2...)
        source_info: Source dict with ra, dec, flux_mJy
        source_list_file: WSClean source list file
        region_file: DS9 region file for this source
        ms_path: MS path
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Model column name or None
    """
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    field_dir = f"ddcal_output/{field}"
    model_column = f"MODEL_SOURCE_{source_idx}"
    
    # Crystalball command
    script = f'''#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "Running crystalball for source {source_idx} (flux={source_info['flux_mJy']:.1f} mJy)"

crystalball {ms_path} \\
    -sm {source_list_file} \\
    -w {region_file} \\
    -o {model_column}

echo "Model written to column: {model_column}"
'''
    
    script_file = f"crystalball_{field}_src{source_idx}.sh"
    with open(script_file, 'w') as f:
        f.write(script)
    os.chmod(script_file, 0o755)
    
    job = hk.submit(
        command=f"bash {os.getcwd()}/{script_file}",
        name=f"crystalball_{field}_src{source_idx}",
        job_subdir=field_dir,
        ppn=ppn,
        walltime=resources.get('walltime', '01:00:00')
    )
    
    if not job.job_id:
        logger.error(f"Failed to submit crystalball for source {source_idx}")
        return None
    
    logger.info(f"Submitted crystalball source {source_idx}: {job.job_id}")
    
    # Wait for this job
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    
    for job_id, (j, log_result) in results.items():
        if log_result.success:
            logger.info(f"Crystalball source {source_idx}: OK")
            return model_column
        else:
            logger.error(f"Crystalball source {source_idx}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
            return None
    
    return None


def run_quartical_peel(hk: Housekeeper,
                       config,
                       field: str,
                       source_idx: int,
                       model_column: str,
                       input_column: str,
                       ms_path: str,
                       logger,
                       whitelist: List[str]) -> Optional[str]:
    """
    Run quartical to peel one source.
    
    Args:
        hk: Housekeeper
        config: Config
        field: Field name
        source_idx: Source index
        model_column: Model column from crystalball
        input_column: Input data column (DATA or previous SUBDD)
        ms_path: MS path
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Output column name (SUBDD_SOURCE_X) or None
    """
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('ddcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    field_dir = f"ddcal_output/{field}"
    output_column = f"SUBDD_SOURCE_{source_idx}"
    
    # Get peeling intervals from config
    ddcal_config = config.flow.get('dd_cal', {})
    peeling_config = ddcal_config.get('peeling', {})
    time_interval = peeling_config.get('time_interval', 60)
    freq_interval = peeling_config.get('freq_interval', 10)
    
    # Quartical command - fixed parameters
    # Terms: G (direction-independent) + dE (direction-dependent for peeling)
    # iter_recipe: [25, 25, 10, 10]
    # G: diag_complex
    # dE: complex, direction_dependent=true
    script = f'''#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "Running quartical peel for source {source_idx}"
echo "Input column: {input_column}"
echo "Model column: {model_column}"
echo "Output column: {output_column}"

cd {field_dir}

goquartical \\
    input_ms.path={os.path.basename(ms_path)} \\
    input_ms.data_column={input_column} \\
    input_ms.time_chunk=300s \\
    input_ms.freq_chunk=0 \\
    input_model.recipe=MODEL_DATA~{model_column}:{model_column} \\
    solver.terms=[G,dE] \\
    solver.iter_recipe=[25,25,10,10] \\
    solver.convergence_fraction=0.95 \\
    output.log_directory=quartical_logs_src{source_idx} \\
    output.gain_directory=quartical_gains_src{source_idx} \\
    output.overwrite=True \\
    output.products=[corrected_data,corrected_residual,corrected_weight] \\
    output.columns=[CORRECTED_DATA,{output_column},WEIGHT_SPECTRUM] \\
    output.subtract_directions=[1] \\
    dask.threads=6 \\
    G.type=diag_complex \\
    G.time_interval={time_interval}s \\
    G.freq_interval={freq_interval} \\
    dE.type=complex \\
    dE.direction_dependent=true \\
    dE.time_interval={time_interval} \\
    dE.freq_interval={freq_interval}

cd {os.getcwd()}
echo "Peeling complete. Output column: {output_column}"
'''
    
    script_file = f"quartical_peel_{field}_src{source_idx}.sh"
    with open(script_file, 'w') as f:
        f.write(script)
    os.chmod(script_file, 0o755)
    
    job = hk.submit(
        command=f"bash {os.getcwd()}/{script_file}",
        name=f"quartical_peel_{field}_src{source_idx}",
        job_subdir=field_dir,
        ppn=ppn,
        walltime=resources.get('walltime', '02:00:00')
    )
    
    if not job.job_id:
        logger.error(f"Failed to submit quartical peel for source {source_idx}")
        return None
    
    logger.info(f"Submitted quartical peel source {source_idx}: {job.job_id}")
    
    # Wait for this job
    results = hk.wait_and_check([job.job_id], whitelist=whitelist)
    
    for job_id, (j, log_result) in results.items():
        if log_result.success:
            logger.info(f"Quartical peel source {source_idx}: OK")
            return output_column
        else:
            logger.error(f"Quartical peel source {source_idx}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
            return None
    
    return None


def run_peeling_loop(hk: Housekeeper,
                     config,
                     field: str,
                     sources: List[Dict],
                     source_list_file: str,
                     ms_path: str,
                     logger,
                     whitelist: List[str]) -> Optional[str]:
    """
    Run sequential peeling loop for one field.
    
    Peels sources one by one, brightest first.
    Each peel uses output from previous peel.
    
    Args:
        hk: Housekeeper
        config: Config
        field: Field name
        sources: List of sources to peel (sorted brightest first)
        source_list_file: WSClean source list
        ms_path: Combined MS path
        logger: Logger
        whitelist: Error whitelist
    
    Returns:
        Final output column name or None
    """
    logger.substep(f"Peeling {len(sources)} sources for {field}...")
    
    field_dir = f"ddcal_output/{field}"
    
    # Start with DATA column
    current_input_column = "DATA"
    final_output_column = None
    
    for idx, source in enumerate(sources):
        logger.info(f"=== Peeling source {idx+1}/{len(sources)} (flux={source['flux_mJy']:.1f} mJy) ===")
        
        # Create region file for this single source
        region_file = f"{field_dir}/peel_source_{idx}.reg"
        with open(region_file, 'w') as f:
            f.write("# Region file format: DS9 version 4.1\n")
            f.write('global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" ')
            f.write('select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n')
            f.write("fk5\n")
            f.write(f'circle({source["ra"]:.7f},{source["dec"]:.7f},{source["region_radius"]:.3f}")\n')
        
        # Step 1: Crystalball
        model_column = run_crystalball(
            hk=hk,
            config=config,
            field=field,
            source_idx=idx,
            source_info=source,
            source_list_file=source_list_file,
            region_file=region_file,
            ms_path=ms_path,
            logger=logger,
            whitelist=whitelist
        )
        
        if not model_column:
            logger.error(f"Crystalball failed for source {idx}, stopping peeling")
            break
        
        # Step 2: Quartical peel
        output_column = run_quartical_peel(
            hk=hk,
            config=config,
            field=field,
            source_idx=idx,
            model_column=model_column,
            input_column=current_input_column,
            ms_path=ms_path,
            logger=logger,
            whitelist=whitelist
        )
        
        if not output_column:
            logger.error(f"Quartical peel failed for source {idx}, stopping peeling")
            break
        
        # Update for next iteration
        current_input_column = output_column
        final_output_column = output_column
        
        logger.info(f"Source {idx+1} peeled. Output column: {output_column}")
    
    if final_output_column:
        logger.success(f"Peeling complete. Final column: {final_output_column}")
    else:
        logger.warning("Peeling did not complete successfully")
    
    return final_output_column
