import os
import time
from .utils import *
from .rfi_remover import * 


def calculate_job_resources(config, job_type):
    """Calculate required job resources based on job type."""
    max_ppn = config['general']['max_ppn']
    
    # Use job type if it exists, otherwise use default
    if job_type in config['resources']:
        resource = config['resources'][job_type]
    else:
        resource = config['resources']['default']
    
    # Apply max_ppn limit
    ppn = min(resource.get('ppn', 1), max_ppn)
    
    return {
        'nodes': resource.get('nodes', 1),
        'ppn': ppn,
        'walltime': resource.get('walltime', "00:30:00")
    }


def combine_ms_for_field(config, logger, tracker, field):
    """Combine MS files from all SPWs for a specific field."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'calibration')
    active_spws = tracker.get_active_spws()
    
    # Check if we have any SPWs to process
    if not active_spws:
        logger.error(f"No active SPWs found for field {field}")
        return None
    
    # Determine which calibration to use (amp_phase or phase)
    if config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0) > 0:
        cal_num = config['pipeline']['imaging_with_debugging']['selfcal']['amp_phase_cal']
        final_cal = f"apcal{cal_num}.ms"
        logger.info(f"Using amp-phase calibrated MS: {final_cal} for field {field}")
    else:
        cal_num = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
        final_cal = f"pcal{cal_num}.ms"
        logger.info(f"Using phase calibrated MS: {final_cal} for field {field}")
    
    # Create list of MS files and verify they exist
    ms_list = []
    for spw in active_spws:
        ms_path = f"{spw}/{field}/{final_cal}"
        if os.path.exists(ms_path):
            ms_list.append(ms_path)
            logger.debug(f"Found MS file for field {field}: {ms_path}")
        else:
            logger.error(f"MS file not found for field {field}: {ms_path}")
            return None
    
    # Check if we found any valid MS files
    if not ms_list:
        logger.error(f"No valid MS files found to combine for field {field}")
        return None
    
    # Create field-specific output directory
    field_output_dir = f"ddcal_output/{field}"
    os.makedirs(field_output_dir, exist_ok=True)
    os.makedirs(f"{field_output_dir}/logs", exist_ok=True)
    
    output_ms = f"{field_output_dir}/combined_{field}.ms"
    logger.info(f"Will combine {len(ms_list)} MS files into {output_ms} for field {field}")
    
    # Create CASA script with proper list initialization
    casa_script = f"""
vis_list = []
{chr(10).join([f"vis_list.append('{ms}')" for ms in ms_list])}
concat(vis=vis_list, concatvis='{output_ms}')
"""
    
    script_file = f"combine_{field}.py"
    batch_file = f"combine_{field}{get_script_extension(scheduler)}"
    
    # Write CASA script
    try:
        with open(script_file, "w") as f:
            f.write(casa_script)
        logger.debug(f"Created CASA script for field {field}: {script_file}")
    except Exception as e:
        logger.error(f"Failed to write CASA script for field {field}: {str(e)}")
        return None
    
    # Create batch header
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=f"combine_{field}",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{field_output_dir}/logs/combine_{field}.log",
        queue=config['general']['queue']
    )
    
    # Create and write batch script
    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
    
    try:
        with open(batch_file, "w") as f:
            f.write(batch_content)
        logger.debug(f"Created batch script for field {field}: {batch_file}")
    except Exception as e:
        logger.error(f"Failed to write batch script for field {field}: {str(e)}")
        return None
        
    # Submit job
    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted combine_ms job for field {field} with ID: {job_id}")
    return job_id


def call_wsclean_field(field, msname, config, logger, niter, datacolumn='DATA', prefix='', save_source_list=False):
    """Submit a wsclean job for a specific field."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    
    field_output_dir = f"ddcal_output/{field}"
    ms_path = f"{field_output_dir}/{msname}"
    
    if not os.path.exists(ms_path):
        logger.error(f"MS file not found for field {field}: {ms_path}")
        return None, None

    imagename = f"{field_output_dir}/{prefix}_{field}" if prefix else f"{field_output_dir}/wsclean_{field}"
    
    # Get image size and cell size from config
    imsize = config['pipeline']['ddcal']['imgsize']
    cellsize = config['pipeline']['ddcal']['cellsize']
    
    wsclean_command = f"""wsclean \\
    -name {imagename} \\
    -weight briggs 0.0 \\
    -super-weight 1.0 \\
    -weighting-rank-filter-size 16 \\
    -taper-gaussian 0 \\
    -size {imsize} {imsize} \\
    -scale {cellsize} \\
    -channels-out 4 \\
    -wstack-grid-mode kb \\
    -wstack-kernel-size 7 \\
    -wstack-oversampling 63 \\
    -pol I \\
    -intervals-out 1 \\
    -data-column {datacolumn} \\
    -niter {niter} \\
    -auto-mask 5 \\
    -auto-threshold 0.05 \\
    -gain 0.1 \\
    -mgain 0.9 \\
    -join-channels \\
    -multiscale-scale-bias 0.6 \\
    -fit-spectral-pol 3 \\
    -fit-beam \\
    -elliptical-beam \\
    -padding 1.3 \\
    -parallel-deconvolution 8192 \\
    {'-save-source-list' if save_source_list else ''} \\
    {ms_path}"""

    batch_file = f"wsclean_{field}_{prefix}{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=f"wsclean_{field}_{prefix}",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{field_output_dir}/logs/wsclean_{prefix}_{field}.log",
        queue=config['general']['queue']
    )

    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{wsclean_command}
"""
    with open(batch_file, "w") as f:
        f.write(batch_content)

    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted WSClean job for field {field} with job ID {job_id}")
    return (job_id, field), imagename


def run_crystalball_field(field, imagename, region_file, logger, config):
    """Run CrystalBall for source modeling for a specific field."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    
    field_output_dir = f"ddcal_output/{field}"
    ms_path = f"{field_output_dir}/combined_{field}.ms"
    
    # Copy region file to field directory
    field_region_file = f"{field_output_dir}/{os.path.basename(region_file)}"
    if not os.path.exists(field_region_file):
        try:
            import shutil
            shutil.copy2(region_file, field_region_file)
            logger.info(f"Copied region file for field {field}: {field_region_file}")
        except Exception as e:
            logger.error(f"Failed to copy region file for field {field}: {str(e)}")
            return None
    
    # Get CrystalBall options from config
    # cb_options = config['pipeline']['ddcal']['peeling']['source_modelling'].get('crystalball_options', '-po')
    
    crystalball_command = f"""crystalball {ms_path} \\
    -sm {imagename}-sources.txt \\
    -w {field_region_file} \\
    -o bright_ext_source_column"""
    
    batch_file = f"crystalball_{field}{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=f"crystalball_{field}",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{field_output_dir}/logs/crystalball_{field}.log",
        queue=config['general']['queue']
    )
    
    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
micromamba activate cubical_env
{crystalball_command}
"""
    
    with open(batch_file, "w") as f:
        f.write(batch_content)
        
    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted CrystalBall job for field {field} with job ID {job_id}")
    return job_id


def run_quartical_delay_selfcal(field, logger, config):
    """Run QuartiCal delay self-calibration for a specific field."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'self_calibration')
    
    field_output_dir = f"ddcal_output/{field}"
    ms_path = f"{field_output_dir}/combined_{field}.ms"
    
    # Get delay selfcal parameters from config
    delay_config = config['pipeline']['ddcal']['delay_selfcal']
    
    quartical_command = f"""goquartical \\
    input_ms.path={ms_path} \\
    input_ms.data_column=DATA \\
    input_ms.time_chunk='0' \\
    input_ms.freq_chunk='0' \\
    input_model.recipe=MODEL_DATA \\
    solver.terms={delay_config['solver_terms']} \\
    solver.iter_recipe={delay_config['solver_iter_recipe']} \\
    solver.propagate_flags={str(delay_config['solver_propagate_flags']).lower()} \\
    solver.robust={str(delay_config['solver_robust']).lower()} \\
    solver.threads={delay_config['solver_threads']} \\
    solver.convergence_fraction={delay_config['solver_convergence_fraction']} \\
    solver.convergence_criteria={delay_config['solver_convergence_criteria']} \\
    output.log_directory={field_output_dir}/quartical_delay_logs \\
    output.gain_directory={field_output_dir}/quartical_delay_gains \\
    output.overwrite=1 \\
    output.products=[corrected_data,corrected_residual] \\
    output.columns=[CORRECTED_DATA,CORRECTED_RESIDUAL] \\
    output.flags=False \\
    dask.threads={delay_config['dask_threads']} \\
    dask.workers={delay_config['dask_workers']} \\
    dask.scheduler={delay_config['dask_scheduler']} \\
    G.type={delay_config['G_type']} \\
    G.time_interval={delay_config['G_time_interval']} \\
    G.freq_interval={delay_config['G_freq_interval']} \\
    G.initial_estimate={str(delay_config['G_initial_estimate']).lower()} \\
    G.solve_per={delay_config['G_solve_per']} \\
    G.interp_mode={delay_config['G_interp_mode']} \\
    G.interp_method={delay_config['G_interp_method']} \\
    mad_flags.enable={str(delay_config['mad_flags_enable']).lower()} \\
    mad_flags.threshold_bl={delay_config['mad_flags_threshold_bl']} \\
    mad_flags.threshold_global={delay_config['mad_flags_threshold_global']} \\
    mad_flags.max_deviation={delay_config['mad_flags_max_deviation']} \\
    K.time_interval={delay_config['K_time_interval']} \\
    K.freq_interval={delay_config['K_freq_interval']} \\
    K.type={delay_config['K_type']} \\
    K.initial_estimate={str(delay_config['K_initial_estimate']).lower()} \\
    K.interp_mode={delay_config['K_interp_mode']} \\
    K.interp_method={delay_config['K_interp_method']}"""
    
    batch_file = f"quartical_delay_{field}{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=f"quartical_delay_{field}",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{field_output_dir}/logs/quartical_delay_{field}.log",
        queue=config['general']['queue']
    )
    
    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{quartical_command}
"""
    
    with open(batch_file, "w") as f:
        f.write(batch_content)
        
    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted QuartiCal delay selfcal job for field {field} with job ID {job_id}")
    return job_id


def run_quartical_ddcal(field, logger, config):
    """Run QuartiCal direction-dependent calibration for a specific field."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    
    field_output_dir = f"ddcal_output/{field}"
    ms_path = f"{field_output_dir}/combined_{field}.ms"
    
    # Get peeling gains parameters from config
    peeling_config = config['pipeline']['ddcal']['peeling']['peeling_gains']
    
    quartical_command = f"""goquartical \\
    input_ms.path={ms_path} \\
    input_ms.data_column=CORRECTED_DATA \\
    input_ms.time_chunk={peeling_config['time_chunk']} \\
    input_ms.freq_chunk={peeling_config['freq_chunk']} \\
    input_model.recipe=MODEL_DATA~bright_ext_source_column:bright_ext_source_column \\
    solver.terms={peeling_config['solver_terms']} \\
    solver.iter_recipe={peeling_config['solver_iter_recipe']} \\
    solver.robust={str(peeling_config['solver_robust']).lower()} \\
    solver.propagate_flags={str(peeling_config['solver_propagate_flags']).lower()} \\
    solver.threads={peeling_config['solver_threads']} \\
    solver.convergence_fraction={peeling_config['solver_convergence_fraction']} \\
    solver.convergence_criteria={peeling_config['solver_convergence_criteria']} \\
    output.log_directory={field_output_dir}/quartical_dd_logs \\
    output.gain_directory={field_output_dir}/quartical_dd_gains \\
    output.log_to_terminal=True \\
    output.overwrite=True \\
    output.products=[corrected_data,corrected_residual,corrected_weight] \\
    output.columns=[CORRECTED_DATA,{peeling_config['output_column']},WEIGHT_SPECTRUM] \\
    output.flags=False \\
    dask.threads={peeling_config['dask_threads']} \\
    dask.workers={peeling_config['dask_workers']} \\
    dask.scheduler={peeling_config['dask_scheduler']} \\
    G.type={peeling_config['G_type']} \\
    G.solve_per={peeling_config['G_solve_per']} \\
    G.time_interval={peeling_config['G_time_interval']} \\
    G.freq_interval={peeling_config['G_freq_interval']} \\
    G.initial_estimate={str(peeling_config['G_initial_estimate']).lower()} \\
    G.interp_mode={peeling_config['G_interp_mode']} \\
    G.interp_method={peeling_config['G_interp_method']} \\
    G.respect_scan_boundaries={str(peeling_config['G_respect_scan_boundaries']).lower()} \\
    dE.direction_dependent={str(peeling_config['dE_direction_dependent']).lower()} \\
    dE.type={peeling_config['dE_type']} \\
    dE.time_interval={peeling_config['dE_time_interval']} \\
    dE.freq_interval={peeling_config['dE_freq_interval']} \\
    mad_flags.enable={str(peeling_config['mad_flags_enable']).lower()} \\
    mad_flags.threshold_bl={peeling_config['mad_flags_threshold_bl']} \\
    mad_flags.threshold_global={peeling_config['mad_flags_threshold_global']} \\
    mad_flags.max_deviation={peeling_config['mad_flags_max_deviation']}"""
    
    batch_file = f"quartical_ddcal_{field}{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=f"quartical_ddcal_{field}",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{field_output_dir}/logs/quartical_ddcal_{field}.log",
        queue=config['general']['queue']
    )
    
    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{quartical_command}
"""
    
    with open(batch_file, "w") as f:
        f.write(batch_content)
        
    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted QuartiCal DD calibration job for field {field} with job ID {job_id}")
    return job_id


def direction_dependent_calibration(config, logger, tracker):
    """Execute field-wise DD calibration pipeline."""
    if not config['pipeline']['ddcal'].get('doit', False):
        logger.info("DD calibration not requested")
        return True

    # Get field list and their corresponding region files
    regions_config = config['pipeline']['ddcal'].get('regions', {})
    if not regions_config:
        logger.error("No regions specified in ddcal configuration")
        return False
    
    field_list = list(regions_config.keys())
    logger.info(f"Starting DD calibration for fields: {field_list}")
    
    scheduler = config['general']['PBS_or_SLURM']
    steps = config['pipeline']['ddcal']['steps']
    
    # Step 1: Combine MS files for each field
    if steps['combine_ms']:
        logger.info("Combining MS files from all SPWs for each field")
        combine_jobs = []
        
        for field in field_list:
            combine_job = combine_ms_for_field(config, logger, tracker, field)
            if combine_job:
                combine_jobs.append((combine_job, field))
                tracker.add_jobs(f'combine_{field}', [(combine_job, field)])
            time.sleep(5)
        
        if not combine_jobs:
            logger.error("No MS combination jobs submitted")
            return False
        
        # Wait for combine jobs to finish using utils functions
        combine_successful, combine_failed = wait_for_ddcal_jobs(
            combine_jobs,
            config['general']['working_directory'],
            logger,
            'combine',
            scheduler
        )
        
        if not tracker.check_brotherhood(combine_failed):
            logger.error(f"MS combination failed for fields: {combine_failed}")
            cleanup_and_exit([job_id for job_id, _ in combine_jobs], scheduler, logger)
            return False
    
    # Step 2: Initial deep imaging for each field
    if steps['initial_image']:
        logger.info("Running initial deep imaging for each field")
        imaging_jobs = []
        
        for field in field_list:
            job_img, img_prefix = call_wsclean_field(
                field,
                f"combined_{field}.ms",
                config,
                logger,
                50000,  # Hardcoded niter
                'DATA',  # Hardcoded datacolumn
                'initial_img',
                save_source_list=True  # Hardcoded save_source_list
            )
            
            if job_img:
                imaging_jobs.append(job_img)
                tracker.add_jobs(f'initial_imaging_{field}', [job_img])
            time.sleep(5)
        
        if not imaging_jobs:
            logger.error("No initial imaging jobs submitted")
            return False
        
        # Wait for imaging jobs to finish using utils functions
        img_successful, img_failed = wait_for_ddcal_wsclean_jobs(
            imaging_jobs,
            config['general']['working_directory'],
            logger,
            'initial_img',
            scheduler
        )
        
        if not tracker.check_brotherhood(img_failed):
            logger.error(f"Initial imaging failed for fields: {img_failed}")
            cleanup_and_exit([job_id for job_id, _ in imaging_jobs], scheduler, logger)
            return False
    else:
        # Check if source lists exist from previous runs
        for field in field_list:
            source_list_file = f"ddcal_output/{field}/initial_img_{field}-sources.txt"
            if not os.path.exists(source_list_file):
                logger.error(f"Source list not found for field {field}: {source_list_file}")
                logger.error("Please run initial_image step first or provide existing source list")
                return False
    
    # Step 3: Delay self-calibration for each field
    if steps['delay_selfcal']:
        logger.info("Running QuartiCal delay self-calibration for each field")
        delay_jobs = []
        
        for field in field_list:
            delay_job = run_quartical_delay_selfcal(field, logger, config)
            
            if delay_job:
                delay_jobs.append((delay_job, field))
                tracker.add_jobs(f'quartical_delay_{field}', [(delay_job, field)])
            time.sleep(5)
        
        if not delay_jobs:
            logger.error("No QuartiCal delay self-calibration jobs submitted")
            return False
        
        # Wait for delay self-calibration jobs to finish using utils functions
        delay_successful, delay_failed = wait_for_ddcal_jobs(
            delay_jobs,
            config['general']['working_directory'],
            logger,
            'quartical_delay',
            scheduler
        )
        
        if not tracker.check_brotherhood(delay_failed):
            logger.error(f"QuartiCal delay self-calibration failed for fields: {delay_failed}")
            cleanup_and_exit([job_id for job_id, _ in delay_jobs], scheduler, logger)
            return False
    
    # Step 4: Delay imaging for each field
    if steps['delay_imaging']:
        logger.info("Running delay self-calibrated imaging for each field")
        delay_imaging_jobs = []
        
        for field in field_list:
            delay_niter = config['pipeline']['ddcal']['delay_imaging']['niter']
            delay_save_source_list = config['pipeline']['ddcal']['delay_imaging']['save_source_list']
            
            job_delay_img, delay_img_prefix = call_wsclean_field(
                field,
                f"combined_{field}.ms",
                config,
                logger,
                delay_niter,
                'CORRECTED_DATA',  # Use delay self-calibrated data
                'delay_selfcal',
                save_source_list=delay_save_source_list
            )
            
            if job_delay_img:
                delay_imaging_jobs.append(job_delay_img)
                tracker.add_jobs(f'delay_imaging_{field}', [job_delay_img])
            time.sleep(5)
        
        if not delay_imaging_jobs:
            logger.error("No delay imaging jobs submitted")
            return False
        
        # Wait for delay imaging jobs to finish using utils functions
        delay_img_successful, delay_img_failed = wait_for_ddcal_wsclean_jobs(
            delay_imaging_jobs,
            config['general']['working_directory'],
            logger,
            'delay_selfcal',
            scheduler
        )
        
        if not tracker.check_brotherhood(delay_img_failed):
            logger.error(f"Delay imaging failed for fields: {delay_img_failed}")
            cleanup_and_exit([job_id for job_id, _ in delay_imaging_jobs], scheduler, logger)
            return False
    
    # Step 5: CrystalBall source modeling for each field
    if steps['peeling']['source_modelling']:
        logger.info("Running CrystalBall for source modeling for each field")
        crystalball_jobs = []
        
        for field in field_list:
            region_file = regions_config[field]
            if not os.path.exists(region_file):
                logger.error(f"Region file not found for field {field}: {region_file}")
                continue
                
            field_output_dir = f"ddcal_output/{field}"
            # Use delay self-calibrated image for source modeling
            imagename = f"{field_output_dir}/delay_selfcal_{field}"
            
            crystalball_job = run_crystalball_field(
                field, imagename, region_file, logger, config
            )
            
            if crystalball_job:
                crystalball_jobs.append((crystalball_job, field))
                tracker.add_jobs(f'crystalball_{field}', [(crystalball_job, field)])
            time.sleep(5)
        
        if not crystalball_jobs:
            logger.error("No CrystalBall jobs submitted")
            return False
        
        # Wait for CrystalBall jobs to finish using utils functions
        crystal_successful, crystal_failed = wait_for_ddcal_jobs(
            crystalball_jobs,
            config['general']['working_directory'],
            logger,
            'crystalball',
            scheduler
        )
        
        if not tracker.check_brotherhood(crystal_failed):
            logger.error(f"CrystalBall modeling failed for fields: {crystal_failed}")
            cleanup_and_exit([job_id for job_id, _ in crystalball_jobs], scheduler, logger)
            return False
    
    # Step 6: QuartiCal DD calibration for each field
    if steps['peeling']['peeling_gains']:
        logger.info("Running QuartiCal DD calibration for each field")
        ddcal_jobs = []
        
        for field in field_list:
            ddcal_job = run_quartical_ddcal(field, logger, config)
            
            if ddcal_job:
                ddcal_jobs.append((ddcal_job, field))
                tracker.add_jobs(f'quartical_ddcal_{field}', [(ddcal_job, field)])
            time.sleep(5)
        
        if not ddcal_jobs:
            logger.error("No QuartiCal DD calibration jobs submitted")
            return False
        
        # Wait for DD calibration jobs to finish using utils functions
        ddcal_successful, ddcal_failed = wait_for_ddcal_jobs(
            ddcal_jobs,
            config['general']['working_directory'],
            logger,
            'quartical_ddcal',
            scheduler
        )
        
        if not tracker.check_brotherhood(ddcal_failed):
            logger.error(f"QuartiCal DD calibration failed for fields: {ddcal_failed}")
            cleanup_and_exit([job_id for job_id, _ in ddcal_jobs], scheduler, logger)
            return False
    
    # Step 7: Final peeled imaging for each field
    if steps['peeling']['peeled_images']:
        logger.info("Running final peeled imaging for each field")
        final_imaging_jobs = []
        
        for field in field_list:
            peeled_niter = config['pipeline']['ddcal']['peeling']['peeled_images']['niter']
            peeled_datacolumn = config['pipeline']['ddcal']['peeling']['peeled_images']['datacolumn']
            
            job_final, final_prefix = call_wsclean_field(
                field,
                f"combined_{field}.ms",
                config,
                logger,
                peeled_niter,
                peeled_datacolumn,
                'peeled'
            )
            
            if job_final:
                final_imaging_jobs.append(job_final)
                tracker.add_jobs(f'peeled_imaging_{field}', [job_final])
            time.sleep(5)
        
        if not final_imaging_jobs:
            logger.error("No final peeled imaging jobs submitted")
            return False
        
        # Wait for final imaging jobs to finish using utils functions
        final_successful, final_failed = wait_for_ddcal_wsclean_jobs(
            final_imaging_jobs,
            config['general']['working_directory'],
            logger,
            'peeled',
            scheduler
        )
        
        if not tracker.check_brotherhood(final_failed):
            logger.error(f"Final peeled imaging failed for fields: {final_failed}")
            cleanup_and_exit([job_id for job_id, _ in final_imaging_jobs], scheduler, logger)
            return False
    
    logger.info(f"DD calibration completed successfully for all fields: {field_list}")
    return True