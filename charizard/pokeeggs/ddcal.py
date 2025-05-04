import os
import time
from .utils import *
from .rfi_remover import * 


def calculate_job_resources(config, job_type):
    """Calculate required job resources based on job type."""
    max_ppn = config['general']['max_ppn']
    job_types = {
        'calibration': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
        'applycal': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
        'imaging': {'nodes': 2, 'ppn': 16, 'walltime': "12:00:00"},
        'ddcal': {'nodes': config['general']['nodes'], 'ppn': max_ppn, 'walltime': "12:00:00"}
    }
    return job_types.get(job_type, {'nodes': 1, 'ppn': 12, 'walltime': "10:30:00"})

def call_wsclean(msname, config, tracker, logger, niter, datacolumn='corrected', prefix='', save_source_list=False, section='selfcal'):
    """Submit a wsclean job."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    
    if section == 'ddcal':
        ms_list = [msname]
        imsize = config['pipeline']['ddcal']['imgsize']
        cellsize = config['pipeline']['ddcal']['cellsize']
    else:
        active_spws = tracker.get_active_spws()
        ms_list = [f'{spw}/{msname}' for spw in active_spws]
        imsize = config['pipeline']['imaging_with_debugging']['selfcal']['imsize']
        cellsize = config['pipeline']['imaging_with_debugging']['selfcal']['cellsize']

    if not ms_list:
        logger.error(f"No MS files found for imaging")
        return None, None

    imagename = prefix if prefix else "wsclean"
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
    {' '.join(ms_list)}"""

    batch_file = f"{imagename}{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name=imagename,
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir=f"{imagename}.log",
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
    return (job_id, "all"), imagename

def combine_ms(config, logger, tracker):
    """Combine MS files from all SPWs."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'calibration')
    active_spws = tracker.get_active_spws()
    
    # Check if we have any SPWs to process
    if not active_spws:
        logger.error("No active SPWs found")
        return None
    
    # Determine which calibration to use (amp_phase or phase)
    if config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0) > 0:
        cal_num = config['pipeline']['imaging_with_debugging']['selfcal']['amp_phase_cal']
        final_cal = f"apcal{cal_num}.ms"
        logger.info(f"Using amp-phase calibrated MS: {final_cal}")
    else:
        cal_num = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
        final_cal = f"pcal{cal_num}.ms"
        logger.info(f"Using phase calibrated MS: {final_cal}")
    
    # Create list of MS files and verify they exist
    ms_list = []
    for spw in active_spws:
        ms_path = f"{spw}/{final_cal}"
        if os.path.exists(ms_path):
            ms_list.append(ms_path)
            logger.debug(f"Found MS file: {ms_path}")
        else:
            logger.error(f"MS file not found: {ms_path}")
            return None
    
    # Check if we found any valid MS files
    if not ms_list:
        logger.error("No valid MS files found to combine")
        return None
        
    output_ms = config['pipeline']['ddcal']['msname']
    logger.info(f"Will combine {len(ms_list)} MS files into {output_ms}")
    
    # Create CASA script with proper list initialization
    casa_script = f"""
vis_list = []
{chr(10).join([f"vis_list.append('{ms}')" for ms in ms_list])}
concat(vis=vis_list, concatvis='{output_ms}')
"""
    
    script_file = "combine_ms.py"
    batch_file = f"combine_ms{get_script_extension(scheduler)}"
    
    # Write CASA script
    try:
        with open(script_file, "w") as f:
            f.write(casa_script)
        logger.debug(f"Created CASA script: {script_file}")
    except Exception as e:
        logger.error(f"Failed to write CASA script: {str(e)}")
        return None
    
    # Create batch header
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name="combine_ms",
        nodes=job_resources['nodes'],
        ppn=job_resources['ppn'],
        walltime=job_resources['walltime'],
        output_dir="combine_ms.log",
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
        logger.debug(f"Created batch script: {batch_file}")
    except Exception as e:
        logger.error(f"Failed to write batch script: {str(e)}")
        return None
        
    # Submit job
    job_id = submit_job(batch_file, scheduler, logger)
    if job_id:
        logger.info(f"Submitted combine_ms job with ID: {job_id}")
    return job_id

def run_crystalball(msname, imagename, logger, config):
    """Run CrystalBall for source modeling."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'ddcal')
    
    crystalball_command = f"""crystalball {msname} \\
    -sm {imagename}-sources.txt \\
    -po \\
    -w ds9.reg \\
    -o bright_ext_source_column"""  # Removed -f flag and fixed flags syntax
    
    batch_file = f"crystalball{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name="crystalball",
        nodes=2,  # Fixed to match actual PBS script
        ppn=12,   # Fixed to match actual PBS script
        walltime=job_resources['walltime'],
        output_dir="crystalball.log",
        queue=config['general']['queue']
    )
    
    batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{crystalball_command}
"""
    
    with open(batch_file, "w") as f:
        f.write(batch_content)
        
    job_id = submit_job(batch_file, scheduler, logger)
    return job_id


def run_quartical(msname, logger, config):
    """Run QuartiCal for DD calibration."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'ddcal')
    
    quartical_command = f"""goquartical \\
    input_ms.path={msname} \\
    input_ms.data_column=DATA \\
    input_ms.time_chunk='300s' \\
    input_ms.freq_chunk='0' \\
    input_model.recipe=MODEL_DATA~bright_ext_source_column:bright_ext_source_column \\
    solver.terms='[G,dE]' \\
    solver.iter_recipe='[25,25,10,10]' \\
    output.products=[corrected_data,corrected_residual,corrected_weight] \\
    output.columns=[CORRECTED_DATA,SUBDD_DATA_bright_ext_q,WEIGHT_SPECTRUM] \\
    G.type=diag_complex \\
    G.time_interval='60s' \\
    G.freq_interval='10' \\
    dE.time_interval='60' \\
    dE.freq_interval='60' \\
    dE.type=complex \\
    dE.direction_dependent=true"""
    
    batch_file = f"quartical{get_script_extension(scheduler)}"
    batch_header = create_batch_header(
        scheduler_type=scheduler,
        job_name="quartical",
        nodes=2,   # Fixed to match actual PBS script
        ppn=16,    # Fixed to match actual PBS script
        walltime=job_resources['walltime'],
        output_dir="quartical.log",  
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
    return job_id

def direction_dependent_calibration(config, logger, tracker):
    """Execute DD calibration pipeline."""
    if not config['pipeline']['ddcal'].get('doit', False):
        logger.info("DD calibration not requested")
        return True

    scheduler = config['general']['PBS_or_SLURM']
    
    # Step 1: Combine MS files
    logger.info("Combining MS files from all SPWs")
    combine_job = combine_ms(config, logger, tracker)
    if not combine_job:
        logger.error("Failed to submit MS combination job")
        return False
        
    tracker.add_jobs('combine_ms', [(combine_job, 'all')])
    combine_successful, combine_failed = wait_for_combine_ms_job(
        combine_job,  # Changed: no longer passing tuple, just job_id
        config['general']['working_directory'],
        logger,
        scheduler
    )
    
    if not tracker.check_brotherhood(combine_failed):
        logger.error("MS combination failed")
        cleanup_and_exit([combine_job], scheduler, logger)
        return False
            
    # Step 2: Initial deep imaging
    logger.info("Running initial deep imaging")
    job_img, img_prefix = call_wsclean(
        config['pipeline']['ddcal']['msname'],
        config,
        tracker,
        logger,
        50000,
        'DATA',
        'initial_img',
        save_source_list=True,
        section='ddcal'
    )
    
    if not job_img:
        logger.error("Failed to submit initial imaging job")
        return False
        
    tracker.add_jobs('initial_imaging', [job_img])
    img_successful, img_failed = wait_for_wsclean_job(
        job_img,
        config['general']['working_directory'],
        logger,
        'initial_img',
        scheduler
    )
    
    if not tracker.check_brotherhood(img_failed):
        logger.error("Initial imaging failed")
        cleanup_and_exit([job_img[0]], scheduler, logger)
        return False
            
    # Step 3: Run CrystalBall
    logger.info("Running CrystalBall for source modeling")
    crystalball_job = run_crystalball(
        config['pipeline']['ddcal']['msname'],
        'initial_img',
        logger,
        config
    )
    
    if not crystalball_job:
        logger.error("Failed to submit CrystalBall job")
        return False
        
    tracker.add_jobs('crystalball', [(crystalball_job, 'all')])
    crystal_successful, crystal_failed = wait_for_wsclean_job(  # Changed: using specialized wait function
        crystalball_job,
        config['general']['working_directory'],
        logger,
        scheduler
    )
    
    if not tracker.check_brotherhood(crystal_failed):
        logger.error("CrystalBall modeling failed")
        cleanup_and_exit([crystalball_job], scheduler, logger)
        return False
            
    # Step 4: Run QuartiCal
    logger.info("Running QuartiCal for DD calibration")
    quartical_job = run_quartical(
        config['pipeline']['ddcal']['msname'],
        logger,
        config
    )
    
    if not quartical_job:
        logger.error("Failed to submit QuartiCal job")
        return False
        
    tracker.add_jobs('quartical', [(quartical_job, 'all')])
    quartical_successful, quartical_failed = wait_for_wsclean_job(  # Changed: using specialized wait function
        quartical_job,
        config['general']['working_directory'],
        logger,
        scheduler
    )
    
    if not tracker.check_brotherhood(quartical_failed):
        logger.error("QuartiCal calibration failed")
        cleanup_and_exit([quartical_job], scheduler, logger)
        return False
            
    # Step 5: Final imaging
    logger.info("Running final imaging after DD calibration")
    job_final, final_prefix = call_wsclean(
        config['pipeline']['ddcal']['msname'],
        config,
        tracker,
        logger,
        50000,
        'SUBDD_DATA_bright_ext_q',
        'ddcal1',
        section='ddcal'
    )
    
    if not job_final:
        logger.error("Failed to submit final imaging job")
        return False
        
    tracker.add_jobs('final_imaging', [job_final])
    final_successful, final_failed = wait_for_wsclean_job(
        job_final,
        config['general']['working_directory'],
        logger,
        'ddcal1',
        scheduler
    )
    
    if not tracker.check_brotherhood(final_failed):
        logger.error("Final imaging failed")
        cleanup_and_exit([job_final[0]], scheduler, logger)
        return False
            
    logger.info("DD calibration completed successfully")
    return True