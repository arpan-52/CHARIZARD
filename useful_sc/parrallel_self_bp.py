
import os
import time
from .utils import *
from .rfi_remover import * 

def convert_jobs_to_tracker_format(job_info):
    """
    Convert job information from various formats to the tracker's expected format.
    
    Parameters:
    - job_info: List of job tuples, either (job_id, spw, field) or (job_id, spw)
    
    Returns:
    - List of (job_id, tracker_key) tuples compatible with the tracker
    """
    tracker_format_jobs = []
    for job_tuple in job_info:
        if len(job_tuple) == 3:
            job_id, spw, field = job_tuple
            # Create a compound key that combines spw and field
            tracker_format_jobs.append((job_id, f"{spw}_{field}"))
        else:
            # Already in the correct format
            tracker_format_jobs.append(job_tuple)
    return tracker_format_jobs



def calculate_job_resources(config, job_type):
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



def prepare_ms(config, logger, tracker):
    """Prepare measurement sets by splitting and flagging for all SPWs and fields."""
    #    if not config['pipeline']['imaging_with_debugging']['selfcal'].get('average_and_flag', False):
    #        if os.path.exists('pcal1.ms'):
    #            logger.info("Found split MS")
    #            return 'pcal1.ms'
    #        logger.error("No split MS found and splitting not requested")
    #        return None

    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.error("No active SPWs")
        return None

    # Extract fields from config
    field_list = config['msinfo']['source_list'].split(',')
    field_list = [field.strip() for field in field_list]
    logger.info(f"Preparing MS for fields: {field_list}")

    scheduler = config['general']['PBS_or_SLURM']
    job_split = []

    for spw in active_spws:
        # Create a multi-field CASA script with multiple mstransform commands in sequence
        casa_script = ""
        
        for field in field_list:
            # Create output directory for each field
            field_dir = f"{spw}/{field}"
            casa_script += f"""
os.makedirs('{field_dir}', exist_ok=True)
mstransform(vis='{spw}/src.ms',
    outputvis='{field_dir}/pcal1.ms',
    datacolumn='corrected',
    chanaverage=True,
    chanbin={config['pipeline']['imaging_with_debugging']['selfcal']['freqbin']},
    field='{field}')
    """

        script_file = f"split_{spw}.py"
        batch_file = f"split_{spw}{get_script_extension(scheduler)}"

        with open(script_file, "w") as f:
            f.write(casa_script)

        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"split_{spw}",
            nodes=1,
            ppn=2,
            walltime="10:00:00",
            output_dir=f"{spw}/split_{spw}.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
    cd {os.getcwd()}
    {config['general']['preamble']}
    {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
    """

        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        if job_id:
            job_split.append((job_id, spw))
            tracker.add_jobs('splitting', [(job_id, spw)])
        time.sleep(5)

    if not job_split:
        logger.error("No splitting jobs submitted")
        return None

    split_successful, split_failed = wait_for_jobs_to_finish(
        job_split,
        config['general']['working_directory'],
        logger,
        'split',
        scheduler
    )

    if not tracker.check_brotherhood(split_failed):
        logger.error(f"Splitting failed for: {split_failed}")
        cleanup_and_exit(job_split, scheduler, logger)
        return None

    if split_successful:
        # Get all field-specific MS files that need flagging
        field_list = config['msinfo']['source_list'].split(',')
        field_list = [field.strip() for field in field_list]
        ms_to_flag = []
        
        for spw in active_spws:
            for field in field_list:
                ms_path = f"{spw}/{field}/pcal1.ms"
                if os.path.exists(ms_path):
                    ms_to_flag.append(ms_path)
        
        logger.info(f"Flagging MS files: {ms_to_flag}")
        
        # First round of flagging
        job_flag = general_selfcal_flagger(
            ms_names=ms_to_flag,
            mode='tfcrop,rflag',
            tracker=tracker,
            logger=logger,
            config=config,
            t_sigma=4.0,
            f_sigma=4.0,
            datacolumn='DATA',
            prefix='avg'
        )
        
        if job_flag:
            # Convert job_flag to tracker format before passing to tracker
            tracker_jobs = convert_jobs_to_tracker_format(job_flag)
            tracker.add_jobs('flagging_avg', tracker_jobs)
            flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                job_flag,
                config['general']['working_directory'],
                logger,
                'flag_tfcrop_rflag_avg',
                scheduler,
                field_list
            )

            if not tracker.check_brotherhood(flag_failed):
                logger.error(f"First flagging round failed for: {flag_failed}")
                cleanup_and_exit([j[0] for j in job_flag], scheduler, logger)
                return None

            # Second round of flagging
            if flag_successful:
                job_flag = general_selfcal_flagger(
                    ms_names=ms_to_flag,
                    mode='tfcrop,rflag',
                    tracker=tracker,
                    logger=logger,
                    config=config,
                    t_sigma=4.0,
                    f_sigma=4.0,
                    datacolumn='DATA',
                    prefix='avg'
                )
                
                if job_flag:
                    tracker_jobs = convert_jobs_to_tracker_format(job_flag)
                    tracker.add_jobs('flagging_avg', tracker_jobs)
                    flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    job_flag,
                    config['general']['working_directory'],
                    logger,
                    'flag_tfcrop_rflag_avg',
                    scheduler,
                    field_list
                )

                    if not tracker.check_brotherhood(flag_failed):
                        logger.error(f"Second flagging round failed for: {flag_failed}")
                        cleanup_and_exit([j[0] for j in job_flag], scheduler, logger)
                        return None

                    if flag_successful:
                        logger.info("Successfully completed MS preparation")
                        # Return a dictionary mapping of fields to their MS paths
                        ms_map = {}
                        for spw in active_spws:
                            for field in field_list:
                                ms_path = f"{spw}/{field}/pcal1.ms"
                                if os.path.exists(ms_path):
                                    if field not in ms_map:
                                        ms_map[field] = []
                                    ms_map[field].append(ms_path)
                        return ms_map

def calibrate_ap(msname, output_ms, config, logger, solint, solname, calmode, tracker):
    """Submit calibration jobs for all SPWs and all fields at once."""
    logger.debug(f"Entering calibrate_ap with msname: {msname}, solname: {solname}")
    
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'self_calibration')
    active_spws = tracker.get_active_spws()
    job_cal = []
    
    # Get list of fields
    field_list = config['msinfo']['source_list'].split(',')
    field_list = [f.strip() for f in field_list]
    
    # Extract the generic prefix from solname (e.g., "pcal1" from "pcal1_J1120+0641")
    generic_prefix = solname.split('_')[0] if '_' in solname else solname
    
    logger.debug(f"Creating calibration jobs for SPWs: {active_spws} and fields: {field_list}")
    
    for spw in active_spws:
        for field in field_list:
            logger.debug(f"Processing SPW {spw}, field {field}")
            
            # Create field-specific selfcal-tables directory
            os.makedirs(f"{spw}/{field}/selfcal-tables", exist_ok=True)
            
            # Check if field-specific MS exists
            field_ms_path = f"{spw}/{field}/{msname}"
            if not os.path.exists(field_ms_path):
                logger.warning(f"MS not found for SPW {spw}, field {field}: {field_ms_path}")
                continue
            
            # Create field-specific solname for calibration tables
            field_solname = f"{generic_prefix}_{field}"
            
            casa_script = f"""
gaincal(vis='{spw}/{field}/{msname}',
       caltable='{spw}/{field}/selfcal-tables/{field_solname}.g',
       field='',
       spw='',
       solint='{solint}',
       refant='{config['pipeline']['imaging_with_debugging']['selfcal']['refant']}',
       minsnr=2.0,
       gaintype='G',
       calmode='{calmode}')

bandpass(vis='{spw}/{field}/{msname}',
       caltable='{spw}/{field}/selfcal-tables/{field_solname}.b',
       field='',
       spw='',
       solint='inf',
       refant='{config['pipeline']['imaging_with_debugging']['selfcal']['refant']}',
       minsnr=3.0,
       gaintable=['{spw}/{field}/selfcal-tables/{field_solname}.g'])

applycal(vis='{spw}/{field}/{msname}',
       gaintable=['{spw}/{field}/selfcal-tables/{field_solname}.g', 
                 '{spw}/{field}/selfcal-tables/{field_solname}.b'],
       applymode='calflag',
       flagbackup=True)

mstransform(vis='{spw}/{field}/{msname}',
          outputvis='{spw}/{field}/{output_ms}',
          datacolumn='corrected')
"""
            
            script_file = f"{field_solname}_{spw}.py"
            batch_file = f"{field_solname}_{spw}{get_script_extension(scheduler)}"
            
            logger.debug(f"Writing scripts for SPW {spw}, field {field}")
            with open(script_file, "w") as f:
                f.write(casa_script)
            
            # Ensure log file goes to field-specific directory with generic prefix
            batch_header = create_batch_header(
                scheduler_type=scheduler,
                job_name=f"{field_solname}_{spw}",
                nodes=job_resources['nodes'],
                ppn=job_resources['ppn'],
                walltime=job_resources['walltime'],
                output_dir=f"{spw}/{field}/{generic_prefix}.log",  # Use generic prefix for log
                queue=config['general']['queue']
            )
            
            batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
            
            with open(batch_file, "w") as f:
                f.write(batch_content)
            
            logger.debug(f"Submitting job for SPW {spw}, field {field}")
            job_id = submit_job(batch_file, scheduler, logger)
            if job_id:
                # Include field in job info
                job_cal.append((job_id, spw, field))
            time.sleep(5)
    
    logger.debug(f"Completed calibrate_ap, returning {len(job_cal)} jobs")
    return job_cal, solname


def call_wsclean(msname, config, tracker, logger, niter, field=None, datacolumn='corrected', prefix='', threshold=0.001):
    """Submit wsclean jobs for specific fields or all fields."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    active_spws = tracker.get_active_spws()
    job_ids = []
    
    # Get list of fields if not provided
    if field is None:
        field_list = config['msinfo']['source_list'].split(',')
        field_list = [f.strip() for f in field_list]
    else:
        field_list = [field]
    
    logger.info(f"Preparing WSClean jobs for fields: {field_list}")
    
    # Process each field separately
    for current_field in field_list:
        # Build list of MS files for this field
        ms_list = []
        for spw in active_spws:
            field_ms_path = f"{spw}/{current_field}/{msname}"
            if os.path.exists(field_ms_path):
                ms_list.append(field_ms_path)
        
        if not ms_list:
            logger.error(f"No {msname} found for field {current_field} in active SPWs")
            continue
        
        # Set field-specific image name
        field_imagename = f"{prefix}_{current_field}" if prefix else f"wsclean_{current_field}"
        
        # Create field-specific output directory
        field_output_dir = f"images/{current_field}"
        os.makedirs(field_output_dir, exist_ok=True)
        
        wsclean_command = f"""wsclean \\
    -name {field_output_dir}/{field_imagename} \\
    -weight briggs 0.0 \\
    -super-weight 1.0 \\
    -weighting-rank-filter-size 16 \\
    -taper-gaussian 0 \\
    -size {config['pipeline']['imaging_with_debugging']['selfcal']['imsize']} {config['pipeline']['imaging_with_debugging']['selfcal']['imsize']} \\
    -scale {config['pipeline']['imaging_with_debugging']['selfcal']['cellsize']} \\
    -channels-out 2 \\
    -wstack-grid-mode kb \\
    -wstack-kernel-size 7 \\
    -wstack-oversampling 63 \\
    -pol I \\
    -intervals-out 1 \\
    -data-column {datacolumn} \\
    -niter {niter} \\
    -auto-mask 7 \\
    -auto-threshold 3 \\
    -abs-threshold {threshold} \\
    -gain 0.1 \\
    -mgain 0.7 \\
    -join-channels \\
    -no-negative \\
    -multiscale-scale-bias 0.6 \\
    -fit-spectral-pol 3 \\
    -fit-beam \\
    -elliptical-beam \\
    -padding 1.3 \\
    -parallel-deconvolution 8192 \\
    {' '.join(ms_list)}"""

        batch_file = f"{field_imagename}{get_script_extension(scheduler)}"
        
        # Ensure log file is placed in field-specific directory
        log_path = f"{field_output_dir}/{field_imagename}.log"
        
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=field_imagename,
            nodes=job_resources['nodes'],
            ppn=job_resources['ppn'],
            walltime=job_resources['walltime'],
            output_dir=log_path,
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
            # Store job_id, field name for tracking
            job_ids.append((job_id, current_field))
            logger.info(f"Submitted WSClean job for field {current_field} with job ID {job_id}")
    
    if not job_ids:
        logger.error("No WSClean jobs were submitted")
        return None, None
    
    return job_ids, prefix




def get_solint_sequence(config):
    """Calculate the solint sequence based on configuration."""
    pcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
    apcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0)
    
    # Phase calibration sequence
    pcal_sequence = []
    if pcal_rounds > 0:
        pcal_sequence.append('4min')  # First round
    if pcal_rounds > 1:
        pcal_sequence.append('2min')  # Second round
    if pcal_rounds > 2:
        pcal_sequence.append('2min')  # Third round
    # Remaining rounds use 1min
    pcal_sequence.extend(['1min'] * (pcal_rounds - len(pcal_sequence)))
    
    # Amp-phase calibration sequence
    apcal_sequence = []
    if apcal_rounds > 0:
        apcal_sequence.append('2min')  # First round
    # Remaining rounds use 1min
    apcal_sequence.extend(['1min'] * (apcal_rounds - 1))
    
    return pcal_sequence + apcal_sequence

def self_calibration(config, logger, tracker):
    """Execute self-calibration pipeline with proper job tracking for multiple fields in parallel."""
    if 'selfcal' not in config['pipeline']['imaging_with_debugging']:
        logger.info("Self-calibration not requested")
        return True
    
    logger.info(f"Starting self-calibration for all fields")
    
    # Prepare MS files - returns a dictionary of fields to their MS paths
    ms_map = prepare_ms(config, logger, tracker)
    if not ms_map:
        logger.error(f"Failed to prepare MS files for self-calibration")
        return False
    
    # Get field list
    field_list = list(ms_map.keys())
    logger.info(f"Processing fields: {field_list}")

    # Get solint sequence for all fields
    solint_sequence = get_solint_sequence(config)
    logger.info(f"Using solint sequence: {solint_sequence}")

    scheduler = config['general']['PBS_or_SLURM']
    threshold = float(config['pipeline']['imaging_with_debugging']['selfcal']['threshold_to_clean'].replace('mJy','')) * 0.001
    niter = config['pipeline']['imaging_with_debugging']['selfcal']['iterations_to_start']

    # Initial dirty image if requested
    if config['pipeline']['imaging_with_debugging'].get('dirty', False):
        logger.info(f"Creating dirty images for fields: {field_list}")
        
        # Track all field MS paths for imaging
        all_ms_paths = []
        for field, ms_paths in ms_map.items():
            all_ms_paths.extend(ms_paths)
        
        # Call wsclean for all fields
        job_ids, prefix = call_wsclean(
            'pcal1.ms',
            config,
            tracker,
            logger,
            0,
            field=None,  # Process all fields
            datacolumn='DATA',
            prefix='dirty',
            threshold=threshold
        )
        
        if job_ids:
            # Add jobs to tracker with their respective fields
            for job_id, field in job_ids:
                tracker_job = convert_jobs_to_tracker_format([(job_id, field)])
                tracker.add_jobs(f'dirty_image_{field}', tracker_job)
                    
            # Wait for all dirty imaging jobs to complete
            dirty_successful, dirty_failed = wait_for_wsclean_jobs(
                job_ids,
                config['general']['working_directory'],
                logger,
                'dirty',
                scheduler
            )
            
            if not tracker.check_brotherhood(dirty_failed):
                logger.error(f"Dirty imaging failed for fields: {dirty_failed}")
                # Extract just the job IDs for cleanup
                job_id_list = [job_id for job_id, _ in job_ids]
                cleanup_and_exit(job_id_list, scheduler, logger)
                return False
    
    # Initialize MS tracking dictionary for all fields
    current_ms_map = ms_map.copy()  # Start with initial MS mapping
    
    # Phase calibration rounds
    for i in range(config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']):
        logger.info(f"Starting phase calibration round {i+1}")
        
        # Next MS name (for output)
        next_ms = f'pcal{i+2}.ms'
        
        # Use appropriate solint from sequence
        current_solint = solint_sequence[i]
        logger.info(f"Using solint: {current_solint} for phase cal round {i+1}")

        # Run imaging for all fields
        job_img, img_prefix = call_wsclean(
            'pcal1.ms' if i == 0 else f'pcal{i+1}.ms',
            config,
            tracker,
            logger,
            niter * (2 if i > 0 else 1),
            field=None,  # Process all fields
            datacolumn='DATA',
            prefix=f'selfcal_p{i}',
            threshold=threshold
        )
        
        if job_img:
            # Track all imaging jobs
            tracker_jobs = convert_jobs_to_tracker_format(job_img)
            tracker.add_jobs(f'image_pcal_{i+1}', tracker_jobs)
            
            img_successful, img_failed = wait_for_wsclean_jobs(
                job_img,
                config['general']['working_directory'],
                logger,
                f'selfcal_p{i}',
                scheduler
            )
            
            if not tracker.check_brotherhood(img_failed):
                logger.error(f"Imaging after phase cal round {i+1} failed")
                job_id_list = [job_id for job_id, _ in job_img]
                cleanup_and_exit(job_id_list, scheduler, logger)
                return False

        # Flag residuals if requested
        if config['pipeline']['imaging_with_debugging']['selfcal'].get('flag_residual', False):
            logger.info(f"Flagging residuals for phase cal round {i+1}")
            
            # Collect all MS paths from all fields
            all_ms_paths = []
            for field, ms_paths in current_ms_map.items():
                all_ms_paths.extend(ms_paths)
            
            flag_jobs1 = general_selfcal_flagger(
                ms_names=all_ms_paths,
                mode='rflag',
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='RESIDUAL',
                prefix=f'pcal{i+1}'
            )
            
            if flag_jobs1:
                tracker_jobs = convert_jobs_to_tracker_format(flag_jobs1)
                tracker.add_jobs(f'flag_pcal_{i+1}', tracker_jobs)
                # Keep using field-specific job waiting for the flaggers
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_pcal{i+1}',
                    scheduler
                )
                
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"RFLAG after phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs1], scheduler, logger)
                    return False
            
            flag_jobs2 = nami_selfcal_flagger(
                ms_names=all_ms_paths,
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='DATA',
                method='poly',
                degree=3,it=2,sigma=3,timebin=10,ncpu=8,
                prefix=f'pcal{i+1}'
            )
            
            if flag_jobs2:
                tracker_jobs = convert_jobs_to_tracker_format(flag_jobs2)
                tracker.add_jobs(f'flag_pcal_{i+1}', tracker_jobs)
                # Keep using field-specific job waiting for the flaggers
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_pcal{i+1}',
                    scheduler
                )
                
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"NAMI flagging after phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs2], scheduler, logger)
                    return False

        # Run calibration once for all fields
        job_cal, solname = calibrate_ap(
            'pcal1.ms' if i == 0 else f'pcal{i+1}.ms',
            next_ms, 
            config, 
            logger, 
            current_solint, 
            f'pcal{i+1}', 
            'p', 
            tracker
        )
        
        if job_cal:
            tracker_jobs = convert_jobs_to_tracker_format(job_cal)
            tracker.add_jobs(f'pcal_{i+1}', tracker_jobs)
            # Keep using field-specific job waiting for calibration
            cal_successful, cal_failed = wait_for_field_jobs_to_finish(
                job_cal,
                config['general']['working_directory'],
                logger,
                f'pcal{i+1}',
                scheduler
            )
            
            if not tracker.check_brotherhood(cal_failed):
                logger.error(f"Phase calibration round {i+1} failed")
                cleanup_and_exit([j[0] for j in job_cal], scheduler, logger)
                return False
        
        # Update MS map with next MS files for all fields
        for field in field_list:
            new_ms_list = []
            for ms_path in current_ms_map[field]:
                # Extract SPW and field
                path_parts = ms_path.split('/')
                if len(path_parts) >= 2:
                    spw = path_parts[0]
                    field_name = path_parts[1]
                    new_ms_path = f"{spw}/{field_name}/{next_ms}"
                    new_ms_list.append(new_ms_path)
            
            # Update the current MS map for this field
            current_ms_map[field] = new_ms_list
        
        # Update threshold and niter for next round
        threshold /= 1.5
        niter *= 2

    # Amplitude-phase calibration rounds
    pcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
    apcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0)
    
    for i in range(apcal_rounds):
        logger.info(f"Starting amp-phase calibration round {i+1}")
        
        # Next MS name (for output)
        next_ms = f'apcal{i+1}.ms'
        
        # Use appropriate solint from sequence
        current_solint = solint_sequence[pcal_rounds + i]
        logger.info(f"Using solint: {current_solint} for amp-phase cal round {i+1}")

        # Run imaging for all fields
        job_img, img_prefix = call_wsclean(
            f'pcal{pcal_rounds+1}.ms' if i == 0 else f'apcal{i}.ms',
            config,
            tracker,
            logger,
            niter,
            field=None,
            datacolumn='DATA',
            prefix=f'selfcal_ap{i}',
            threshold=threshold
        )
        
        if job_img:
            tracker_jobs = convert_jobs_to_tracker_format(job_img)
            tracker.add_jobs(f'image_apcal_{i+1}', tracker_jobs)
            img_successful, img_failed = wait_for_wsclean_jobs(
                job_img,
                config['general']['working_directory'],
                logger,
                f'selfcal_ap{i}',
                scheduler
            )
            
            if not tracker.check_brotherhood(img_failed):
                logger.error(f"Imaging after amp-phase cal round {i+1} failed")
                job_id_list = [job_id for job_id, _ in job_img]
                cleanup_and_exit(job_id_list, scheduler, logger)
                return False

        # Flag residuals if requested
        if config['pipeline']['imaging_with_debugging']['selfcal'].get('flag_residual', False):
            logger.info(f"Flagging residuals for amp-phase cal round {i+1}")
            
            # Collect all MS paths from all fields
            all_ms_paths = []
            for field, ms_paths in current_ms_map.items():
                all_ms_paths.extend(ms_paths)
            
            flag_jobs1 = general_selfcal_flagger(
                ms_names=all_ms_paths,
                mode='rflag',
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='RESIDUAL',
                prefix=f'apcal{i+1}'
            )
            
            if flag_jobs1:
                tracker_jobs = convert_jobs_to_tracker_format(flag_jobs1)
                tracker.add_jobs(f'flag_apcal_{i+1}', tracker_jobs)
                # Keep using field-specific job waiting for the flaggers
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_apcal{i+1}',
                    scheduler
                )
                
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"RFLAG after amp-phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs1], scheduler, logger)
                    return False
            
            flag_jobs2 = nami_selfcal_flagger(
                ms_names=all_ms_paths,
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='DATA',
                method='poly',
                degree=3,it=2,sigma=3,timebin=10,ncpu=8,
                prefix=f'apcal{i+1}'
            )
            
            if flag_jobs2:
                tracker_jobs = convert_jobs_to_tracker_format(flag_jobs2)
                tracker.add_jobs(f'flag_apcal_{i+1}', tracker_jobs)
                # Keep using field-specific job waiting for the flaggers
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_apcal{i+1}',
                    scheduler
                )
                
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"NAMI flagging after amp-phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs2], scheduler, logger)
                    return False

        # Run calibration once for all fields
        job_cal, solname = calibrate_ap(
            f'pcal{pcal_rounds+1}.ms' if i == 0 else f'apcal{i}.ms',
            next_ms,
            config,
            logger,
            current_solint,
            f'apcal{i+1}',
            'ap',
            tracker
        )
        
        if job_cal:
            tracker_jobs = convert_jobs_to_tracker_format(job_cal)
            tracker.add_jobs(f'apcal_{i+1}', tracker_jobs)
            # Keep using field-specific job waiting for calibration
            cal_successful, cal_failed = wait_for_field_jobs_to_finish(
                job_cal,
                config['general']['working_directory'],
                logger,
                f'apcal{i+1}',
                scheduler
            )
            
            if not tracker.check_brotherhood(cal_failed):
                logger.error(f"Amp-phase calibration round {i+1} failed")
                cleanup_and_exit([j[0] for j in job_cal], scheduler, logger)
                return False
        
        # Update MS map with next MS files for all fields
        for field in field_list:
            new_ms_list = []
            for ms_path in current_ms_map[field]:
                # Extract SPW and field
                path_parts = ms_path.split('/')
                if len(path_parts) >= 2:
                    spw = path_parts[0]
                    field_name = path_parts[1]
                    new_ms_path = f"{spw}/{field_name}/{next_ms}"
                    new_ms_list.append(new_ms_path)
            
            # Update the current MS map for this field
            current_ms_map[field] = new_ms_list
        
        # Update threshold and niter for next round
        threshold /= 1.5
        niter *= 2

    logger.info(f"Self-calibration completed successfully for all fields")
    return True

