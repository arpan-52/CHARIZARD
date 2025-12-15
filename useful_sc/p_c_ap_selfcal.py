
import os
import time
from .utils import *
from .rfi_remover import * 
from .auto_detect_utils import *
import yaml

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
    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.error("No active SPWs")
        return None

    # Handle auto-detection for field selection
    if config['msinfo'].get('calibrator_auto_detect', False):
        ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
        calibrator_file = f"{ms_basename}_calibrators.yaml"
        
        if not os.path.exists(calibrator_file):
            logger.warning(f"Calibrator file not found: {calibrator_file}, running auto-detection...")
            try:
                from .auto_detect_utils import auto_detect_calibrators
                auto_detect_calibrators(config, logger)
            except Exception as e:
                logger.error(f"Auto-detection failed: {e}")
                return None
        
        if os.path.exists(calibrator_file):
            with open(calibrator_file, 'r') as f:
                cal_data = yaml.safe_load(f)
                source_list = cal_data.get('source_list', '')
        else:
            logger.error(f"Auto-detection failed to create calibrator file: {calibrator_file}")
            return None
    else:
        # Use config values
        source_list = config['msinfo']['source_list']

    # Extract fields from source list
    field_list = source_list.split(',')
    field_list = [field.strip() for field in field_list if field.strip()]
    
    if not field_list:
        logger.error("No source fields found")
        return None
        
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



def generate_catalogs_and_calibrate(msname, output_ms, config, logger, solint, solname, calmode, tracker):
    """
    Submit combined foresight + crystalball + quartical jobs for all SPWs and fields.
    
    Parameters:
    - msname: Input MS name (e.g., 'pcal1.ms')
    - output_ms: Output MS name (e.g., 'pcal2.ms')
    - config: Configuration dictionary
    - logger: Logger object
    - solint: Solution interval (e.g., '2min')
    - solname: Solution name prefix (e.g., 'pcal1')
    - calmode: Calibration mode ('phase' or 'phase,amplitude')
    - tracker: Job tracker object
    
    Returns:
    - List of (job_id, spw, field) tuples for submitted jobs, or None if failed
    """
    logger.info("Submitting combined foresight + crystalball + quartical jobs")
    
    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.error("No active SPWs")
        return None

    # Handle auto-detection for field selection
    if config['msinfo'].get('calibrator_auto_detect', False):
        ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
        calibrator_file = f"{ms_basename}_calibrators.yaml"
        
        if not os.path.exists(calibrator_file):
            logger.warning(f"Calibrator file not found: {calibrator_file}, running auto-detection...")
            try:
                from .auto_detect_utils import auto_detect_calibrators
                auto_detect_calibrators(config, logger)
            except Exception as e:
                logger.error(f"Auto-detection failed: {e}")
                return None
        
        if os.path.exists(calibrator_file):
            with open(calibrator_file, 'r') as f:
                cal_data = yaml.safe_load(f)
                source_list = cal_data.get('source_list', '')
        else:
            logger.error(f"Auto-detection failed to create calibrator file: {calibrator_file}")
            return None
    else:
        # Use config values
        source_list = config['msinfo']['source_list']

    # Extract fields from source list
    field_list = source_list.split(',')
    field_list = [field.strip() for field in field_list if field.strip()]
    
    if not field_list:
        logger.error("No source fields found")
        return None

    # Get imaging parameters for foresight
    imsize = config['pipeline']['imaging_with_debugging']['selfcal']['imsize']
    cellsize_raw = config['pipeline']['imaging_with_debugging']['selfcal']['cellsize']
    
    # Extract numeric value from cellsize (remove 'asec' suffix)
    cellsize = cellsize_raw.replace('asec', '').strip()
    
    # Hard-coded source types for foresight
    source_types = 'S,M,U,L,C,I'
    
    # Convert solint to seconds for quartical
    solint_seconds = 120  # 2 minutes as requested
    
    # Get reference antenna index (assuming it's an integer or needs conversion)
    refant = config['pipeline']['imaging_with_debugging']['selfcal']['refant']
    # If refant is a string like 'C00', convert to index (you may need to adjust this)
    refant_index = 0  # Default to 0, you might need antenna name to index mapping
    
    logger.info(f"Generating catalogs and calibrating for fields: {field_list}")
    logger.info(f"Using imsize: {imsize}, cellsize: {cellsize}, source_types: {source_types}")
    logger.info(f"Calibration mode: {calmode}, solint: {solint_seconds}s")

    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'self_calibration')
    job_catalog_cal = []

    for spw in active_spws:
        for field in field_list:
            # Check if field-specific MS exists
            field_ms_path = f"{spw}/{field}/{msname}"
            if not os.path.exists(field_ms_path):
                logger.warning(f"MS not found for SPW {spw}, field {field}: {field_ms_path}")
                continue
            
            # Create masks directory for this field
            mask_dir = f"{spw}/{field}/masks"
            os.makedirs(mask_dir, exist_ok=True)
            
            # Create selfcal-tables directory for this field
            os.makedirs(f"{spw}/{field}/selfcal-tables", exist_ok=True)
            
            # Define output paths
            mask_file = f"{mask_dir}/{field}_mask.fits"
            source_list_file = f"{mask_dir}/{field}_sources.txt"
            output_ms_path = f"{spw}/{field}/{output_ms}"
            
            # Create field-specific solname for logging
            field_solname = f"{solname}_{field}"
            
            # Create batch script directly
            batch_file = f"catalog_cal_{field_solname}_{spw}{get_script_extension(scheduler)}"
            
            # Create batch header
            batch_header = create_batch_header(
                scheduler_type=scheduler,
                job_name=f"catalog_cal_{field_solname}_{spw}",
                nodes=job_resources.get('nodes', 1),
                ppn=job_resources.get('ppn', 4),
                walltime=job_resources.get('walltime', "02:00:00"),
                output_dir=f"{spw}/{field}/catalog_cal.log",
                queue=config['general']['queue']
            )
            
            batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}

echo "Starting foresight for {field_ms_path}"
foresight {field_ms_path} \\
    --imsize {imsize} \\
    --cellsize {cellsize} \\
    --source-types {source_types} \\
    -o {source_list_file} \\
    -m {mask_file}

echo "Starting crystalball for {field_ms_path}"
crystalball {field_ms_path} \\
    -sm {source_list_file} \\
    -j 10 \\
    -o MODEL_DATA

echo "Starting quartical for {field_ms_path}"
cd {spw}/{field}
goquartical \\
    input_ms.path={msname} \\
    input_ms.data_column=DATA \\
    input_model.recipe=MODEL_DATA \\
    solver.terms=[G] \\
    G.type={calmode} \\
    solver.iter_recipe=[50] \\
    solver.convergence_fraction=0.95 \\
    solver.reference_antenna={refant_index} \\
    output.products=[corrected_data] \\
    output.columns=[CORRECTED_DATA] \\
    output.overwrite=True

echo "Extracting corrected data to output MS {output_ms}"
{config['general']['casa_dir']}/bin/casa --nologger --nogui -c "
mstransform(vis='{msname}',
          outputvis='{output_ms}',
          datacolumn='CORRECTED')
"
cd {os.getcwd()}

echo "Completed catalog-based calibration for {field_ms_path}"
"""
            
            with open(batch_file, "w") as f:
                f.write(batch_content)
            
            # Submit job
            logger.info(f"Submitting catalog-based calibration job for SPW {spw}, field {field}")
            job_id = submit_job(batch_file, scheduler, logger)
            if job_id:
                job_catalog_cal.append((job_id, spw, field))
            time.sleep(2)

    if not job_catalog_cal:
        logger.error("No catalog-based calibration jobs submitted")
        return None

    logger.info(f"Successfully submitted {len(job_catalog_cal)} catalog-based calibration jobs")
    return job_catalog_cal


def get_mask_path(spw, field):
    """
    Get the path to the foresight mask file for a given SPW and field.
    
    Parameters:
    - spw: SPW identifier
    - field: Field name
    
    Returns:
    - Path to the mask file
    """
    return f"{spw}/{field}/masks/{field}_mask.fits"


def calibrate_ap(msname, output_ms, config, logger, solint, solname, calmode, solnorm, tracker):
    """Submit calibration jobs for all SPWs and all fields at once."""
    logger.debug(f"Entering calibrate_ap with msname: {msname}, solname: {solname}")
    
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'self_calibration')
    active_spws = tracker.get_active_spws()
    job_cal = []
    
    # Handle auto-detection for field selection
    if config['msinfo'].get('calibrator_auto_detect', False):
        ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
        calibrator_file = f"{ms_basename}_calibrators.yaml"
        
        if not os.path.exists(calibrator_file):
            logger.warning(f"Calibrator file not found: {calibrator_file}, running auto-detection...")
            try:
                from .auto_detect_utils import auto_detect_calibrators
                auto_detect_calibrators(config, logger)
            except Exception as e:
                logger.error(f"Auto-detection failed: {e}")
                return None, None
        
        if os.path.exists(calibrator_file):
            with open(calibrator_file, 'r') as f:
                cal_data = yaml.safe_load(f)
                source_list = cal_data.get('source_list', '')
        else:
            logger.error(f"Auto-detection failed to create calibrator file: {calibrator_file}")
            return None, None
    else:
        # Use config values
        source_list = config['msinfo']['source_list']

    # Extract fields from source list
    field_list = source_list.split(',')
    field_list = [field.strip() for field in field_list if field.strip()]

    if not field_list:
        logger.error("No source fields found")
        return None, None
    
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
       calmode='{calmode}',
       solnorm={solnorm})

bandpass(vis='{spw}/{field}/{msname}',
       caltable='{spw}/{field}/selfcal-tables/{field_solname}.b',
       field='',
       spw='',
       solint='inf',
       refant='{config['pipeline']['imaging_with_debugging']['selfcal']['refant']}',
       minsnr=3.0,
       gaintable=['{spw}/{field}/selfcal-tables/{field_solname}.g'],solnorm={solnorm},)

applycal(vis='{spw}/{field}/{msname}',
       gaintable=['{spw}/{field}/selfcal-tables/{field_solname}.g', 
                 '{spw}/{field}/selfcal-tables/{field_solname}.b'],
       applymode='calflag',
       flagbackup=True)

mstransform(vis='{spw}/{field}/{msname}',
          outputvis='{spw}/{field}/{output_ms}',
          datacolumn='corrected')

import shutil
shutil.rmtree('{spw}/{field}/{msname}')
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


def call_wsclean(msname, config, tracker, logger, niter, field=None, datacolumn='corrected', prefix='', threshold=0.001, use_masks=True):
    """Submit wsclean jobs for specific fields or all fields."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    active_spws = tracker.get_active_spws()
    job_ids = []
    
    # Get list of fields if not provided
    if field is None:
        # Handle auto-detection for field selection
        if config['msinfo'].get('calibrator_auto_detect', False):
            ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
            calibrator_file = f"{ms_basename}_calibrators.yaml"
            
            if not os.path.exists(calibrator_file):
                logger.warning(f"Calibrator file not found: {calibrator_file}, running auto-detection...")
                try:
                    from .auto_detect_utils import auto_detect_calibrators
                    auto_detect_calibrators(config, logger)
                except Exception as e:
                    logger.error(f"Auto-detection failed: {e}")
                    return None, None
            
            if os.path.exists(calibrator_file):
                with open(calibrator_file, 'r') as f:
                    cal_data = yaml.safe_load(f)
                    source_list = cal_data.get('source_list', '')
            else:
                logger.error(f"Auto-detection failed to create calibrator file: {calibrator_file}")
                return None, None
        else:
            # Use config values
            source_list = config['msinfo']['source_list']
        
        # Extract fields from source list
        field_list = source_list.split(',')
        field_list = [f.strip() for f in field_list if f.strip()]
        
        if not field_list:
            logger.error("No source fields found")
            return None, None
    else:
        field_list = [field]
    
    logger.info(f"Preparing WSClean jobs for fields: {field_list}")
    
    # Process each field separately
    for current_field in field_list:
        # Build list of MS files for this field
        ms_list = []
        first_spw = None
        for spw in active_spws:
            field_ms_path = f"{spw}/{current_field}/{msname}"
            if os.path.exists(field_ms_path):
                ms_list.append(field_ms_path)
                if first_spw is None:
                    first_spw = spw
        
        if not ms_list:
            logger.error(f"No {msname} found for field {current_field} in active SPWs")
            continue
        
        # Set field-specific image name
        field_imagename = f"{prefix}_{current_field}" if prefix else f"wsclean_{current_field}"
        
        # Create field-specific output directory
        field_output_dir = f"images/{current_field}"
        os.makedirs(field_output_dir, exist_ok=True)
        
        # Get mask file path if masks should be used
        mask_option = ""
        if use_masks and first_spw is not None:
            mask_file = get_mask_path(first_spw, current_field)
            if os.path.exists(mask_file):
                mask_option = f"-fits-mask {mask_file}"
                logger.info(f"Using mask for field {current_field}: {mask_file}")
            else:
                logger.warning(f"Mask file not found for field {current_field}: {mask_file}")
        
        # Build wsclean command with optional mask
        base_command = f"""wsclean \\
    -name {field_output_dir}/{field_imagename} \\
    -weight briggs 0.0 \\
    -super-weight 1.0 \\
    -weighting-rank-filter-size 16 \\
    -taper-gaussian 0 \\
    -size {config['pipeline']['imaging_with_debugging']['selfcal']['imsize']} {config['pipeline']['imaging_with_debugging']['selfcal']['imsize']} \\
    -scale {config['pipeline']['imaging_with_debugging']['selfcal']['cellsize']} \\
    -channels-out 4 \\
    -wstack-grid-mode kb \\
    -wstack-kernel-size 7 \\
    -wstack-oversampling 63 \\
    -pol I \\
    -intervals-out 1 \\
    -data-column {datacolumn} \\
    -niter {niter} \\
    -auto-mask 7 \\
    -auto-threshold 3 \\
    -gain 0.1 \\
    -mgain 0.7 \\
    -join-channels \\
    -no-negative \\
    -multiscale-scale-bias 0.6 \\
    -fit-spectral-pol 3 \\
    -fit-beam \\
    -elliptical-beam \\
    -padding 1.3 \\
    -parallel-deconvolution 8192"""
        
        if mask_option:
            wsclean_command = f"{base_command} \\\n    {mask_option} \\\n    {' '.join(ms_list)}"
        else:
            wsclean_command = f"{base_command} \\\n    {' '.join(ms_list)}"

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
    """Generate solint sequence: halve each round, floor at 1min."""
    
    pcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
    apcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0)
    initial_solint = config['pipeline']['imaging_with_debugging']['selfcal']['solint']
    
    initial_min = int(initial_solint.replace('min', ''))
    
    # Phase cal sequence
    pcal_sequence = []
    current = initial_min
    for _ in range(pcal_rounds):
        pcal_sequence.append(f"{max(current, 1)}min")
        current = current // 2
    
    # Amp-phase cal sequence - starts at initial_solint / 2
    apcal_sequence = []
    current = initial_min // 2  # Start at 2min
    for _ in range(apcal_rounds):
        apcal_sequence.append(f"{max(current, 1)}min")
        current = current // 2
    
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
            threshold=threshold,
            use_masks=False
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

    # NEW: Catalog-based self-calibration if requested
    if config['pipeline']['imaging_with_debugging']['selfcal'].get('use_catalogs', False):
        logger.info("Running catalog-based self-calibration (foresight + crystalball + quartical)")
        
        job_catalog_cal = generate_catalogs_and_calibrate(
            'pcal1.ms',      # Input MS
            'catalog1.ms',   # Output MS after catalog-based calibration
            config,
            logger,
            '2min',          # Fixed 2-minute solution interval
            'catalog1',      # Solution name
            'phase',         # Phase-only calibration for catalog step
            tracker
        )
        
        if job_catalog_cal:
            tracker_jobs = convert_jobs_to_tracker_format(job_catalog_cal)
            tracker.add_jobs('catalog_calibration', tracker_jobs)
            
            catalog_successful, catalog_failed = wait_for_field_jobs_to_finish(
                job_catalog_cal,
                config['general']['working_directory'],
                logger,
                'catalog_cal',
                scheduler,
                field_list
            )
            
            if not tracker.check_brotherhood(catalog_failed):
                logger.error(f"Catalog-based calibration failed")
                cleanup_and_exit([j[0] for j in job_catalog_cal], scheduler, logger)
                return False
            
            # Update MS map to use catalog-calibrated MS files
            for field in field_list:
                new_ms_list = []
                for ms_path in current_ms_map[field]:
                    # Extract SPW and field, replace MS name with catalog1.ms
                    path_parts = ms_path.split('/')
                    if len(path_parts) >= 2:
                        spw = path_parts[0]
                        field_name = path_parts[1]
                        new_ms_path = f"{spw}/{field_name}/catalog1.ms"
                        new_ms_list.append(new_ms_path)
                
                current_ms_map[field] = new_ms_list
            
            logger.info("Catalog-based calibration completed successfully")

    # Phase calibration rounds
    for i in range(config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']):
        logger.info(f"Starting phase calibration round {i+1}")
        
        # Next MS name (for output)
        next_ms = f'pcal{i+2}.ms'
        
        # Use appropriate solint from sequence
        current_solint = solint_sequence[i]
        logger.info(f"Using solint: {current_solint} for phase cal round {i+1}")

        # Determine input MS name based on whether catalog calibration was done
        if i == 0:
            input_ms = 'catalog1.ms' if config['pipeline']['imaging_with_debugging']['selfcal'].get('use_catalogs', False) else 'pcal1.ms'
        else:
            input_ms = f'pcal{i+1}.ms'

        # Run imaging for all fields
        job_img, img_prefix = call_wsclean(
            input_ms,
            config,
            tracker,
            logger,
            niter * (2 if i > 0 else 1),
            field=None,  # Process all fields
            datacolumn='DATA',
            prefix=f'selfcal_p{i}',
            threshold=threshold,
            use_masks=False  # No masks as requested
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
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_pcal{i+1}',
                    scheduler,
                    field_list
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
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_pcal{i+1}',
                    scheduler,
                    field_list
                )
                
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"NAMI flagging after phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs2], scheduler, logger)
                    return False

        # Run calibration once for all fields
        job_cal, solname = calibrate_ap(
            input_ms,
            next_ms, 
            config, 
            logger, 
            current_solint, 
            f'pcal{i+1}', 
            'p', 
            'True',
            tracker,
        )
        
        if job_cal:
            tracker_jobs = convert_jobs_to_tracker_format(job_cal)
            tracker.add_jobs(f'pcal_{i+1}', tracker_jobs)
            cal_successful, cal_failed = wait_for_field_jobs_to_finish(
                job_cal,
                config['general']['working_directory'],
                logger,
                f'pcal{i+1}',
                scheduler,
                field_list
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
            threshold=threshold,
            use_masks=False  # No masks as requested
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
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_apcal{i+1}',
                    scheduler,
                    field_list
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
                flag_successful, flag_failed = wait_for_field_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_apcal{i+1}',
                    scheduler,
                    field_list
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
            'False',
            tracker
        )
        
        if job_cal:
            tracker_jobs = convert_jobs_to_tracker_format(job_cal)
            tracker.add_jobs(f'apcal_{i+1}', tracker_jobs)
            cal_successful, cal_failed = wait_for_field_jobs_to_finish(
                job_cal,
                config['general']['working_directory'],
                logger,
                f'apcal{i+1}',
                scheduler,
                field_list
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