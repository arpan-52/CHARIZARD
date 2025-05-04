
import os
import time
from .utils import *
from .rfi_remover import * 


def calculate_job_resources(config, job_type):
   max_ppn = config['general']['max_ppn']
   job_types = {
       'calibration': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
       'applycal': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
       'imaging': {'nodes': 2, 'ppn': 16, 'walltime': "12:00:00"}
   }
   return job_types.get(job_type, {'nodes': 1, 'ppn': 12, 'walltime': "10:30:00"})



def prepare_ms(config, logger, tracker):
   """Prepare measurement sets by splitting and flagging for all SPWs."""
   if not config['pipeline']['imaging_with_debugging']['selfcal'].get('average_and_flag', False):
       if os.path.exists('pcal1.ms'):
           logger.info("Found split MS")
           return 'pcal1.ms'
       logger.error("No split MS found and splitting not requested")
       return None

   active_spws = tracker.get_active_spws()
   if not active_spws:
       logger.error("No active SPWs")
       return None

   scheduler = config['general']['PBS_or_SLURM']
   job_split = []

   for spw in active_spws:
       casa_script = f"""
mstransform(vis='{spw}/src.ms',
        outputvis='{spw}/pcal1.ms',
        datacolumn='corrected',
        chanaverage=True,
        chanbin={config['pipeline']['imaging_with_debugging']['selfcal']['freqbin']},
        field='{config['msinfo']['source_list']}')
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
       # First round of flagging
       job_flag = general_flagger(
           ms_names=['pcal1.ms'],
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
           tracker.add_jobs('flagging_avg', job_flag)
           flag_successful, flag_failed = wait_for_jobs_to_finish(
               job_flag,
               config['general']['working_directory'],
               logger,
               'flag_tfcrop_rflag_avg',
               scheduler
           )

           if not tracker.check_brotherhood(flag_failed):
               logger.error(f"First flagging round failed for: {flag_failed}")
               cleanup_and_exit([j[0] for j in job_flag], scheduler, logger)
               return None

           # Second round of flagging
           if flag_successful:
               job_flag = general_flagger(
                   ms_names=['pcal1.ms'],
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
                   tracker.add_jobs('flagging_avg', job_flag)
                   flag_successful, flag_failed = wait_for_jobs_to_finish(
                       job_flag,
                       config['general']['working_directory'],
                       logger,
                       'flag_tfcrop_rflag_avg',
                       scheduler
                   )

                   if not tracker.check_brotherhood(flag_failed):
                       logger.error(f"Second flagging round failed for: {flag_failed}")
                       cleanup_and_exit([j[0] for j in job_flag], scheduler, logger)
                       return None

                   if flag_successful:
                       logger.info("Successfully completed MS preparation")
                       return 'pcal1.ms'

   return None


def calibrate_ap(msname, output_ms, config, logger, solint, solname, calmode, tracker):
   """Submit calibration jobs for all SPWs at once."""
   logger.debug(f"Entering calibrate_ap with msname: {msname}, solname: {solname}")
   logger.debug(f"Config keys: {list(config.keys())}")
   if 'pipeline' in config:
       logger.debug(f"Pipeline keys: {list(config['pipeline'].keys())}")
       if 'calibration' in config['pipeline']:
           logger.debug(f"Calibration keys: {list(config['pipeline']['calibration'].keys())}")
   
   scheduler = config['general']['PBS_or_SLURM']
   job_resources = calculate_job_resources(config, 'calibration')
   active_spws = tracker.get_active_spws()
   job_cal = []

   logger.debug(f"Creating jobs for SPWs: {active_spws}")
   for spw in active_spws:
       logger.debug(f"Processing SPW {spw}")
       os.makedirs(f"{spw}/selfcal-tables", exist_ok=True)

       casa_script = f"""
gaincal(vis='{spw}/{msname}',
       caltable='{spw}/selfcal-tables/{solname}.g',
       field='',
       spw='',
       solint='{solint}',
       refant='{config['pipeline']['imaging_with_debugging']['selfcal']['refant']}',
       minsnr=2.0,
       gaintype='G',
       calmode='{calmode}')

bandpass(vis='{spw}/{msname}',
       caltable='{spw}/selfcal-tables/{solname}.b',
       field='',
       spw='',
       solint='inf',
       refant='{config['pipeline']['imaging_with_debugging']['selfcal']['refant']}',
       minsnr=3.0,
       gaintable=['{spw}/selfcal-tables/{solname}.g'])

applycal(vis='{spw}/{msname}',
       gaintable=['{spw}/selfcal-tables/{solname}.g', 
                 '{spw}/selfcal-tables/{solname}.b'],
       applymode='calflag',
       flagbackup=True)

mstransform(vis='{spw}/{msname}',
          outputvis='{spw}/{output_ms}',
          datacolumn='corrected')
"""

       script_file = f"{solname}_{spw}.py"
       batch_file = f"{solname}_{spw}{get_script_extension(scheduler)}"
       
       logger.debug(f"Writing scripts for SPW {spw}")
       with open(script_file, "w") as f:
           f.write(casa_script)

       batch_header = create_batch_header(
           scheduler_type=scheduler,
           job_name=f"{solname}_{spw}",
           nodes=job_resources['nodes'],
           ppn=job_resources['ppn'],
           walltime=job_resources['walltime'],
           output_dir=f"{spw}/{solname}.log",
           queue=config['general']['queue']
       )

       batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""

       with open(batch_file, "w") as f:
           f.write(batch_content)
       
       logger.debug(f"Submitting job for SPW {spw}")
       job_id = submit_job(batch_file, scheduler, logger)
       if job_id:
           job_cal.append((job_id, spw))
       time.sleep(5)

   logger.debug(f"Completed calibrate_ap, returning {len(job_cal)} jobs")
   return job_cal, solname


def call_wsclean(msname, config, tracker, logger, niter, datacolumn='corrected', prefix='',threshold=0.001):
    """Submit a single wsclean job that processes MS files from all SPWs."""
    scheduler = config['general']['PBS_or_SLURM']
    job_resources = calculate_job_resources(config, 'imaging')
    active_spws = tracker.get_active_spws()

    ms_list = [f'{spw}/{msname}' for spw in active_spws]
    if not ms_list:
        logger.error(f"No {msname} found in active SPWs")
        return None, None

    imagename = prefix if prefix else "wsclean"
    wsclean_command = f"""wsclean \\
    -name {imagename} \\
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
    """Execute self-calibration pipeline with proper job tracking."""
    if 'selfcal' not in config['pipeline']['imaging_with_debugging']:
        logger.info("Self-calibration not requested")
        return True

    # Prepare MS files
    initial_ms = prepare_ms(config, logger, tracker)
    if not initial_ms:
        logger.error("Failed to prepare MS")
        return False

    # Get solint sequence
    solint_sequence = get_solint_sequence(config)
    logger.info(f"Using solint sequence: {solint_sequence}")

    scheduler = config['general']['PBS_or_SLURM']
    threshold = float(config['pipeline']['imaging_with_debugging']['selfcal']['threshold_to_clean'].replace('mJy','')) * 0.001
    niter = config['pipeline']['imaging_with_debugging']['selfcal']['iterations_to_start']

    # Initial dirty image if requested
    if config['pipeline']['imaging_with_debugging'].get('dirty', False):
        logger.info("Creating dirty image")
        job_id, prefix = call_wsclean(initial_ms, config, tracker, logger, 0, 'DATA', 'dirty',threshold)
        if job_id:
            tracker.add_jobs('dirty_image', [job_id])
            dirty_successful, dirty_failed = wait_for_wsclean_job(
                job_id,
                config['general']['working_directory'],
                logger,
                'dirty',
                scheduler
            )
            if not tracker.check_brotherhood(dirty_failed):
                logger.error("Dirty imaging failed")
                cleanup_and_exit([job_id[0]], scheduler, logger)
                return False

    # Phase calibration rounds
    current_ms = initial_ms
    for i in range(config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']):
        logger.info(f"Starting phase calibration round {i+1}")
        next_ms = f'pcal{i+2}.ms'  # pcal2.ms, pcal3.ms, etc.

        # Use appropriate solint from sequence
        current_solint = solint_sequence[i]
        logger.info(f"Using solint: {current_solint} for phase cal round {i+1}")

        # Run imaging on current MS
        job_img, img_prefix = call_wsclean(
            current_ms, 
            config, 
            tracker, 
            logger, 
            niter * (2 if i > 0 else 1),
            'DATA',
            f'selfcal_p{i}',threshold
        )
        if job_img:
            tracker.add_jobs(f'image_pcal_{i+1}', [job_img])
            img_successful, img_failed = wait_for_wsclean_job(
                job_img,
                config['general']['working_directory'],
                logger,
                f'selfcal_p{i}',
                scheduler
            )
            if not tracker.check_brotherhood(img_failed):
                logger.error(f"Imaging after phase cal round {i+1} failed")
                cleanup_and_exit([job_img[0]], scheduler, logger)
                return False

        # Flag residuals if requested
        if config['pipeline']['imaging_with_debugging']['selfcal'].get('flag_residual', False):
            logger.info(f"Flagging residuals for phase cal round {i+1}")
            logger.info("Removing the lock file first")
            # active_spws = tracker.get_active_spws()
            # for spw in active_spws:
            #     remove_lock(f"{spw}/{current_ms}")
            logger.info("Phew, done!")
            flag_jobs1 = general_flagger(
                ms_names=[current_ms],
                mode='rflag',
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='RESIDUAL',
                prefix=f'pcal{i+1}'
            )
            
            if flag_jobs1:
                tracker.add_jobs(f'flag_pcal_{i+1}', flag_jobs1)
                flag_successful, flag_failed = wait_for_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_pcal{i+1}',
                    scheduler
                )
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"Flagging after phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs1], scheduler, logger)
                    return False
                

            flag_jobs2 = nami_flagger(
                ms_names=[current_ms],
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='DATA',                         # Have some attention to it, either flag after calibration, so you have correctted data column, or before imaging. Think about it AP. 
                method='poly',
                degree=3,it=2,sigma=3,timebin=10,ncpu=8,
                prefix=f'pcal{i+1}'
            )
                
            if flag_jobs2:
                tracker.add_jobs(f'flag_pcal_{i+1}', flag_jobs2)
                flag_successful, flag_failed = wait_for_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_pcal{i+1}',
                    scheduler
                )
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"Flagging after phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs2], scheduler, logger)
                    return False

        # Run calibration to create next MS
        job_cal, solname = calibrate_ap(current_ms, next_ms, config, logger, current_solint, f'pcal{i+1}', 'p', tracker)
        if job_cal:
            tracker.add_jobs(f'pcal_{i+1}', job_cal)
            cal_successful, cal_failed = wait_for_jobs_to_finish(
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

        current_ms = next_ms
        threshold /= 1.5
        niter *= 2

    # Amplitude-phase calibration rounds
    pcal_rounds = config['pipeline']['imaging_with_debugging']['selfcal']['phase_cal']
    for i in range(config['pipeline']['imaging_with_debugging']['selfcal'].get('amp_phase_cal', 0)):
        logger.info(f"Starting amp-phase calibration round {i+1}")
        next_ms = f'apcal{i+1}.ms'

        # Use appropriate solint from sequence
        current_solint = solint_sequence[pcal_rounds + i]
        logger.info(f"Using solint: {current_solint} for amp-phase cal round {i+1}")

        # Run imaging
        job_img, img_prefix = call_wsclean(
            current_ms, 
            config, 
            tracker, 
            logger, 
            niter,
            'DATA',
            f'selfcal_ap{i}',threshold
        )
        if job_img:
            tracker.add_jobs(f'image_apcal_{i+1}', [job_img])
            img_successful, img_failed = wait_for_wsclean_job(
                job_img,
                config['general']['working_directory'],
                logger,
                f'selfcal_ap{i}',
                scheduler
            )
            if not tracker.check_brotherhood(img_failed):
                logger.error(f"Imaging after amp-phase cal round {i+1} failed")
                cleanup_and_exit([job_img[0]], scheduler, logger)
                return False

        # Flag residuals if requested
        if config['pipeline']['imaging_with_debugging']['selfcal'].get('flag_residual', False):
            logger.info(f"Flagging residuals for amp-phase cal round {i+1}")
            flag_jobs1 = general_flagger(
                ms_names=[current_ms],
                mode='rflag',
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='RESIDUAL',
                prefix=f'apcal{i+1}'
            )
            

            if flag_jobs1:
                tracker.add_jobs(f'flag_apcal_{i+1}', flag_jobs1)
                flag_successful, flag_failed = wait_for_jobs_to_finish(
                    flag_jobs1,
                    config['general']['working_directory'],
                    logger,
                    f'flag_rflag_apcal{i+1}',
                    scheduler
                )
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"Flagging after amp-phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs1], scheduler, logger)
                    return False
            
            flag_jobs2 = nami_flagger(
                ms_names=[current_ms],
                tracker=tracker,
                logger=logger,
                config=config,
                datacolumn='DATA',
                method='poly',
                degree=3,it=2,sigma=3,timebin=10,ncpu=8,
                prefix=f'apcal{i+1}'
            )
            
            
            if flag_jobs2:
                tracker.add_jobs(f'flag_apcal_{i+1}', flag_jobs2)
                flag_successful, flag_failed = wait_for_jobs_to_finish(
                    flag_jobs2,
                    config['general']['working_directory'],
                    logger,
                    f'flag_nami_apcal{i+1}',
                    scheduler
                )
                if not tracker.check_brotherhood(flag_failed):
                    logger.error(f"Flagging after amp-phase cal round {i+1} failed")
                    cleanup_and_exit([j[0] for j in flag_jobs2], scheduler, logger)
                    return False
        
        # Run calibration
        job_cal, solname = calibrate_ap(
            current_ms,
            next_ms,
            config,
            logger,
            current_solint,
            f'apcal{i+1}',
            'ap',
            tracker
        )
        
        if job_cal:
            tracker.add_jobs(f'apcal_{i+1}', job_cal)
            cal_successful, cal_failed = wait_for_jobs_to_finish(
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

        current_ms = next_ms
        threshold /= 1.5
        niter *= 2

    logger.info("Self-calibration completed successfully")
    return True