import os
import time
from .utils import *
from .rfi_remover import * 


def calculate_job_resources(config, job_type):
    max_ppn = config['general']['max_ppn']
    job_types = {
        'calibration': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
        'applycal': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"},
        'flagging': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "12:00:00"}
    }
    return job_types.get(job_type, {'nodes': 1, 'ppn': 12, 'walltime': "10:30:00"})

def get_unique_calibrators(calibrators):
    """
    Get unique calibrator list handling both lists and comma-separated strings
    
    Args:
        calibrators: List of strings that might contain comma-separated values
        
    Returns:
        String of unique calibrators joined by commas
    """
    seen = set()
    unique_cals = []
    
    for cal in calibrators:
        if cal:  # Only process if not None/empty
            # Split if it's a comma-separated string
            cal_list = cal.split(',') if isinstance(cal, str) else [cal]
            for c in cal_list:
                c = c.strip()  # Remove any whitespace
                if c and c not in seen:  # Only add non-empty strings
                    seen.add(c)
                    unique_cals.append(c)
    
    return ','.join(unique_cals)



# Define polarization calibrator properties
POLCAL_SOURCES = {
    "3C286": {
        "source": "3C286",
        "reffreq": "0.65GHz",
        "stokes_I": 21.1724326,
        "spectral_index": [-0.398465, -0.14583],
        "polarization_fraction": [0.03089562, 0.07728649, -0.07319306],
        "polarization_angle": [1.10376585,  10.86354924,  71.17898745, 240.50941923, 409.24434549, 300.61503066]
    },
    "3C48": {
        "source": "3C48",
        "reffreq": "1.0GHz",
        "stokes_I": 16.50,
        "spectral_index": [-0.47, -0.13],
        "polarization_fraction": [0.005],
        "polarization_angle": [0.0]
    }
}

def get_calibrator_solutions(tracker, logger, config, cal_round):
    """Execute calibration steps for each SPW"""
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []
    
    job_resources = calculate_job_resources(config, 'calibration')
    active_spws = tracker.get_active_spws()
    
    if not active_spws:
        logger.warning("No active SPWs available for calibration")
        return []

    logger.info(f"Running calibration round {cal_round} for SPWs: {active_spws}")
    
    # Get calibrator info
    amp_cal = config['msinfo']['amp_cal']
    phase_cal = config['msinfo']['phase_cal']
    leakage_cal = config['msinfo'].get('leakage_cal')
    polang_cal = config['msinfo'].get('polang_cal')
    refant = config['pipeline']['calibration']['refant']
    leakage_mode = config['pipeline']['calibration']['leakage_mode']
    # Check if we have valid polarization calibrators
    do_polcal = leakage_cal and polang_cal and polang_cal in POLCAL_SOURCES
    field_list = get_unique_calibrators([amp_cal, phase_cal, polang_cal, leakage_cal] if do_polcal else [amp_cal, phase_cal])
    transfer_list = get_unique_calibrators([phase_cal, polang_cal, leakage_cal] if do_polcal else [phase_cal])

    for spw in active_spws:
        casa_script = f"""
# Delay calibration

setjy(vis='{spw}/cal.ms',field='{amp_cal}')

gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/delays.cal{cal_round}',
        field='{amp_cal}',
        refant='{refant}',
        gaintype='K',
        solint='inf',
        combine='scan',
        minsnr=3)

# Initial phase calibration
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/phase_int.cal{cal_round}',
        field='{field_list}',
        refant='{refant}',
        gaintype='G',
        calmode='p',
        solint='int',
        minsnr=3)

# Bandpass calibration
bandpass(vis='{spw}/cal.ms',
         caltable='{spw}/caltables/bandpass.cal{cal_round}',
         field='{amp_cal}',
         refant='{refant}',
         solint='inf',
         combine='scan',
         solnorm=True,
         minsnr=3,
         gaintable=['{spw}/caltables/delays.cal{cal_round}',
                    '{spw}/caltables/phase_int.cal{cal_round}'])

# Amplitude and phase calibration
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/amp_phase.cal{cal_round}',
        field='{field_list}',
        refant='{refant}',
        gaintype='G',
        calmode='ap',
        solint='120s',
        minsnr=3,
        gaintable=['{spw}/caltables/delays.cal{cal_round}',
                   '{spw}/caltables/bandpass.cal{cal_round}'])


# Flux calibration
fluxscale(vis='{spw}/cal.ms',
          caltable='{spw}/caltables/amp_phase.cal{cal_round}',
          fluxtable='{spw}/caltables/flux.cal{cal_round}',
          reference='{amp_cal}',
          transfer='{transfer_list}')
"""

        if do_polcal:
            polcal_data = POLCAL_SOURCES[polang_cal]
            casa_script += f"""
# Set polarization calibrator model
setjy(vis='{spw}/cal.ms',
      field='{polang_cal}',
      standard='manual',
      fluxdensity=[{polcal_data['stokes_I']}, 0, 0, 0],
      spix={polcal_data['spectral_index']},
      reffreq="{polcal_data['reffreq']}",
      polindex={polcal_data['polarization_fraction']},
      polangle={polcal_data['polarization_angle']})

# Cross-hand delay calibration
gaincal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/delaycross.cal{cal_round}', 
       field='{polang_cal}',
       refant='{refant}',
       gaintype='KCROSS',  # Changed to KCROSS for cross-hand delays
       solint='inf',
       calmode='ap',     # Changed to 'a' for cross-hand delays
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                 '{spw}/caltables/bandpass.cal{cal_round}',
                 '{spw}/caltables/flux.cal{cal_round}'])

polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/leakage.cal{cal_round}',
       field='{leakage_cal}',
       refant='{refant}',
       poltype='{leakage_mode}',
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                  '{spw}/caltables/bandpass.cal{cal_round}',
                  '{spw}/caltables/flux.cal{cal_round}',
                  '{spw}/caltables/delaycross.cal{cal_round}'])

polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/polangle.cal{cal_round}',
       field='{polang_cal}',
       refant='{refant}',
       poltype='Xf',
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                  '{spw}/caltables/bandpass.cal{cal_round}',
                  '{spw}/caltables/flux.cal{cal_round}',
                  '{spw}/caltables/delaycross.cal{cal_round}',
                  '{spw}/caltables/leakage.cal{cal_round}'])
"""
        elif polang_cal and polang_cal not in POLCAL_SOURCES:
            logger.warning(f"Polarization angle calibrator {polang_cal} not found in known sources. Skipping polarization calibration.")

        script_file = f"calibrate_{cal_round}_{spw}.py"
        batch_file = f"calibrate_{cal_round}_{spw}{get_script_extension(scheduler)}"
        
        with open(script_file, "w") as f:
            f.write(casa_script)

        ppn = job_resources['ppn']
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"calibrate_{spw}_{cal_round}",
            nodes=job_resources['nodes'],
            ppn=ppn,
            walltime=job_resources['walltime'],
            output_dir=f"{spw}/calibrate_{cal_round}.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(5)

        if job_id:
            job_info.append((job_id, spw))
            
    return job_info




def apply_calibration(tracker, logger, config, cal_round, target_type='calibrators'):
    """Apply calibration solutions to calibrators or target sources"""
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []
    
    job_resources = calculate_job_resources(config, 'applycal')
    active_spws = tracker.get_active_spws()

    if not active_spws:
        logger.warning("No active SPWs available for applying calibration")
        return []

    # Get calibrator info for field selection
    amp_cal = config['msinfo']['amp_cal']
    phase_cal = config['msinfo']['phase_cal']
    leakage_cal = config['msinfo'].get('leakage_cal')
    polang_cal = config['msinfo'].get('polang_cal')
    source_list = config['msinfo']['source_list']

    # Check if polarization calibration was done
    do_polcal = (leakage_cal and polang_cal and polang_cal in POLCAL_SOURCES)

    # Set up calibrator fields
    calibrator_list = get_unique_calibrators([amp_cal, phase_cal, polang_cal, leakage_cal] if do_polcal else [amp_cal, phase_cal])

    logger.info(f"Applying calibration round {cal_round} for {target_type}")

    for spw in active_spws:
        # Basic gain tables
        gaintables = [
            f"{spw}/caltables/delays.cal{cal_round}",
            f"{spw}/caltables/bandpass.cal{cal_round}",
            f"{spw}/caltables/flux.cal{cal_round}"
        ]
        
        if do_polcal:
            gaintables.extend([
                f"{spw}/caltables/delaycross.cal{cal_round}",
                f"{spw}/caltables/leakage.cal{cal_round}",
                f"{spw}/caltables/polangle.cal{cal_round}"
            ])

        gaintables_str = "['" + "','".join(gaintables) + "']"
        gainfields = "['nearest'] * " + str(len(gaintables))

        if target_type == 'calibrators':
            casa_script = f"""
# Apply calibration to calibrators
applycal(vis='{spw}/cal.ms',
         field='{calibrator_list}',
         gaintable={gaintables_str},
         gainfield={gainfields},
         interp=['nearest'] * {len(gaintables)},
         parang=True,
         calwt=True,
         flagbackup=True)
"""
        else:
            casa_script = f"""
# Apply calibration to science targets
applycal(vis='{spw}/src.ms',
         field='{source_list}',
         gaintable={gaintables_str},
         gainfield={gainfields},
         interp=['linear'] * {len(gaintables)},
         parang=True,
         calwt=True,
         flagbackup=True)
"""

        script_file = f"applycal_{target_type}_{cal_round}_{spw}.py"
        batch_file = f"applycal_{target_type}_{cal_round}_{spw}{get_script_extension(scheduler)}"
        
        with open(script_file, "w") as f:
            f.write(casa_script)

        ppn = job_resources['ppn']
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"applycal_{target_type}_{spw}_{cal_round}",
            nodes=job_resources['nodes'],
            ppn=ppn,
            walltime=job_resources['walltime'],
            output_dir=f"{spw}/applycal_{target_type}_{cal_round}.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(5)

        if job_id:
            job_info.append((job_id, spw))
            
    return job_info



def do_calibration(config, logger, tracker):
    """
    Execute calibration pipeline with flagging stages
    
    Flow:
    1. First calibration round
    2. Optional source flagging in parallel
    3. Apply to calibrators
    4. Flag calibrators if requested
    5. Apply to sources if requested
    6. Second calibration round
    7. Final application and flagging
    """

    if 'calibration' not in config['pipeline']:
        logger.info("Skipping calibration")
        return True

    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.error("No active SPWs")
        return

    # First calibration round 
    cal_round = 1
    job_cal = get_calibrator_solutions(tracker, logger, config, cal_round)
    tracker.add_jobs('calibration_1', job_cal)

    # Launch source flagging in parallel 
    job_source_flag = None
    if config['pipeline']['calibration'].get('flag_source_before_cal'):
        job_source_flag = general_flagger(
            ms_names=['src.ms'],
            mode='tfcrop',
            tracker=tracker,
            logger=logger,
            config=config,
            datacolumn='DATA'
        )
        tracker.add_jobs('source_flagging_1', job_source_flag)

    # First check point
    cal_successful, cal_failed = wait_for_jobs_to_finish(
        job_cal,
        config['general']['working_directory'],
        logger,
        f'calibrate_{cal_round}',
        config['general']['PBS_or_SLURM']
    )

    if not tracker.check_brotherhood(cal_failed):
        logger.error(f"Calibration failed for: {cal_failed}")
        cleanup_and_exit(job_cal, config['general']['PBS_or_SLURM'], logger) 
        return

    # Apply to calibrators
    if config['pipeline']['calibration'].get('applycal_cals'):
        logger.info("Applying calibration to the calibrators.")
        job_apply_cal = apply_calibration(tracker, logger, config, cal_round, target_type='calibrators')
        tracker.add_jobs('applycal_cal_1', job_apply_cal)

        # Second check point 
        apply_cal_successful, apply_cal_failed = wait_for_jobs_to_finish(
            job_apply_cal,
            config['general']['working_directory'],
            logger,
            f'applycal_calibrators_{cal_round}',
            config['general']['PBS_or_SLURM']
        )

        if not tracker.check_brotherhood(apply_cal_failed):
            logger.error(f"Calibrator applycal failed for: {apply_cal_failed}")
            cleanup_and_exit(job_apply_cal, config['general']['PBS_or_SLURM'], logger)
            return

        # Flag calibrators if requested
        if config['pipeline']['calibration'].get('flag_after_cal'):
            # job_cal_flag = general_flagger(
            #     ms_names=['cal.ms'],
            #     mode='tfcrop,rflag',
            #     tracker=tracker,
            #     logger=logger,
            #     config=config,
            #     datacolumn='CORRECTED'
            # )
            # I am replacing the calibrated flagging with nami, let's see how it goes
            job_cal_flag =  nami_flagger(['cal.ms'], tracker=tracker, logger=logger, config=config, method='poly',
                                         sigma=4,it=1,degree=2,ncpu=8,timebin=10,datacolumn='CORRECTED_DATA')
            tracker.add_jobs('calibrator_flagging_1', job_cal_flag)

            cal_flag_successful, cal_flag_failed = wait_for_jobs_to_finish(
                job_cal_flag,
                config['general']['working_directory'],
                logger,
                'flag_nami', 
                config['general']['PBS_or_SLURM']
            )
            if not tracker.check_brotherhood(cal_flag_failed):
                logger.error(f"Calibrator flagging failed for: {cal_flag_failed}")
                cleanup_and_exit([j[0] for j in job_cal_flag], config['general']['PBS_or_SLURM'], logger)
            else:
                logger.info(f"Calibrator flagging 1 completed. Failed SPWs: {cal_flag_failed if cal_flag_failed else 'None'}")
                
                active_spws = tracker.get_active_spws()
                logger.info(f"Calibration will continue with SPWs: {active_spws}")
                for spw in active_spws:
                    tracker.handle_job_completion('calibrator_flagging_1', spw, True)
                    cleanup_files(spw, 'flag_nami', config['general']['PBS_or_SLURM'], logger)

    # Second calibration round
    cal_round = 2
    job_cal2 = get_calibrator_solutions(tracker, logger, config, cal_round)
    tracker.add_jobs('calibration_2', job_cal2)

    # Third check point
    cal2_successful, cal2_failed = wait_for_jobs_to_finish(
        job_cal2,
        config['general']['working_directory'],
        logger,
        f'calibrate_{cal_round}',
        config['general']['PBS_or_SLURM']
    )

    if not tracker.check_brotherhood(cal2_failed):
        logger.error(f"Second calibration failed for: {cal2_failed}")
        cleanup_and_exit(job_cal2, config['general']['PBS_or_SLURM'], logger)
        return
    source_flag_successful, source_flag_failed = wait_for_jobs_to_finish(
    job_source_flag,
    config['general']['working_directory'],
    logger,
    'flag_tfcrop',
    config['general']['PBS_or_SLURM']
)
    
    if not tracker.check_brotherhood(source_flag_failed):
        logger.error(f"Source flagging failed for: {source_flag_failed}")
        cleanup_and_exit(job_source_flag, config['general']['PBS_or_SLURM'], logger)
        return
    
    # Final application to calibrators and sources
    if config['pipeline']['calibration'].get('applycal_cals') or config['pipeline']['calibration'].get('applycal_targets'):
        job_apply_cal2 = None
        job_apply_src2 = None

        # Submit all jobs first
        if config['pipeline']['calibration'].get('applycal_cals'):
            logger.info(f"Submitting calibration round {cal_round} application to calibrators")
            job_apply_cal2 = apply_calibration(tracker, logger, config, cal_round, target_type='calibrators')
            tracker.add_jobs('applycal_cal_2', job_apply_cal2)

        if config['pipeline']['calibration'].get('applycal_targets'):
            logger.info(f"Submitting calibration round {cal_round} application to target sources")
            job_apply_src2 = apply_calibration(tracker, logger, config, cal_round, target_type='sources')
            tracker.add_jobs('applycal_src_2', job_apply_src2)

        # Now wait for calibrators
        if job_apply_cal2:
            cal_successful, cal_failed = wait_for_jobs_to_finish(
                job_apply_cal2,
                config['general']['working_directory'],
                logger,
                f'applycal_calibrators_{cal_round}',
                config['general']['PBS_or_SLURM']
            )

            if not tracker.check_brotherhood(cal_failed):
                logger.error(f"Final calibrator applycal failed for: {cal_failed}")
                cleanup_and_exit(job_apply_cal2, config['general']['PBS_or_SLURM'], logger)
                return

        # Then wait for sources
        if job_apply_src2:
            src_successful, src_failed = wait_for_jobs_to_finish(
                job_apply_src2,
                config['general']['working_directory'],
                logger,
                f'applycal_sources_{cal_round}',
                config['general']['PBS_or_SLURM']
            )

            if not tracker.check_brotherhood(src_failed):
                logger.error(f"Final source applycal failed for: {src_failed}")
                cleanup_and_exit(job_apply_src2, config['general']['PBS_or_SLURM'], logger)
                return
        # Final flagging
        if config['pipeline']['calibration'].get('flag_after_cal'):
            job_final_flag_cal = None
            job_final_flag_src = None

            # Submit all flagging jobs first
            if config['pipeline']['calibration'].get('applycal_cals'):
                logger.info("Submitting final flagging for calibrators")
                # job_final_flag_cal = general_flagger(
                #     ms_names=['cal.ms'],
                #     mode='tfcrop,rflag',
                #     tracker=tracker,
                #     logger=logger,
                #     config=config,
                #     prefix='cal',
                #     datacolumn='CORRECTED'
                # )
                job_final_flag_cal =  nami_flagger(['cal.ms'], tracker=tracker, logger=logger, config=config, method='poly',prefix='cal',
                                         sigma=4,it=1,degree=1,ncpu=8,timebin=10,datacolumn='CORRECTED_DATA')
                tracker.add_jobs('final_flagging_cal', job_final_flag_cal)

            if config['pipeline']['calibration'].get('applycal_targets'):
                logger.info("Submitting final flagging for target sources")
                # job_final_flag_src = general_flagger(
                #     ms_names=['src.ms'],
                #     mode='tfcrop,rflag',
                #     tracker=tracker,
                #     logger=logger,
                #     config=config,
                #     prefix='src',
                #     datacolumn='CORRECTED'
                # )
                job_final_flag_src =  nami_flagger(['src.ms'], tracker=tracker, logger=logger, config=config, method='poly',prefix='src',
                                         sigma=4,it=2,degree=3,ncpu=8,timebin=10,datacolumn='CORRECTED_DATA')
                tracker.add_jobs('final_flagging_src', job_final_flag_src)

            # Wait for calibrator flagging
            if job_final_flag_cal:
                cal_flag_successful, cal_flag_failed = wait_for_jobs_to_finish(
                    job_final_flag_cal,
                    config['general']['working_directory'],
                    logger,
                    'flag_nami_cal',
                    config['general']['PBS_or_SLURM']
                )

                if not tracker.check_brotherhood(cal_flag_failed):
                    logger.error(f"Final calibrator flagging failed for: {cal_flag_failed}")
                    cleanup_and_exit(job_final_flag_cal, config['general']['PBS_or_SLURM'], logger)
                    return

            # Wait for source flagging
            if job_final_flag_src:
                src_flag_successful, src_flag_failed = wait_for_jobs_to_finish(
                    job_final_flag_src,
                    config['general']['working_directory'],
                    logger,
                    'flag_nami_src',
                    config['general']['PBS_or_SLURM']
                )

                if not tracker.check_brotherhood(src_flag_failed):
                    logger.error(f"Final source flagging failed for: {src_flag_failed}")
                    cleanup_and_exit(job_final_flag_src, config['general']['PBS_or_SLURM'], logger)
                    return

        logger.info("First generation calibration complete")
        return True