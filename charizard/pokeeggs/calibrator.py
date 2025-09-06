import os
import time
import yaml
from .utils import *
from .rfi_remover import * 


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
    
    job_resources = calculate_job_resources(config, 'cross_calibration')
    active_spws = tracker.get_active_spws()
    
    if not active_spws:
        logger.warning("No active SPWs available for calibration")
        return []

    logger.info(f"Running calibration round {cal_round} for SPWs: {active_spws}")
    
    # Handle calibrator info and UV ranges
    calibrator_details = {}
    

    if config['msinfo'].get('calibrator_auto_detect', False):
        # Auto-detection mode: get calibrators from auto-detected file
        ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
        calibrator_file = f"{ms_basename}_calibrators.yaml"
        
        if not os.path.exists(calibrator_file):
            logger.warning(f"Calibrator file not found: {calibrator_file}, regenerating...")
            from .auto_detect_utils import auto_detect_calibrators
            auto_detect_calibrators(config, logger)
        
        if os.path.exists(calibrator_file):
            with open(calibrator_file, 'r') as f:
                cal_data = yaml.safe_load(f)
                amp_cal = cal_data.get('amp_cal', '')
                phase_cal = cal_data.get('phase_cal', '')
                calibrator_details = cal_data.get('calibrator_details', {})  # Get auto-detected UV ranges
        else:
            logger.error("Auto-detection enabled but calibrator file not found")
            raise FileNotFoundError("Auto-detection failed")
        
        # Polarization calibrators always from config
        leakage_cal = config['msinfo'].get('leakage_cal')
        polang_cal = config['msinfo'].get('polang_cal')

    else:
        # Config mode: get all calibrators from config
        amp_cal = config['msinfo']['amp_cal']
        phase_cal = config['msinfo'].get('phase_cal')
        leakage_cal = config['msinfo'].get('leakage_cal')
        polang_cal = config['msinfo'].get('polang_cal')
        # calibrator_details starts empty - no UV ranges unless user specifies
    
    do_polcal = leakage_cal and polang_cal and polang_cal in POLCAL_SOURCES
    # Apply user UV range overrides (works for both modes)
    config_uvranges = config['msinfo'].get('uvrange', {})

    if config_uvranges:
        logger.info("Processing UV range configuration")
        
        if isinstance(config_uvranges, dict):
            logger.info("Applying user UV range overrides")
            
            # Get all calibrator names for processing
            all_cal_names = []
            for cal in [amp_cal, phase_cal]:
                if cal:
                    all_cal_names.extend([c.strip() for c in cal.split(',') if c.strip()])
            
            # Add polarization calibrators if doing polcal
            if do_polcal:
                for cal in [polang_cal, leakage_cal]:
                    if cal:
                        cal_list = [c.strip() for c in cal.split(',') if c.strip()]
                        for c in cal_list:
                            if c not in all_cal_names:  # Avoid duplicates
                                all_cal_names.append(c)
            
            # Process UV range specifications
            for key, uvrange_str in config_uvranges.items():
                if uvrange_str is not None and isinstance(uvrange_str, str):
                    uvrange_value = uvrange_str.strip() if uvrange_str.strip() else None
                    
                    if key == "all":
                        # Apply to all calibrators
                        logger.info(f"Applying UV range '{uvrange_value}' to all calibrators")
                        for cal_name in all_cal_names:
                            if cal_name not in calibrator_details:
                                calibrator_details[cal_name] = {}
                            calibrator_details[cal_name]['uvrange'] = uvrange_value
                            logger.info(f"  UV range set for {cal_name}: {uvrange_value}")
                    
                    elif key == "amp_cal":
                        # Apply to amplitude calibrators
                        amp_cal_names = [c.strip() for c in amp_cal.split(',') if c.strip()]
                        logger.info(f"Applying UV range '{uvrange_value}' to amplitude calibrators: {amp_cal_names}")
                        for cal_name in amp_cal_names:
                            if cal_name not in calibrator_details:
                                calibrator_details[cal_name] = {}
                            calibrator_details[cal_name]['uvrange'] = uvrange_value
                            logger.info(f"  UV range set for amp_cal {cal_name}: {uvrange_value}")
                    
                    elif key == "phase_cal":
                        # Apply to phase calibrators
                        if phase_cal:
                            phase_cal_names = [c.strip() for c in phase_cal.split(',') if c.strip()]
                            logger.info(f"Applying UV range '{uvrange_value}' to phase calibrators: {phase_cal_names}")
                            for cal_name in phase_cal_names:
                                if cal_name not in calibrator_details:
                                    calibrator_details[cal_name] = {}
                                calibrator_details[cal_name]['uvrange'] = uvrange_value
                                logger.info(f"  UV range set for phase_cal {cal_name}: {uvrange_value}")
                        else:
                            logger.warning("phase_cal UV range specified but no phase calibrator defined")
                    
                    elif key == "leakage_cal":
                        # Apply to leakage calibrators
                        if leakage_cal:
                            leakage_cal_names = [c.strip() for c in leakage_cal.split(',') if c.strip()]
                            logger.info(f"Applying UV range '{uvrange_value}' to leakage calibrators: {leakage_cal_names}")
                            for cal_name in leakage_cal_names:
                                if cal_name not in calibrator_details:
                                    calibrator_details[cal_name] = {}
                                calibrator_details[cal_name]['uvrange'] = uvrange_value
                                logger.info(f"  UV range set for leakage_cal {cal_name}: {uvrange_value}")
                        else:
                            logger.warning("leakage_cal UV range specified but no leakage calibrator defined")
                    
                    elif key == "polang_cal":
                        # Apply to polarization angle calibrators
                        if polang_cal:
                            polang_cal_names = [c.strip() for c in polang_cal.split(',') if c.strip()]
                            logger.info(f"Applying UV range '{uvrange_value}' to polang calibrators: {polang_cal_names}")
                            for cal_name in polang_cal_names:
                                if cal_name not in calibrator_details:
                                    calibrator_details[cal_name] = {}
                                calibrator_details[cal_name]['uvrange'] = uvrange_value
                                logger.info(f"  UV range set for polang_cal {cal_name}: {uvrange_value}")
                        else:
                            logger.warning("polang_cal UV range specified but no polarization angle calibrator defined")
                    
                    else:
                        # Treat as specific source name
                        logger.info(f"Applying UV range '{uvrange_value}' to specific source: {key}")
                        if key not in calibrator_details:
                            calibrator_details[key] = {}
                        calibrator_details[key]['uvrange'] = uvrange_value
                        logger.info(f"  UV range set for source {key}: {uvrange_value}")
                
                elif uvrange_str == "":
                    # Empty string - user wants to clear UV range
                    if key in ["all", "amp_cal", "phase_cal", "leakage_cal", "polang_cal"]:
                        logger.info(f"Clearing UV ranges for {key} calibrators")
                        # Handle calibrator type clearing logic here if needed
                    else:
                        logger.info(f"Clearing UV range for source {key}")
                        if key not in calibrator_details:
                            calibrator_details[key] = {}
                        calibrator_details[key]['uvrange'] = None
                
                else:
                    logger.info(f"No UV range specified for {key}")
        
        else:
            logger.error(f"UV range configuration must be a dictionary, got {type(config_uvranges)}: {config_uvranges}")
            logger.error("Expected YAML format:")
            logger.error("  uvrange:")
            logger.error("    source_name: \">5klambda\"")
            logger.error("    amp_cal: \">10klambda\"")
            logger.error("    all: \">8klambda\"")
            raise ValueError(f"Invalid uvrange configuration format. Expected dict, got {type(config_uvranges)}")
    else:
        logger.info("No UV range overrides specified")
    refant = config['pipeline']['calibration']['refant']
    leakage_mode = config['pipeline']['calibration']['leakage_mode']
    
    # Check if we have valid polarization calibrators
    do_polcal = leakage_cal and polang_cal and polang_cal in POLCAL_SOURCES
    
    # Get unique list of calibrators
    all_calibrators = [amp_cal, phase_cal]
    if do_polcal:
        all_calibrators.extend([polang_cal, leakage_cal])
    field_list = get_unique_calibrators(all_calibrators)
    
    # Determine transfer sources (calibrators that are different from amp_cal)
    transfer_sources = [cal for cal in [phase_cal, polang_cal, leakage_cal] 
                      if cal and cal != amp_cal]
    transfer_list = get_unique_calibrators(transfer_sources)
    
    # Determine whether to run flux scaling
    do_fluxscale = len(transfer_list) > 0

    for spw in active_spws:
        casa_script = f"""
# Set flux density model for amplitude calibrator(s)
"""
        
        # Handle setjy for each amp calibrator
        amp_cal_list = [cal.strip() for cal in amp_cal.split(',') if cal.strip()]
        for amp_cal_name in amp_cal_list:
            casa_script += f"setjy(vis='{spw}/cal.ms',field='{amp_cal_name}')\n"
        
        casa_script += "\n# Delay calibration\n"
        
        # Handle delay calibration - separate calls for different UV ranges
        for i, amp_cal_name in enumerate(amp_cal_list):
            amp_cal_uvrange = calibrator_details.get(amp_cal_name, {}).get('uvrange')
            uvrange_param = f",uvrange='{amp_cal_uvrange}'" if amp_cal_uvrange else ""
            append_param = ",append=True" if i > 0 else ""
            
            casa_script += f"""gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/delays.cal{cal_round}',
        field='{amp_cal_name}',
        refant='{refant}',
        gaintype='K',
        solint='inf',
        combine='scan',
        minsnr=3{uvrange_param}{append_param})
"""

        casa_script += "# Initial phase calibration\n"
        
        # Handle initial phase calibration - only for amp calibrators
        for i, amp_cal_name in enumerate(amp_cal_list):
            amp_cal_uvrange = calibrator_details.get(amp_cal_name, {}).get('uvrange')
            uvrange_param = f",uvrange='{amp_cal_uvrange}'" if amp_cal_uvrange else ""
            append_param = ",append=True" if i > 0 else ""
            
            casa_script += f"""gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/phase_int.cal{cal_round}',
        field='{amp_cal_name}',
        refant='{refant}',
        gaintype='G',
        calmode='p',
        solint='int',
        minsnr=3{uvrange_param}{append_param})
"""

        casa_script += "\n"
        
        # Handle bandpass calibration - separate calls for different UV ranges
        casa_script += "# Bandpass calibration\n"
        for i, amp_cal_name in enumerate(amp_cal_list):
            amp_cal_uvrange = calibrator_details.get(amp_cal_name, {}).get('uvrange')
            uvrange_param = f",uvrange='{amp_cal_uvrange}'" if amp_cal_uvrange else ""
            append_param = ",append=True" if i > 0 else ""
            
            casa_script += f"""bandpass(vis='{spw}/cal.ms',
         caltable='{spw}/caltables/bandpass.cal{cal_round}',
         field='{amp_cal_name}',
         refant='{refant}',
         solint='inf',
         combine='scan',
         solnorm=True,
         minsnr=3,
         gaintable=['{spw}/caltables/delays.cal{cal_round}',
                    '{spw}/caltables/phase_int.cal{cal_round}']{uvrange_param}{append_param})
"""

        casa_script += "# Amplitude and phase calibration\n"
        
        # Handle amp/phase calibration - split by UV ranges for all calibrators in field_list
        all_cal_names = []
        for cal in [amp_cal, phase_cal]:
            if cal:
                all_cal_names.extend([c.strip() for c in cal.split(',') if c.strip()])

        # Add polarization calibrators if doing polcal
        if do_polcal:
            for cal in [polang_cal, leakage_cal]:
                if cal:
                    cal_list = [c.strip() for c in cal.split(',') if c.strip()]
                    for c in cal_list:
                        if c not in all_cal_names:  # Avoid duplicates
                            all_cal_names.append(c)
        
        for i, cal_name in enumerate(all_cal_names):
            cal_uvrange = calibrator_details.get(cal_name, {}).get('uvrange')
            uvrange_param = f",uvrange='{cal_uvrange}'" if cal_uvrange else ""
            append_param = ",append=True" if i > 0 else ""
            
            casa_script += f"""gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/amp_phase.cal{cal_round}',
        field='{cal_name}',
        refant='{refant}',
        gaintype='G',
        calmode='ap',
        solint='120s',
        minsnr=3,
        gaintable=['{spw}/caltables/delays.cal{cal_round}',
                   '{spw}/caltables/bandpass.cal{cal_round}']{uvrange_param}{append_param})
"""

        casa_script += "\n"

        # Only do flux scaling if there are transfer sources different from amp_cal
        if do_fluxscale:
            casa_script += f"""
# Flux calibration
fluxscale(vis='{spw}/cal.ms',
          caltable='{spw}/caltables/amp_phase.cal{cal_round}',
          fluxtable='{spw}/caltables/flux.cal{cal_round}',
          reference='{amp_cal}',
          transfer='{transfer_list}')
"""
            # The flux table to use in subsequent steps
            flux_table = f"'{spw}/caltables/flux.cal{cal_round}'"
        else:
            # If no flux scaling, use the amp_phase table directly
            logger.info(f"No transfer calibrators different from amp_cal for SPW {spw}, skipping fluxscale")
            flux_table = f"'{spw}/caltables/amp_phase.cal{cal_round}'"

        if do_polcal:
            polcal_data = POLCAL_SOURCES[polang_cal]
            
            # Add UV ranges for polarization calibration if available
            polang_uvrange = calibrator_details.get(polang_cal, {}).get('uvrange')
            polang_uvrange_param = f",uvrange='{polang_uvrange}'" if polang_uvrange else ""
            
            leakage_uvrange = calibrator_details.get(leakage_cal, {}).get('uvrange')
            leakage_uvrange_param = f",uvrange='{leakage_uvrange}'" if leakage_uvrange else ""
            
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
       gaintype='KCROSS',
       solint='inf',
       calmode='ap',
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                 '{spw}/caltables/bandpass.cal{cal_round}',
                 {flux_table}]{polang_uvrange_param})

polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/leakage.cal{cal_round}',
       field='{leakage_cal}',
       refant='{refant}',
       poltype='{leakage_mode}',
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                  '{spw}/caltables/bandpass.cal{cal_round}',
                  {flux_table},
                  '{spw}/caltables/delaycross.cal{cal_round}']{leakage_uvrange_param})

polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/polangle.cal{cal_round}',
       field='{polang_cal}',
       refant='{refant}',
       poltype='Xf',
       gaintable=['{spw}/caltables/delays.cal{cal_round}',
                  '{spw}/caltables/bandpass.cal{cal_round}',
                  {flux_table},
                  '{spw}/caltables/delaycross.cal{cal_round}',
                  '{spw}/caltables/leakage.cal{cal_round}']{polang_uvrange_param})
"""
        elif polang_cal and polang_cal not in POLCAL_SOURCES:
            logger.warning(f"Polarization angle calibrator {polang_cal} not found in known sources. Skipping polarization calibration.")

        # REST EXACTLY AS ORIGINAL
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
    job_resources = calculate_job_resources(config, 'cross_calibration')
    active_spws = tracker.get_active_spws()
    
    if not active_spws:
        logger.warning("No active SPWs available for applying calibration")
        return []
        
    # Handle auto-detection for field selection
    if config['msinfo'].get('calibrator_auto_detect', False):
        ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
        calibrator_file = f"{ms_basename}_calibrators.yaml"
        
        if os.path.exists(calibrator_file):
            import yaml
            with open(calibrator_file, 'r') as f:
                cal_data = yaml.safe_load(f)
                amp_cal = cal_data.get('amp_cal', '')
                phase_cal = cal_data.get('phase_cal', '')
                source_list = cal_data.get('source_list', '')
        else:
            # Fallback to config if file doesn't exist
            amp_cal = config['msinfo']['amp_cal']
            phase_cal = config['msinfo'].get('phase_cal')
            source_list = config['msinfo']['source_list']
    else:
        # Use config values
        amp_cal = config['msinfo']['amp_cal']
        phase_cal = config['msinfo'].get('phase_cal')
        source_list = config['msinfo']['source_list']
    
    # Always use config for polarization calibrators
    leakage_cal = config['msinfo'].get('leakage_cal')
    polang_cal = config['msinfo'].get('polang_cal')
    
    # Check if polarization calibration was done
    do_polcal = (leakage_cal and polang_cal and polang_cal in POLCAL_SOURCES)
    
    # Set up calibrator fields
    all_calibrators = [amp_cal]
    if phase_cal:
        all_calibrators.append(phase_cal)
    if do_polcal:
        if polang_cal:
            all_calibrators.append(polang_cal)
        if leakage_cal:
            all_calibrators.append(leakage_cal)
    
    calibrator_list = get_unique_calibrators(all_calibrators)
    
    # Determine if fluxscale was run based on transfer sources
    transfer_sources = [cal for cal in [phase_cal, polang_cal, leakage_cal] 
                      if cal and cal != amp_cal]
    do_fluxscale = len(transfer_sources) > 0
    
    logger.info(f"Applying calibration round {cal_round} for {target_type}")
    
    for spw in active_spws:
        # Basic gain tables - always include these
        gaintables = [
            f"{spw}/caltables/delays.cal{cal_round}",
            f"{spw}/caltables/bandpass.cal{cal_round}"
        ]
        
        # Add the appropriate flux/amp table
        if do_fluxscale:
            gaintables.append(f"{spw}/caltables/flux.cal{cal_round}")
        else:
            gaintables.append(f"{spw}/caltables/amp_phase.cal{cal_round}")
            logger.info(f"Using amp_phase.cal{cal_round} instead of flux.cal{cal_round} for SPW {spw}")
        
        # Add polarization calibration tables if applicable
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