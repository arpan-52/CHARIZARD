# charizard/charizard.py
"""
CHARIZARD - The Orchestrator
Arpan's style - be VOCAL about what's happening!

Pipeline is HARDCODED. Config only provides settings.

Flow:
1. Analyze MS, build calibration plan
2. Split (cal.ms, src.ms) - parallel hands only if no polcal
3. Bad antenna detection → writes badants.txt
4. Initial flagging (apply badants.txt)
5. RFI flagging on calibrators (catboss)
6. Find best refant ← AFTER flagging for clean data
7. Calibration round 1 + Source flagging (parallel)
8. Post-cal flagging (catboss + nimki)
9. Calibration round 2
10. Apply to both, final flagging
11. Diagnostic plots (if requested)
12. Imaging + Self-calibration (if configured)
13. DDCal (if configured)
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper

from .utils.general.ms_utils import get_ms_info
from .utils.general.source_utils import build_calibration_plan
from .utils.general.tracker import JobTracker
from .utils.general.jobs import wait_and_check
from .utils.splitting_utils.splitter import run_split
from .utils.flagging_utils.antenna_analysis import run_bad_antenna_detection, run_find_refant
from .utils.flagging_utils.initial_flagger import run_initial_flagging
from .utils.flagging_utils.catboss import run_catboss
from .utils.flagging_utils.nimki import run_nimki
from .utils.flagging_utils.flag_commands import write_flag_commands
from .utils.calibration_utils.gains import run_calibration
from .utils.calibration_utils.applycal import run_applycal


def charizard(config, logger, scheduler_config: Optional[str] = None,
              whitelist: List[str] = None) -> bool:
    """
    Run the full calibration pipeline.
    
    Returns True ONLY if ALL requested steps completed successfully.
    """
    whitelist = whitelist or []
    
    # Track pipeline status
    pipeline_status = {
        'completed_steps': [],
        'failed_steps': [],
        'warnings': [],
        'all_steps_requested': [],
    }
    
    # Setup housekeeper.
    #
    # jobs_dir here is only a DEFAULT: `job_dir` in the scheduler config file
    # overrides it. That matters because the scheduler writes its -o/-e output
    # to this directory, and not every filesystem that can hold the data can
    # also take scheduler output - on bhima, jobs write fine to /scratch but
    # their .out files are never delivered there. Pointing job_dir at a
    # different filesystem is the only fix, so it has to actually be honoured.
    #
    # Whichever path wins is also where wait_and_check() looks for logs, so the
    # two can no longer disagree. They used to: job_dir was parsed into
    # SchedulerConfig and then read by nothing, so setting it moved neither the
    # logs nor the search, and every job was reported as "No log files found".
    hk = Housekeeper(
        config=scheduler_config,
        jobs_dir=os.path.join(config.working_dir, "jobs"),
        scheduler=config.environment.get('scheduler', 'pbs')
    )
    logger.info(f"Job scripts and logs: {hk.jobs_dir}")

    # Get settings from config
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    setup = init_cal.get('setup', {})
    calibration = init_cal.get('calibration', {})
    control = calibration.get('control', {})
    
    brotherhood = setup.get('brotherhood', True)
    user_refant = calibration.get('refant')
    do_plotting = control.get('plot', False)
    
    # Build list of requested steps
    pipeline_status['all_steps_requested'] = [
        'analyze', 'split', 'badant', 'initial_flag', 
        'rfi_flag', 'refant', 'cal1', 'postcal_flag',
        'cal2', 'apply_targets', 'final_flag'
    ]
    if do_plotting:
        pipeline_status['all_steps_requested'].append('plotting')
    if flow.get('imaging_selfcal'):
        pipeline_status['all_steps_requested'].append('selfcal')
    if flow.get('dd_cal'):
        pipeline_status['all_steps_requested'].append('ddcal')
    
    # =========================================================================
    # STEP 1: ANALYZE MS
    # =========================================================================
    logger.step("ANALYZING MS")
    
    ms_info = get_ms_info(config.ms_path)
    
    logger.info(f"Fields: {len(ms_info['fields'])}")
    for fid, fdata in ms_info['fields'].items():
        logger.info(f"  [{fid}] {fdata['name']}")
    logger.info(f"SPWs: {ms_info['num_spws']}, Channels/SPW: {ms_info['num_channels']}")
    logger.info(f"Antennas: {ms_info['num_antennas']}")
    logger.info(f"Central freq: {ms_info['central_freq_hz']/1e9:.3f} GHz")
    logger.info(f"Correlations: {ms_info.get('corr_names', [])} ({ms_info.get('pol_basis', 'unknown')} basis)")
    
    # Build calibration plan
    cal_plan = build_calibration_plan(ms_info, config, logger)
    
    logger.info(f"Flux cal: {cal_plan['flux_cal']}")
    logger.info(f"Phase cal: {cal_plan['phase_cal']}")
    if cal_plan.get('leakage_cal'):
        logger.info(f"Leakage cal: {cal_plan['leakage_cal']}")
    if cal_plan.get('polangle_cal'):
        logger.info(f"Pol angle cal: {cal_plan['polangle_cal']}")
    logger.info(f"Targets: {', '.join(cal_plan['targets']) if cal_plan['targets'] else 'None'}")
    logger.info(f"All calibrators: {cal_plan['all_calibrators']}")

    if not cal_plan.get('flux_cal'):
        logger.error("No flux calibrator found in MS or overrides - cannot calibrate!")
        logger.error("Set sources.overrides.calibrators.amp in the config.")
        pipeline_status['failed_steps'].append('analyze')
        return False
    
    # Check if polcal possible
    do_polcal = (cal_plan.get('leakage_cal') and 
                 cal_plan.get('polangle_cal') and 
                 cal_plan.get('polangle_cal') in cal_plan.get('polcal_models', {}))
    
    if do_polcal:
        logger.info("Full polarization calibration enabled")
        logger.info("  -> Will split ALL correlations")
    else:
        logger.info("No polarization calibration")
        if ms_info.get('num_corrs', 4) == 4:
            basis = ms_info.get('pol_basis', 'unknown')
            if basis == 'circular':
                logger.info("  -> Will split only RR,LL (parallel hands)")
            elif basis == 'linear':
                logger.info("  -> Will split only XX,YY (parallel hands)")
            else:
                logger.info("  -> Will split all correlations (unknown basis)")
    
    # Initialize tracker
    tracker = JobTracker(config.target_spws, logger)
    pipeline_status['completed_steps'].append('analyze')
    
    # =========================================================================
    # STEP 2: SPLIT
    # =========================================================================
    logger.step("SPLITTING")
    
    success = run_split(
        hk=hk,
        config=config,
        ms_info=ms_info,
        cal_plan=cal_plan,
        tracker=tracker,
        logger=logger,
        whitelist=whitelist
    )
    
    if not success:
        pipeline_status['failed_steps'].append('split')
        if brotherhood:
            logger.error("Split failed - brotherhood enabled, stopping")
            return False
        logger.warning("Split had failures, continuing with remaining SPWs")
        pipeline_status['warnings'].append("Split had partial failures")
    
    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.error("No active SPWs!")
        pipeline_status['failed_steps'].append('split')
        return False
    
    logger.success(f"Split complete. Active SPWs: {active_spws}")
    pipeline_status['completed_steps'].append('split')
    
    # Write calplan file for later use (selfcal reads this for Stokes imaging)
    import yaml
    calplan_file = f"{config.ms_name}.calplan"
    calplan_data = {
        'flux_cal': cal_plan.get('flux_cal'),
        'phase_cal': cal_plan.get('phase_cal'),
        'leakage_cal': cal_plan.get('leakage_cal'),
        'polangle_cal': cal_plan.get('polangle_cal'),
        'leakage_cal_status': cal_plan.get('leakage_cal_status', 'unknown'),
        'targets': cal_plan.get('targets', []),
        'do_polcal': do_polcal,
        'num_correlations': 4 if do_polcal else 2,
        'correlation_names': ms_info.get('corr_names', []),
        'pol_basis': cal_plan.get('pol_basis', 'circular'),
        'gain_calibrators': cal_plan.get('gain_calibrators', []),
        'polangle_has_full_stokes_model': cal_plan.get('polangle_has_full_stokes_model', False),
        'active_spws': active_spws,
        'refant': None,  # Will be updated after refant step
    }
    with open(calplan_file, 'w') as f:
        yaml.dump(calplan_data, f, default_flow_style=False)
    logger.info(f"Wrote {calplan_file}")
    
    # =========================================================================
    # STEP 3: BAD ANTENNA DETECTION
    # =========================================================================
    logger.step("BAD ANTENNA DETECTION")
    
    active_spws = run_bad_antenna_detection(
        hk=hk,
        config=config,
        active_spws=active_spws,
        logger=logger,
        whitelist=whitelist
    )
    
    if active_spws is None:
        logger.error("Bad antenna detection failed completely!")
        pipeline_status['failed_steps'].append('badant')
        return False
    
    logger.success("Bad antenna detection complete")
    pipeline_status['completed_steps'].append('badant')
    
    # Write source flag commands (for later)
    for spw in active_spws:
        write_flag_commands(
            f"{spw}/source_flags.txt",
            mode='w',
            flags_to_include=['autocorr', 'clip', 'quack'],
            quack_interval=10
        )
    
    # =========================================================================
    # STEP 4: INITIAL FLAGGING
    # =========================================================================
    logger.step("INITIAL FLAGGING")
    
    # Apply badants.txt to cal.ms
    prev_spws = active_spws.copy()
    active_spws = run_initial_flagging(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        flag_file='badants.txt',
        logger=logger,
        whitelist=whitelist,
        prefix='cal'
    )
    
    if active_spws is None:
        logger.error("ALL SPWs failed initial flagging!")
        pipeline_status['failed_steps'].append('initial_flag')
        return False
    
    # Check if any SPWs failed
    failed_spws = set(prev_spws) - set(active_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, SPWs failed: {failed_spws}, stopping!")
            pipeline_status['failed_steps'].append('initial_flag')
            return False
        else:
            logger.warning(f"Removed failed SPWs: {failed_spws}, continuing with: {active_spws}")
    
    # Apply source_flags.txt to src.ms
    if cal_plan['targets']:
        run_initial_flagging(
            hk=hk,
            config=config,
            active_spws=active_spws,
            ms_names=['src.ms'],
            flag_file='source_flags.txt',
            logger=logger,
            whitelist=whitelist,
            prefix='src'
        )
    
    logger.success("Initial flagging complete")
    pipeline_status['completed_steps'].append('initial_flag')
    
    # =========================================================================
    # STEP 5: RFI FLAGGING ON CALIBRATORS (catboss)
    # =========================================================================
    logger.step("RFI FLAGGING - CALIBRATORS")
    
    prev_spws = active_spws.copy()
    active_spws = run_catboss(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        stage='initial',
        datacolumn='DATA',
        logger=logger,
        whitelist=whitelist,
        wait=True,
        prefix='cal'
    )
    
    if active_spws is None:
        logger.error("ALL SPWs failed RFI flagging!")
        pipeline_status['failed_steps'].append('rfi_flag')
        return False
    
    # Check if any SPWs failed
    failed_spws = set(prev_spws) - set(active_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, SPWs failed: {failed_spws}, stopping!")
            pipeline_status['failed_steps'].append('rfi_flag')
            return False
        else:
            logger.warning(f"Removed failed SPWs: {failed_spws}, continuing with: {active_spws}")
    
    logger.success("RFI flagging complete")
    pipeline_status['completed_steps'].append('rfi_flag')
    
    # =========================================================================
    # STEP 6: FIND BEST REFANT (after flagging!)
    # =========================================================================
    logger.step("FINDING BEST REFERENCE ANTENNA")
    
    if user_refant:
        refant = user_refant
        logger.info(f"Using user-specified refant: {refant}")
    else:
        _, refant = run_find_refant(
            hk=hk,
            config=config,
            active_spws=active_spws,
            logger=logger,
            whitelist=whitelist
        )
        
        if not refant:
            refant = ms_info['antennas'][0]
            logger.warning(f"Could not determine refant, using first antenna: {refant}")
            pipeline_status['warnings'].append(f"Refant auto-detection failed, using {refant}")
    
    cal_plan['refant'] = refant
    logger.success(f"Refant: {refant}")
    pipeline_status['completed_steps'].append('refant')
    
    # Update calplan with refant
    try:
        calplan_file = f"{config.ms_name}.calplan"
        with open(calplan_file, 'r') as f:
            calplan_data = yaml.safe_load(f)
        calplan_data['refant'] = refant
        calplan_data['active_spws'] = active_spws
        with open(calplan_file, 'w') as f:
            yaml.dump(calplan_data, f, default_flow_style=False)
        logger.info(f"Updated {calplan_file} with refant")
    except Exception as e:
        logger.warning(f"Could not update {calplan_file}: {e}")
    
    # =========================================================================
    # STEP 7: CALIBRATION ROUND 1 + SOURCE FLAGGING (PARALLEL)
    # =========================================================================
    logger.step("CALIBRATION ROUND 1")
    
    # Launch calibration (don't wait)
    cal_job_ids = run_calibration(
        hk=hk,
        config=config,
        active_spws=active_spws,
        cal_plan=cal_plan,
        refant=refant,
        cal_round=1,
        logger=logger,
        whitelist=whitelist,
        wait=False
    )
    
    # Launch source flagging in parallel
    src_flag_job_ids = []
    if cal_plan['targets']:
        logger.substep("Launching source flagging in parallel...")
        src_flag_job_ids = run_catboss(
            hk=hk,
            config=config,
            active_spws=active_spws,
            ms_names=['src.ms'],
            stage='initial',
            datacolumn='DATA',
            logger=logger,
            whitelist=whitelist,
            wait=False,
            prefix='src'
        )
        if src_flag_job_ids:
            logger.info(f"Source flagging running in background ({len(src_flag_job_ids)} jobs)")
    
    # Wait for calibration
    logger.substep("Waiting for calibration round 1...")
    if cal_job_ids:
        results = wait_and_check(hk, cal_job_ids, whitelist=whitelist, logger=logger)
        
        successful = []
        failed_spws = []
        for job_id, (job, log_result) in results.items():
            spw = job.job_subdir if hasattr(job, 'job_subdir') else 'unknown'
            if log_result.success:
                successful.append(spw)
                logger.info(f"{spw}: Calibration OK")
            else:
                failed_spws.append(spw)
                logger.error(f"{spw}: Calibration FAILED")
                if log_result.error_lines:
                    for err in log_result.error_lines[:3]:
                        logger.error(f"  >> {err}")
        
        if failed_spws:
            if brotherhood:
                logger.error(f"Brotherhood=True, SPWs failed: {failed_spws}, stopping!")
                pipeline_status['failed_steps'].append('cal1')
                return False
            else:
                logger.warning(f"Removing failed SPWs: {failed_spws}, continuing with: {successful}")
                active_spws = successful
        
        if not successful:
            logger.error("ALL SPWs failed calibration!")
            pipeline_status['failed_steps'].append('cal1')
            return False
    
    # Apply to calibrators
    prev_spws = active_spws.copy()
    active_spws = run_applycal(
        hk=hk,
        config=config,
        active_spws=active_spws,
        cal_plan=cal_plan,
        cal_round=1,
        target_type='calibrators',
        logger=logger,
        whitelist=whitelist
    )

    if active_spws is None:
        logger.error("ALL SPWs failed applycal (round 1)!")
        pipeline_status['failed_steps'].append('cal1')
        return False

    failed_spws = set(prev_spws) - set(active_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, SPWs failed applycal: {failed_spws}, stopping!")
            pipeline_status['failed_steps'].append('cal1')
            return False
        else:
            logger.warning(f"Removed failed SPWs: {failed_spws}, continuing with: {active_spws}")

    logger.success("Calibration round 1 complete")
    pipeline_status['completed_steps'].append('cal1')
    
    # =========================================================================
    # STEP 8: POST-CAL FLAGGING
    # =========================================================================
    logger.step("POST-CAL FLAGGING")
    
    # Catboss on corrected data
    run_catboss(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        stage='postcal',
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        prefix='postcal'
    )
    
    # NIMKI
    run_nimki(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        sigma=5.0,
        prefix='postcal'
    )
    
    logger.success("Post-cal flagging complete")
    pipeline_status['completed_steps'].append('postcal_flag')
    
    # =========================================================================
    # STEP 9: CALIBRATION ROUND 2
    # =========================================================================
    logger.step("CALIBRATION ROUND 2")
    
    prev_spws = active_spws.copy()
    active_spws = run_calibration(
        hk=hk,
        config=config,
        active_spws=active_spws,
        cal_plan=cal_plan,
        refant=refant,
        cal_round=2,
        logger=logger,
        whitelist=whitelist,
        wait=True
    )
    
    if active_spws is None:
        logger.error("ALL SPWs failed calibration round 2!")
        pipeline_status['failed_steps'].append('cal2')
        return False
    
    # Check if any SPWs failed
    failed_spws = set(prev_spws) - set(active_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, SPWs failed: {failed_spws}, stopping!")
            pipeline_status['failed_steps'].append('cal2')
            return False
        else:
            logger.warning(f"Removed failed SPWs: {failed_spws}, continuing with: {active_spws}")
    
    # Apply to calibrators
    prev_spws = active_spws.copy()
    active_spws = run_applycal(
        hk=hk,
        config=config,
        active_spws=active_spws,
        cal_plan=cal_plan,
        cal_round=2,
        target_type='calibrators',
        logger=logger,
        whitelist=whitelist
    )

    if active_spws is None:
        logger.error("ALL SPWs failed applycal (round 2)!")
        pipeline_status['failed_steps'].append('cal2')
        return False

    failed_spws = set(prev_spws) - set(active_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, SPWs failed applycal: {failed_spws}, stopping!")
            pipeline_status['failed_steps'].append('cal2')
            return False
        else:
            logger.warning(f"Removed failed SPWs: {failed_spws}, continuing with: {active_spws}")

    logger.success("Calibration round 2 complete")
    pipeline_status['completed_steps'].append('cal2')
    
    # =========================================================================
    # STEP 10: APPLY TO TARGETS + CHECK SOURCE FLAGGING
    # =========================================================================
    if cal_plan['targets']:
        logger.step("APPLY TO TARGETS")
        
        # Check if source flagging done
        if src_flag_job_ids:
            logger.substep("Checking if source flagging completed...")
            logger.info(f"Waiting for {len(src_flag_job_ids)} source flagging jobs...")
            results = wait_and_check(hk, src_flag_job_ids, whitelist=whitelist, timeout=300, logger=logger)
            
            src_flag_ok = 0
            src_flag_fail = 0
            for job_id, (job, log_result) in results.items():
                if log_result.success:
                    src_flag_ok += 1
                else:
                    src_flag_fail += 1
            
            logger.info(f"Source flagging: {src_flag_ok} OK, {src_flag_fail} failed")
            if src_flag_fail > 0:
                pipeline_status['warnings'].append(f"Source flagging: {src_flag_fail} jobs had issues")
        else:
            logger.info("No source flagging jobs were running")
        
        # Apply calibration to targets
        prev_spws = active_spws.copy()
        apply_result = run_applycal(
            hk=hk,
            config=config,
            active_spws=active_spws,
            cal_plan=cal_plan,
            cal_round=2,
            target_type='targets',
            logger=logger,
            whitelist=whitelist
        )

        if apply_result is None:
            logger.error("ALL SPWs failed applycal to targets!")
            pipeline_status['failed_steps'].append('apply_targets')
            return False

        failed_spws = set(prev_spws) - set(apply_result)
        if failed_spws:
            if brotherhood:
                logger.error(f"Brotherhood=True, SPWs failed applycal to targets: {failed_spws}, stopping!")
                pipeline_status['failed_steps'].append('apply_targets')
                return False
            else:
                logger.warning(f"Applycal to targets failed for: {failed_spws}, continuing with: {apply_result}")
                active_spws = apply_result

        logger.success("Applied to targets")
        pipeline_status['completed_steps'].append('apply_targets')
    
    # =========================================================================
    # STEP 11: FINAL FLAGGING
    # All catboss in parallel, then all nimki in parallel
    # =========================================================================
    logger.step("FINAL FLAGGING")
    
    # Catboss on ALL (cal.ms + src.ms) in parallel
    ms_to_flag = ['cal.ms']
    if cal_plan['targets']:
        ms_to_flag.append('src.ms')
    
    logger.substep(f"Running catboss on {ms_to_flag}...")
    run_catboss(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=ms_to_flag,
        stage='final',
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        prefix='final'
    )
    
    # NIMKI on calibrators only. Its UV-domain Gabor model assumes a compact,
    # well-behaved source; on a target field the sky structure is what we are
    # trying to image, so fitting and clipping against that model is not
    # appropriate. catboss pooh above already covers src.ms.
    logger.substep("Running NIMKI on ['cal.ms']...")
    run_nimki(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        sigma=5.0,
        prefix='final'
    )
    
    logger.success("Final flagging complete")
    pipeline_status['completed_steps'].append('final_flag')
    
    # =========================================================================
    # STEP 12: DIAGNOSTIC PLOTS (if requested)
    # =========================================================================
    if do_plotting:
        from .utils.plotting_utils.plotting import run_diagnostic_plots
        
        logger.step("GENERATING DIAGNOSTIC PLOTS")
        
        plots_ok = run_diagnostic_plots(
            hk=hk,
            config=config,
            active_spws=active_spws,
            cal_plan=cal_plan,
            do_polcal=do_polcal,
            logger=logger,
            whitelist=whitelist,
            plot_targets=bool(cal_plan['targets'])
        )

        if plots_ok:
            logger.success("Diagnostic plots complete")
            pipeline_status['completed_steps'].append('plotting')
        else:
            logger.error("Diagnostic plotting failed")
            pipeline_status['failed_steps'].append('plotting')
    
    # =========================================================================
    # STEP 13: IMAGING + SELF-CALIBRATION (if configured)
    # =========================================================================
    selfcal_success = True
    ms_map_result = None
    selfcal_final_images = None
    if flow.get('imaging_selfcal'):
        from .utils.selfcal_utils.prepare import prepare_selfcal_ms
        from .utils.selfcal_utils.imaging import run_dirty_image, run_wsclean
        from .utils.selfcal_utils.selfcal import run_selfcal_loop
        
        logger.step("IMAGING AND SELF-CALIBRATION")
        
        selfcal_config = flow.get('imaging_selfcal', {}).get('selfcal', {})
        loops_config = selfcal_config.get('loops', {})
        setup_config = flow.get('imaging_selfcal', {}).get('setup', {})
        
        selfcal_brotherhood = setup_config.get('brotherhood', True)
        selfcal_refant = loops_config.get('refant') or refant
        
        logger.info(f"Using refant for selfcal: {selfcal_refant}")
        logger.info(f"Selfcal brotherhood: {selfcal_brotherhood}")
        
        # Prepare MS
        freqbin = selfcal_config.get('freqbin', 10)
        
        ms_map = prepare_selfcal_ms(
            hk=hk,
            config=config,
            active_spws=active_spws,
            targets=cal_plan['targets'],
            freqbin=freqbin,
            logger=logger,
            whitelist=whitelist
        )
        
        if not ms_map:
            logger.error("Failed to prepare MS for selfcal!")
            pipeline_status['failed_steps'].append('selfcal')
            selfcal_success = False
            if selfcal_brotherhood:
                return False
        else:
            # Initial flagging on selfcal MS - use proper catboss (GPU) + nimki (CPU)
            if selfcal_config.get('avg_flag', True):
                logger.substep("Initial flagging on selfcal MS...")
                
                # Convert ms_map to format for run_catboss/run_nimki
                # ms_map: {field: [spw0/field/sc.ms, spw1/field/sc.ms, ...]}
                # Need: active_spws = [spw0, spw1, ...], ms_names = [field/sc.ms]
                spw_ms_map = {}
                for field, ms_list in ms_map.items():
                    for ms_path in ms_list:
                        parts = ms_path.split('/')
                        spw = parts[0]
                        ms_rel = '/'.join(parts[1:])  # field/sc.ms
                        if spw not in spw_ms_map:
                            spw_ms_map[spw] = []
                        if ms_rel not in spw_ms_map[spw]:
                            spw_ms_map[spw].append(ms_rel)
                
                active_spws_sc = list(spw_ms_map.keys())
                ms_names_sc = list(spw_ms_map.values())[0] if spw_ms_map else []
                
                if ms_names_sc:
                    # Catboss (GPU)
                    logger.substep("Running catboss on selfcal MS...")
                    result_spws = run_catboss(
                        hk=hk,
                        config=config,
                        active_spws=active_spws_sc,
                        ms_names=ms_names_sc,
                        stage='initial',
                        datacolumn='DATA',
                        logger=logger,
                        whitelist=whitelist,
                        wait=True,
                        prefix='sc_init'
                    )
                    
                    if result_spws is None:
                        logger.warning("Catboss failed on all SPWs")
                    else:
                        active_spws_sc = result_spws

                    # NIMKI is deliberately not run here. It is a calibrator-only
                    # flagger: its UV-domain Gabor model assumes a compact source,
                    # which is exactly what a target field is not.
                    logger.info("Initial selfcal flagging complete")
            
            # Dirty image if requested
            if flow.get('imaging_selfcal', {}).get('dirty_image', False):
                logger.substep("Creating dirty images...")
                dirty_result = run_dirty_image(
                    hk=hk,
                    config=config,
                    ms_map=ms_map,
                    logger=logger,
                    whitelist=whitelist
                )
                if not dirty_result:
                    logger.warning("Dirty imaging had issues")
            
            # Run selfcal loop
            ms_map_result = run_selfcal_loop(
                hk=hk,
                config=config,
                ms_map=ms_map,
                refant=selfcal_refant,
                logger=logger,
                whitelist=whitelist
            )
            
            if ms_map_result:
                logger.success("Self-calibration complete!")
                pipeline_status['completed_steps'].append('selfcal')
                
                # Store final image info for DDCal
                selfcal_final_images = ms_map_result
            else:
                logger.error("Self-calibration FAILED!")
                pipeline_status['failed_steps'].append('selfcal')
                selfcal_success = False
                selfcal_final_images = None
    
    # =========================================================================
    # STEP 14: DIRECTION-DEPENDENT CALIBRATION (if configured)
    # =========================================================================
    if flow.get('dd_cal') and not flow.get('imaging_selfcal'):
        logger.error("dd_cal requires imaging_selfcal to run first - skipping DDCal")
        pipeline_status['failed_steps'].append('ddcal')

    if flow.get('dd_cal') and flow.get('imaging_selfcal') and selfcal_success:
        from .utils.ddcal_utils.concat import concat_ms
        from .utils.ddcal_utils.pybdsf_runner import run_pybdsf
        from .utils.ddcal_utils.source_matcher import (
            load_pybdsf_catalog,
            find_bright_sources_and_write_regions
        )
        from .utils.ddcal_utils.peeling import run_ddcal_peeling
        
        logger.step("DIRECTION-DEPENDENT CALIBRATION (PEELING)")
        
        ddcal_config = flow.get('dd_cal', {})
        source_finding = ddcal_config.get('source_finding', {})
        
        flux_threshold = source_finding.get('flux_threshold_mJy', 50)
        region_radius = source_finding.get('region_radius_arcsec', 15.0)
        min_distance_fraction = source_finding.get('min_distance_fraction', 0.1)

        logger.info(f"Flux threshold: {flux_threshold} mJy")
        logger.info(f"Region radius: {region_radius} arcsec")
        logger.info(f"Inner exclusion: {min_distance_fraction:.0%} of image half-width")
        
        # Get ms_map from selfcal results
        if ms_map_result:
            selfcal_ms_map = ms_map_result
        else:
            logger.error("No selfcal MS available for DDCal")
            pipeline_status['failed_steps'].append('ddcal')
            selfcal_ms_map = None
        
        if selfcal_ms_map:
            # Check calplan for correlations
            calplan_file = f"{config.ms_name}.calplan"
            num_corrs = 2
            try:
                if os.path.exists(calplan_file):
                    with open(calplan_file, 'r') as f:
                        calplan = yaml.safe_load(f)
                    num_corrs = calplan.get('num_correlations', 2)
                    logger.info(f"Read {calplan_file}: {num_corrs} correlations")
            except Exception as e:
                logger.warning(f"Could not read {calplan_file}: {e}")
            
            # Step 1: Concat MS (no split if 2 corrs)
            logger.substep("Concatenating MS files...")
            combined_ms_map = concat_ms(
                hk=hk,
                config=config,
                ms_map=selfcal_ms_map,
                logger=logger,
                whitelist=whitelist)  # Only split if 4 correlations
            
            
            if not combined_ms_map:
                logger.error("MS concatenation failed")
                pipeline_status['failed_steps'].append('ddcal')
            else:
                # Step 2: Run PyBDSF on final Stokes I images
                stokes_i_images = {}
                for field in combined_ms_map.keys():
                    image_path = f"images/{field}/final_I_{field}-MFS-image.fits"
                    if os.path.exists(image_path):
                        stokes_i_images[field] = image_path
                        logger.info(f"Found Stokes I image for {field}")
                    else:
                        logger.warning(f"No Stokes I image for {field}")
                
                if not stokes_i_images:
                    logger.error("No Stokes I images found")
                    pipeline_status['failed_steps'].append('ddcal')
                else:
                    pybdsf_results = run_pybdsf(
                        hk=hk,
                        config=config,
                        image_map=stokes_i_images,
                        logger=logger,
                        whitelist=whitelist
                    )
                    
                    if not pybdsf_results:
                        logger.error("PyBDSF failed")
                        pipeline_status['failed_steps'].append('ddcal')
                    else:
                        # Step 3: Find bright sources and create region files
                        ddcal_failed_fields = []
                        for field in combined_ms_map.keys():
                            if field not in pybdsf_results:
                                logger.warning(f"No PyBDSF results for {field}")
                                ddcal_failed_fields.append(field)
                                continue
                            
                            logger.substep(f"Processing field {field}...")
                            
                            pybdsf_df = load_pybdsf_catalog(
                                pybdsf_results[field]['catalog'],
                                logger=logger
                            )
                            
                            output_dir = f"ddcal_output/{field}"
                            region_files = find_bright_sources_and_write_regions(
                                pybdsf_df=pybdsf_df,
                                output_dir=output_dir,
                                flux_threshold_mJy=flux_threshold,
                                region_radius_arcsec=region_radius,
                                image_file=stokes_i_images.get(field),
                                min_distance_fraction=min_distance_fraction,
                                logger=logger
                            )
                            
                            if not region_files:
                                logger.info(f"{field}: No sources to peel")
                                continue
                            
                            logger.info(f"{field}: {len(region_files)} sources to peel")
                            
                            # Get source list file
                            source_list_file = f"images/{field}/final_I_{field}-sources.txt"
                            
                            # Step 4: Run DDCal peeling
                            final_image = run_ddcal_peeling(
                                hk=hk,
                                config=config,
                                field=field,
                                region_files=region_files,
                                source_list_file=source_list_file,
                                ms_path=combined_ms_map[field],
                                logger=logger,
                                whitelist=whitelist
                            )
                            
                            if final_image:
                                logger.success(f"{field}: DDCal complete!")
                            else:
                                logger.error(f"{field}: DDCal failed")
                                ddcal_failed_fields.append(field)

                        if ddcal_failed_fields:
                            pipeline_status['failed_steps'].append('ddcal')
                            logger.error(f"DDCal failed for fields: {ddcal_failed_fields}")
                        else:
                            pipeline_status['completed_steps'].append('ddcal')
                            logger.success("DDCal complete!")
    # =========================================================================
    # PIPELINE SUMMARY
    # =========================================================================
    logger.info("")
    logger.info("=" * 60)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Completed steps: {pipeline_status['completed_steps']}")
    
    if pipeline_status['failed_steps']:
        logger.error(f"Failed steps: {pipeline_status['failed_steps']}")
    
    if pipeline_status['warnings']:
        logger.warning("Warnings during pipeline:")
        for w in pipeline_status['warnings']:
            logger.warning(f"  - {w}")
    
    logger.info(f"Active SPWs: {active_spws}")
    
    # Determine final status
    all_requested = set(pipeline_status['all_steps_requested'])
    all_completed = set(pipeline_status['completed_steps'])
    missing_steps = all_requested - all_completed
    
    if missing_steps:
        logger.error(f"Pipeline did NOT complete all requested steps!")
        logger.error(f"Missing: {missing_steps}")
        logger.warning("PIPELINE FINISHED WITH INCOMPLETE STEPS")
        return False
    else:
        logger.success("PIPELINE COMPLETE - ALL REQUESTED STEPS FINISHED!")
        return True
