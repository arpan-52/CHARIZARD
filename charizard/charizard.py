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
8. Post-cal flagging (catboss + nami)
9. Calibration round 2
10. Apply to both, final flagging
11. Diagnostic plots (if requested)
12. Imaging + Self-calibration (if configured)
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper

from .utils.general.ms_utils import get_ms_info
from .utils.general.source_utils import build_calibration_plan
from .utils.general.tracker import JobTracker
from .utils.splitting_utils.splitter import run_split
from .utils.flagging_utils.antenna_analysis import run_bad_antenna_detection, run_find_refant
from .utils.flagging_utils.initial_flagger import run_initial_flagging
from .utils.flagging_utils.catboss import run_catboss
from .utils.flagging_utils.nami import run_nami
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
    
    # Setup housekeeper
    hk = Housekeeper(
        config=scheduler_config,
        jobs_dir=os.path.join(config.working_dir, "jobs"),
        scheduler=config.environment.get('scheduler', 'pbs')
    )
    
    # Get settings from config
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    setup = init_cal.get('setup', {})
    flagging = init_cal.get('flagging', {})
    calibration = init_cal.get('calibration', {})
    control = calibration.get('control', {})
    
    brotherhood = setup.get('brotherhood', True)
    user_refant = calibration.get('refant')
    do_plotting = control.get('plot', False)
    
    # Build list of requested steps
    pipeline_status['all_steps_requested'] = ['analyze', 'split', 'badant', 'initial_flag', 
                                               'rfi_flag', 'refant', 'cal1', 'postcal_flag',
                                               'cal2', 'apply_targets', 'final_flag']
    if do_plotting:
        pipeline_status['all_steps_requested'].append('plotting')
    if flow.get('imaging_selfcal'):
        pipeline_status['all_steps_requested'].append('selfcal')
    
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
        pipeline_status['failed_steps'].append('initial_flag')
        if brotherhood:
            return False
    
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
        pipeline_status['failed_steps'].append('rfi_flag')
        if brotherhood:
            return False
    
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
        results = hk.wait_and_check(cal_job_ids, whitelist=whitelist)
        
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
            logger.warning(f"Calibration round 1 failed for: {failed_spws}")
            pipeline_status['warnings'].append(f"Cal1 failed for {failed_spws}")
        
        if not successful and brotherhood:
            pipeline_status['failed_steps'].append('cal1')
            return False
        
        active_spws = successful if successful else active_spws
    
    # Apply to calibrators
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
    
    # NAMI
    run_nami(
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
        pipeline_status['failed_steps'].append('cal2')
        if brotherhood:
            return False
    
    # Apply to calibrators
    run_applycal(
        hk=hk,
        config=config,
        active_spws=active_spws,
        cal_plan=cal_plan,
        cal_round=2,
        target_type='calibrators',
        logger=logger,
        whitelist=whitelist
    )
    
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
            results = hk.wait_and_check(src_flag_job_ids, whitelist=whitelist, timeout=300)
            
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
        run_applycal(
            hk=hk,
            config=config,
            active_spws=active_spws,
            cal_plan=cal_plan,
            cal_round=2,
            target_type='targets',
            logger=logger,
            whitelist=whitelist
        )
        
        logger.success("Applied to targets")
        pipeline_status['completed_steps'].append('apply_targets')
    
    # =========================================================================
    # STEP 11: FINAL FLAGGING
    # =========================================================================
    logger.step("FINAL FLAGGING")
    
    # On cal.ms
    run_catboss(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        stage='final',
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        prefix='final_cal'
    )
    
    run_nami(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=['cal.ms'],
        datacolumn='CORRECTED_DATA',
        logger=logger,
        whitelist=whitelist,
        sigma=5.0,
        prefix='final_cal'
    )
    
    # On src.ms
    if cal_plan['targets']:
        run_catboss(
            hk=hk,
            config=config,
            active_spws=active_spws,
            ms_names=['src.ms'],
            stage='final',
            datacolumn='CORRECTED_DATA',
            logger=logger,
            whitelist=whitelist,
            prefix='final_src'
        )
        
        run_nami(
            hk=hk,
            config=config,
            active_spws=active_spws,
            ms_names=['src.ms'],
            datacolumn='CORRECTED_DATA',
            logger=logger,
            whitelist=whitelist,
            sigma=5.0,
            prefix='final_src'
        )
    
    logger.success("Final flagging complete")
    pipeline_status['completed_steps'].append('final_flag')
    
    # =========================================================================
    # STEP 12: DIAGNOSTIC PLOTS (if requested)
    # =========================================================================
    if do_plotting:
        from .utils.plotting_utils.plotting import run_diagnostic_plots
        
        logger.step("GENERATING DIAGNOSTIC PLOTS")
        
        run_diagnostic_plots(
            hk=hk,
            config=config,
            active_spws=active_spws,
            cal_plan=cal_plan,
            do_polcal=do_polcal,
            logger=logger,
            whitelist=whitelist,
            plot_targets=bool(cal_plan['targets'])
        )
        
        logger.success("Diagnostic plots complete")
        pipeline_status['completed_steps'].append('plotting')
    
    # =========================================================================
    # STEP 13: IMAGING + SELF-CALIBRATION (if configured)
    # =========================================================================
    selfcal_success = True
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
            if not selfcal_brotherhood:
                return False
        else:
            # Initial flagging on selfcal MS
            if selfcal_config.get('avg_flag', True):
                logger.substep("Initial flagging on selfcal MS...")
                
                env = config.environment
                preamble = env.get('shell_preamble', '')
                resources = config.resources.get('flagging', config.resources.get('default', {}))
                ppn = resources.get('ppn', 8)
                
                flag_jobs = []
                flag_job_map = {}
                
                for field, ms_list in ms_map.items():
                    for ms_path in ms_list:
                        spw = ms_path.split('/')[0]
                        field_dir = f"{spw}/{field}"
                        
                        script = f'''#!/usr/bin/env python3
import subprocess
import os

ms = '{ms_path}'

# Catboss initial - sigma 6.0, combinations 1,2
cmd = f"catboss --cat pooh {{ms}} --combinations 1,2 --sigma 6.0 --rho 1.5 --poly-degree 5 --deviation-threshold 3.0 --datacolumn DATA --apply-flags --max-threads {ppn} --max-memory-usage 0.8 --verbose"
print(f"Running: {{cmd}}")
result = subprocess.run(cmd, shell=True)
if result.returncode != 0:
    print(f"WARNING: Catboss exited with code {{result.returncode}}")

# Remove lock
lock_file = os.path.join(ms, 'table.lock')
if os.path.exists(lock_file):
    os.remove(lock_file)

print("Initial flagging complete")
'''
                        script_file = f"scflag_{spw}_{field}.py"
                        with open(script_file, 'w') as f:
                            f.write(script)
                        
                        command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""
                        job = hk.submit(
                            command=command,
                            name=f"scflag_{spw}_{field}",
                            job_subdir=field_dir,
                            ppn=ppn,
                            walltime=resources.get('walltime', '02:00:00')
                        )
                        
                        if job.job_id:
                            flag_jobs.append(job.job_id)
                            flag_job_map[job.job_id] = (field, spw)
                        
                        time.sleep(0.3)
                
                if flag_jobs:
                    logger.substep(f"Waiting for {len(flag_jobs)} initial flagging jobs...")
                    results = hk.wait_and_check(flag_jobs, whitelist=whitelist)
                    
                    flag_ok = 0
                    flag_fail = 0
                    for job_id, (job, log_result) in results.items():
                        field, spw = flag_job_map.get(job_id, ('unknown', 'unknown'))
                        if log_result.success:
                            flag_ok += 1
                            logger.info(f"{spw}/{field}: OK")
                        else:
                            flag_fail += 1
                            logger.warning(f"{spw}/{field}: flagging had issues")
                            if log_result.error_lines:
                                for err in log_result.error_lines[:2]:
                                    logger.error(f"  >> {err}")
                    
                    logger.info(f"Initial flagging: {flag_ok} OK, {flag_fail} had issues")
            
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
            else:
                logger.error("Self-calibration FAILED!")
                pipeline_status['failed_steps'].append('selfcal')
                selfcal_success = False
    
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
