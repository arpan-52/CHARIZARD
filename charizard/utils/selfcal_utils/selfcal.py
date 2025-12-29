# charizard/utils/selfcal_utils/selfcal.py
"""
Self-calibration loop.

Brotherhood logic (same as entire pipeline):
- Housekeeper checks job success via logs + whitelist
- If any SPW fails and brotherhood=True → STOP
- If any SPW fails and brotherhood=False → Remove SPW, continue

Flow per round:
1. Flag (catboss + nami on DATA)
2. Image (wsclean)
3. Calibrate (gaincal + bandpass + applycal + mstransform)
"""

import os
import time
import yaml
from typing import List, Dict, Optional, Tuple

from housekeeper import Housekeeper


def get_solint_sequence(phase_rounds: int, ap_rounds: int, initial_solint: str) -> List[str]:
    """Generate solint sequence - constant for now."""
    initial_min = int(initial_solint.replace('min', '').replace('m', ''))
    sequence = [f"{max(initial_min, 1)}min"] * (phase_rounds + ap_rounds)
    return sequence


def get_niter_sequence(start_iters: int, total_rounds: int) -> List[int]:
    """Generate niter sequence. Doubles each round."""
    sequence = []
    current = start_iters
    for i in range(total_rounds):
        sequence.append(current)
        current *= 2
    return sequence


def run_flag_ms(hk: Housekeeper,
                config,
                ms_map: Dict[str, List[str]],
                round_name: str,
                logger,
                whitelist: List[str],
                brotherhood: bool) -> Optional[Dict[str, List[str]]]:
    """
    Run catboss + nami flagging on DATA column before imaging.
    
    Returns:
        Updated ms_map (failed SPWs removed if brotherhood=False), or None if brotherhood=True and failure
    """
    logger.substep(f"Flagging before {round_name}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('flagging', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    job_ids = []
    job_map = {}  # job_id -> (field, spw, ms_path)
    
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            if not os.path.exists(ms_path):
                continue
            
            parts = ms_path.split('/')
            spw = parts[0]
            field_dir = f"{spw}/{field}"
            
            script = f'''#!/usr/bin/env python3
# Pre-imaging flagging {round_name} for {spw}/{field}
import subprocess
import os

ms = '{ms_path}'

# Catboss on DATA - sigma=5.0, combinations=1,2
catboss_cmd = f"catboss --cat pooh {{ms}} --combinations 1,2 --sigma 5.0 --rho 1.5 --poly-degree 5 --deviation-threshold 3.0 --datacolumn DATA --apply-flags --max-threads {ppn} --max-memory-usage 0.8 --verbose"
print(f"Running: {{catboss_cmd}}")
subprocess.run(catboss_cmd, shell=True)

# Nami on DATA - sigma=5.0, timebin in MINUTES
nami_cmd = f"nami {{ms}} --datacolumn DATA --sigma 5.0 --nknots 3 --timebin 10 --ncpu {ppn}"
print(f"Running: {{nami_cmd}}")
subprocess.run(nami_cmd, shell=True)

# Remove lock
lock_file = os.path.join(ms, 'table.lock')
if os.path.exists(lock_file):
    os.remove(lock_file)

print("Flagging complete")
'''
            
            script_file = f"flag_{round_name}_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""
            
            job = hk.submit(
                command=command,
                name=f"flag_{round_name}_{spw}_{field}",
                job_subdir=field_dir,
                ppn=ppn,
                walltime=resources.get('walltime', '02:00:00')
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (field, spw, ms_path)
                logger.info(f"Submitted flag {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        return ms_map
    
    # Wait - housekeeper handles whitelist checking
    logger.substep(f"Waiting for {len(job_ids)} flagging jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Build new ms_map based on results
    new_ms_map = {field: [] for field in ms_map.keys()}
    
    for job_id, (job, log_result) in results.items():
        field, spw, ms_path = job_map.get(job_id, ('unknown', 'unknown', ''))
        
        if log_result.success:
            logger.info(f"{spw}/{field}: OK")
            new_ms_map[field].append(ms_path)
        else:
            logger.error(f"{spw}/{field}: FAILED")
            if brotherhood:
                logger.error("Brotherhood=True, stopping pipeline!")
                return None
            else:
                logger.warning(f"Removing {spw}/{field}, continuing with remaining")
    
    # Remove empty fields
    for field in list(new_ms_map.keys()):
        if not new_ms_map[field]:
            del new_ms_map[field]
    
    return new_ms_map if new_ms_map else None


def run_gaincal_bandpass(hk: Housekeeper,
                         config,
                         ms_map: Dict[str, List[str]],
                         refant: str,
                         solint: str,
                         calmode: str,
                         solnorm: bool,
                         round_name: str,
                         output_ms_suffix: str,
                         logger,
                         whitelist: List[str],
                         brotherhood: bool) -> Optional[Dict[str, List[str]]]:
    """
    Run gaincal + bandpass + applycal + mstransform for all fields in parallel.
    
    Returns:
        Updated ms_map with new MS paths, or None if brotherhood=True and failure
    """
    mode_name = 'phase' if calmode == 'p' else 'amp+phase'
    logger.substep(f"Running gaincal+bandpass ({mode_name}, solint={solint}, solnorm={solnorm})...")
    
    env = config.environment
    casa_path = env.get('casa_path', '')
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('selfcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}  # job_id -> (field, spw, output_ms_path)
    
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            if not os.path.exists(ms_path):
                continue
            
            parts = ms_path.split('/')
            spw = parts[0]
            field_dir = f"{spw}/{field}"
            
            input_ms = os.path.basename(ms_path)
            output_ms = output_ms_suffix
            output_ms_path = f"{field_dir}/{output_ms}"
            
            caltable_g = f"{field_dir}/selfcal-tables/{round_name}.g"
            caltable_b = f"{field_dir}/selfcal-tables/{round_name}.b"
            
            script = f'''# Selfcal {round_name} for {spw}/{field}
# calmode={calmode}, solnorm={solnorm}, solint={solint}

import os

vis = '{field_dir}/{input_ms}'
output = '{field_dir}/{output_ms}'

# Create selfcal-tables directory
os.makedirs('{field_dir}/selfcal-tables', exist_ok=True)

print(f"Input: {{vis}}")
print(f"Output: {{output}}")

# Gaincal
print("Running gaincal...")
gaincal(
    vis=vis,
    caltable='{caltable_g}',
    field='',
    spw='',
    solint='{solint}',
    refant='{refant}',
    minsnr=2.0,
    gaintype='G',
    calmode='{calmode}',
    solnorm={solnorm}
)

# Check gaincal
if not os.path.exists('{caltable_g}'):
    raise RuntimeError("Gaincal failed - no caltable!")

# Bandpass
print("Running bandpass...")
bandpass(
    vis=vis,
    caltable='{caltable_b}',
    field='',
    spw='',
    solint='inf',
    refant='{refant}',
    minsnr=3.0,
    gaintable=['{caltable_g}'],
    solnorm={solnorm}
)

# Check bandpass
if not os.path.exists('{caltable_b}'):
    raise RuntimeError("Bandpass failed - no caltable!")

# Applycal
print("Running applycal...")
applycal(
    vis=vis,
    gaintable=['{caltable_g}', '{caltable_b}'],
    applymode='calflag',
    flagbackup=True
)

# Mstransform
print("Running mstransform...")
mstransform(
    vis=vis,
    outputvis=output,
    datacolumn='corrected'
)

# Check output
if not os.path.exists(output):
    raise RuntimeError("Mstransform failed - no output!")

print(f"SUCCESS: {round_name} complete")
'''
            
            script_file = f"selfcal_{round_name}_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
            
            job = hk.submit(
                command=command,
                name=f"selfcal_{round_name}_{spw}_{field}",
                job_subdir=field_dir,
                ppn=ppn,
                walltime=resources.get('walltime', '02:00:00')
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (field, spw, output_ms_path)
                logger.info(f"Submitted {round_name} {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        logger.error("No gaincal jobs submitted")
        return None
    
    # Wait - housekeeper handles whitelist
    logger.substep(f"Waiting for {len(job_ids)} gaincal+bandpass jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Build new ms_map
    new_ms_map = {field: [] for field in ms_map.keys()}
    
    for job_id, (job, log_result) in results.items():
        field, spw, output_ms_path = job_map.get(job_id, ('unknown', 'unknown', ''))
        
        if log_result.success:
            logger.info(f"{spw}/{field}: OK")
            new_ms_map[field].append(output_ms_path)
        else:
            logger.error(f"{spw}/{field}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
            
            if brotherhood:
                logger.error("Brotherhood=True, stopping pipeline!")
                return None
            else:
                logger.warning(f"Removing {spw}/{field}, continuing with remaining")
    
    # Remove empty fields
    for field in list(new_ms_map.keys()):
        if not new_ms_map[field]:
            logger.error(f"Field {field}: ALL SPWs failed!")
            del new_ms_map[field]
    
    return new_ms_map if new_ms_map else None


def run_selfcal_loop(hk: Housekeeper,
                     config,
                     ms_map: Dict[str, List[str]],
                     refant: str,
                     logger,
                     whitelist: List[str]) -> Optional[Dict[str, List[str]]]:
    """
    Run full self-calibration loop.
    
    Flow per round:
    1. Flag (catboss + nami on DATA)
    2. Image (wsclean)
    3. Calibrate (gaincal + bandpass + applycal + mstransform)
    """
    from .imaging import run_wsclean
    
    # Get config
    flow = config.flow
    selfcal_config = flow.get('imaging_selfcal', {}).get('selfcal', {})
    loops_config = selfcal_config.get('loops', {})
    clean_config = selfcal_config.get('clean', {})
    setup_config = flow.get('imaging_selfcal', {}).get('setup', {})
    
    phase_rounds = loops_config.get('phase', 4)
    ap_rounds = loops_config.get('amp_phase', 2)
    initial_solint = loops_config.get('solint', '4min')
    brotherhood = setup_config.get('brotherhood', True)
    
    start_iters = clean_config.get('start_iters', 1000)
    
    # Get sequences
    solint_sequence = get_solint_sequence(phase_rounds, ap_rounds, initial_solint)
    total_rounds = phase_rounds + ap_rounds
    niter_sequence = get_niter_sequence(start_iters, total_rounds + 2)
    
    logger.info(f"Selfcal: {phase_rounds} phase + {ap_rounds} ap rounds")
    logger.info(f"Solint: {initial_solint}")
    logger.info(f"Niter sequence: {niter_sequence[:total_rounds+1]}")
    logger.info(f"Brotherhood: {brotherhood}")
    
    round_idx = 0
    
    # =========================================================================
    # PHASE CALIBRATION ROUNDS
    # =========================================================================
    for i in range(phase_rounds):
        round_num = i + 1
        solint = solint_sequence[i]
        niter = niter_sequence[round_idx]
        output_suffix = f"pcal{round_num}.ms"
        
        logger.substep(f"=== Phase cal round {round_num}/{phase_rounds} (solint={solint}, niter={niter}) ===")
        
        # 1. Flag
        ms_map = run_flag_ms(hk, config, ms_map, f'pcal{round_num}', logger, whitelist, brotherhood)
        if ms_map is None:
            return None
        
        # 2. Image
        run_wsclean(hk, config, ms_map, niter, f'pcal{round_num}', logger, whitelist, datacolumn='DATA')
        
        # 3. Calibrate
        ms_map = run_gaincal_bandpass(
            hk, config, ms_map, refant, solint, 'p', True,
            f'pcal{round_num}', output_suffix, logger, whitelist, brotherhood
        )
        if ms_map is None:
            return None
        
        round_idx += 1
    
    # =========================================================================
    # AMP+PHASE CALIBRATION ROUNDS
    # =========================================================================
    for i in range(ap_rounds):
        round_num = i + 1
        solint = solint_sequence[phase_rounds + i]
        niter = niter_sequence[round_idx]
        output_suffix = f"apcal{round_num}.ms"
        
        logger.substep(f"=== Amp+phase cal round {round_num}/{ap_rounds} (solint={solint}, niter={niter}) ===")
        
        # 1. Flag
        ms_map = run_flag_ms(hk, config, ms_map, f'apcal{round_num}', logger, whitelist, brotherhood)
        if ms_map is None:
            return None
        
        # 2. Image
        run_wsclean(hk, config, ms_map, niter, f'apcal{round_num}', logger, whitelist, datacolumn='DATA')
        
        # 3. Calibrate
        ms_map = run_gaincal_bandpass(
            hk, config, ms_map, refant, solint, 'ap', False,
            f'apcal{round_num}', output_suffix, logger, whitelist, brotherhood
        )
        if ms_map is None:
            return None
        
        round_idx += 1
    
    # =========================================================================
    # FINAL IMAGE
    # =========================================================================
    logger.substep("=== Creating final selfcal images ===")
    final_niter = niter_sequence[min(round_idx, len(niter_sequence)-1)] * 2
    
    # Final flagging
    ms_map = run_flag_ms(hk, config, ms_map, 'final', logger, whitelist, brotherhood)
    if ms_map is None:
        return None
    
    # Read .calplan for Stokes
    num_corrs = 2
    try:
        if os.path.exists('.calplan'):
            with open('.calplan', 'r') as f:
                calplan = yaml.safe_load(f)
            num_corrs = calplan.get('num_correlations', 2)
            logger.info(f"Read .calplan: {num_corrs} correlations")
    except Exception as e:
        logger.warning(f"Could not read .calplan: {e}")
    
    if num_corrs >= 4:
        stokes_list = ['I', 'Q', 'U', 'V']
        logger.info("4 correlations - imaging I, Q, U, V")
    else:
        stokes_list = ['I']
        logger.info("2 correlations - imaging only Stokes I")
    
    for stokes in stokes_list:
        save_sources = (stokes == 'I')
        logger.info(f"Imaging Stokes {stokes} (save_sources={save_sources})...")
        
        run_wsclean(
            hk, config, ms_map, final_niter, f'final_{stokes}',
            logger, whitelist, datacolumn='DATA', use_masks=True,
            stokes=stokes, save_source_list=save_sources
        )
    
    logger.info(f"Selfcal complete. Remaining fields: {list(ms_map.keys())}")
    return ms_map
