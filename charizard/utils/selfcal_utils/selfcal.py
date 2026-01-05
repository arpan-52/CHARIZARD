# charizard/utils/selfcal_utils/selfcal.py
"""
Self-calibration loop.

Flow per round:
1. Catboss (GPU, parallel) on DATA
2. Nami (CPU, parallel) on DATA  
3. Image (wsclean)
4. Calibrate (gaincal + bandpass + applycal + mstransform)

Brotherhood: if ANY SPW fails and brotherhood=True → STOP
"""

import os
import time
import yaml
from typing import List, Dict, Optional

from housekeeper import Housekeeper

# Import existing flagging functions
from ..flagging_utils.catboss import run_catboss
from ..flagging_utils.nami import run_nami


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


def get_threshold_sequence(start_threshold: float, total_rounds: int) -> List[float]:
    """Generate threshold sequence. Decreases by 1.5x each round."""
    sequence = []
    current = start_threshold
    for i in range(total_rounds):
        sequence.append(current)
        current = current / 1.5
    return sequence

def run_selfcal_flagging(hk: Housekeeper,
                         config,
                         ms_map: Dict[str, List[str]],
                         round_name: str,
                         logger,
                         whitelist: List[str],
                         brotherhood: bool) -> Optional[Dict[str, List[str]]]:
    """
    Run catboss (GPU) then nami (CPU) on selfcal MS files.
    Uses existing run_catboss and run_nami functions.
    
    Returns:
        Updated ms_map, or None if failure and brotherhood=True
    """
    # Convert ms_map to list of (spw, ms_path) for flagging
    # ms_map: {field: [spw0/field/sc.ms, spw1/field/sc.ms, ...]}
    
    # Get unique SPWs and build spw -> ms_name mapping
    spw_ms_map = {}  # spw -> [ms_names relative to spw dir]
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            # ms_path = spw0/G71+28/sc.ms
            parts = ms_path.split('/')
            spw = parts[0]
            # For selfcal, we need to flag field-specific MS
            # The path relative to spw would be: field/sc.ms
            ms_rel = '/'.join(parts[1:])  # G71+28/sc.ms
            
            if spw not in spw_ms_map:
                spw_ms_map[spw] = []
            if ms_rel not in spw_ms_map[spw]:
                spw_ms_map[spw].append(ms_rel)
    
    active_spws = list(spw_ms_map.keys())
    
    # Get list of MS names (should be same for all SPWs)
    # e.g., ['G71+28/sc.ms'] or ['G71+28/pcal1.ms']
    ms_names = list(spw_ms_map.values())[0] if spw_ms_map else []
    
    if not ms_names:
        logger.warning("No MS files to flag")
        return ms_map
    
    prev_spws = active_spws.copy()
    
    # Step 1: Catboss (GPU)
    logger.substep(f"Catboss for {round_name}...")
    result_spws = run_catboss(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=ms_names,
        stage='postcal',  # Use postcal settings (sigma=5, combinations=1,2,4,8)
        datacolumn='DATA',
        logger=logger,
        whitelist=whitelist,
        wait=True,
        prefix=f'sc_{round_name}'
    )
    
    if result_spws is None:
        logger.error("ALL SPWs failed catboss!")
        return None
    
    # Check for failures
    failed_spws = set(prev_spws) - set(result_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, catboss failed for: {failed_spws}, stopping!")
            return None
        else:
            logger.warning(f"Catboss failed for {failed_spws}, continuing with {result_spws}")
    
    active_spws = result_spws
    prev_spws = active_spws.copy()
    
    # Step 2: Nami (CPU)
    logger.substep(f"Nami for {round_name}...")
    result_spws = run_nami(
        hk=hk,
        config=config,
        active_spws=active_spws,
        ms_names=ms_names,
        datacolumn='DATA',
        logger=logger,
        whitelist=whitelist,
        sigma=5.0,
        prefix=f'sc_{round_name}'
    )
    
    if result_spws is None:
        logger.error("ALL SPWs failed nami!")
        return None
    
    # Check for failures
    failed_spws = set(prev_spws) - set(result_spws)
    if failed_spws:
        if brotherhood:
            logger.error(f"Brotherhood=True, nami failed for: {failed_spws}, stopping!")
            return None
        else:
            logger.warning(f"Nami failed for {failed_spws}, continuing with {result_spws}")
    
    # Build updated ms_map with only successful SPWs
    new_ms_map = {}
    for field, ms_list in ms_map.items():
        new_list = []
        for ms_path in ms_list:
            spw = ms_path.split('/')[0]
            if spw in result_spws:
                new_list.append(ms_path)
        if new_list:
            new_ms_map[field] = new_list
    
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
                logger.warning(f"Removing {spw}/{field}, continuing")
    
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
    1. Catboss (GPU, parallel)
    2. Nami (CPU, parallel)
    3. Image (wsclean)
    4. Calibrate (gaincal + bandpass + applycal + mstransform)
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
        
        # 1. Flag (catboss + nami)
        ms_map = run_selfcal_flagging(hk, config, ms_map, f'pcal{round_num}', logger, whitelist, brotherhood)
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
        
        # 1. Flag (catboss + nami)
        ms_map = run_selfcal_flagging(hk, config, ms_map, f'apcal{round_num}', logger, whitelist, brotherhood)
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
    ms_map = run_selfcal_flagging(hk, config, ms_map, 'final', logger, whitelist, brotherhood)
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
