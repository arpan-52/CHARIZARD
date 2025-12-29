# charizard/utils/selfcal_utils/selfcal.py
"""
Self-calibration loop.

Each SPW/field processed in parallel.
Brotherhood: if one SPW fails for a field, continue with others.
If ALL SPWs fail for a field, drop that field (if brotherhood=False, stop).

Flow:
1. Phase cal rounds (gaincal + bandpass, solnorm=True, calmode='p')
2. Catalog calibration (if use_catalogs=True): foresight + crystalball + quartical
3. Amp+phase cal rounds (gaincal + bandpass, solnorm=False, calmode='ap')
4. Final image
"""

import os
import time
from typing import List, Dict, Optional, Tuple

from housekeeper import Housekeeper


def get_solint_sequence(phase_rounds: int, ap_rounds: int, initial_solint: str) -> List[str]:
    """
    Generate solint sequence for selfcal.
    Halves solint each round, floor at 1min.
    """
    initial_min = int(initial_solint.replace('min', '').replace('m', ''))
    
    sequence = []
    current = initial_min
    
    # Phase rounds
    for _ in range(phase_rounds):
        sequence.append(f"{max(current, 1)}min")
        current = current // 2
    
    # Amp+phase rounds - continues from where phase left off
    for _ in range(ap_rounds):
        sequence.append(f"{max(current, 1)}min")
        current = current // 2
    
    return sequence


def get_niter_sequence(start_iters: int, total_rounds: int) -> List[int]:
    """
    Generate niter sequence. Doubles each round.
    """
    sequence = []
    current = start_iters
    for i in range(total_rounds):
        if i == 0:
            sequence.append(current)
        else:
            current *= 2
            sequence.append(current)
    return sequence


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
                         brotherhood: bool = True) -> Tuple[Optional[Dict[str, List[str]]], List[str]]:
    """
    Run gaincal + bandpass for all fields in parallel.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        ms_map: Dict mapping field -> list of MS paths
        refant: Reference antenna
        solint: Solution interval
        calmode: 'p' for phase, 'ap' for amp+phase
        solnorm: Normalize solutions (True for phase, False for ap)
        round_name: Round name for naming (e.g., 'pcal1', 'apcal1')
        output_ms_suffix: Output MS suffix (e.g., 'pcal2.ms')
        logger: Logger
        whitelist: Error whitelist
        brotherhood: If False, stop on any failure
    
    Returns:
        (updated_ms_map, failed_fields)
    """
    mode_name = 'phase' if calmode == 'p' else 'amp+phase'
    logger.substep(f"Running gaincal+bandpass ({mode_name}, solint={solint}, solnorm={solnorm})...")
    
    env = config.environment
    casa_path = env.get('casa_path', '')
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('selfcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}  # job_id -> (field, spw, input_ms, output_ms)
    
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            if not os.path.exists(ms_path):
                continue
            
            # Get SPW from path: spw0/field/sc.ms -> spw0
            parts = ms_path.split('/')
            spw = parts[0]
            field_dir = f"{spw}/{field}"
            
            # Input/output MS
            input_ms = os.path.basename(ms_path)
            output_ms = output_ms_suffix
            
            caltable_g = f"{field_dir}/selfcal-tables/{round_name}.g"
            caltable_b = f"{field_dir}/selfcal-tables/{round_name}.b"
            
            # CASA script - gaincal + bandpass + applycal + mstransform
            script = f'''# Selfcal {round_name} for {spw}/{field}
# calmode={calmode}, solnorm={solnorm}, solint={solint}

import os
import shutil

vis = '{field_dir}/{input_ms}'
output = '{field_dir}/{output_ms}'

# Gaincal
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

# Bandpass
bandpass(
    vis=vis,
    caltable='{caltable_b}',
    field='',
    spw='',
    solint='inf',
    refant='{refant}',
    minsnr=2.0,
    gaintable=['{caltable_g}'],
    solnorm={solnorm}
)

# Applycal
applycal(
    vis=vis,
    gaintable=['{caltable_g}', '{caltable_b}'],
    applymode='calflag',
    flagbackup=True
)

# Extract corrected data to new MS
mstransform(
    vis=vis,
    outputvis=output,
    datacolumn='corrected'
)

# Remove old MS to save space
if os.path.exists(output):
    shutil.rmtree(vis)
    print(f"Removed {{vis}}, output: {{output}}")
else:
    print(f"ERROR: Output MS not created!")

print("Calibration complete: {round_name}")
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
                job_map[job.job_id] = (field, spw, ms_path, f"{field_dir}/{output_ms}")
                logger.info(f"Submitted {round_name} {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        logger.error("No gaincal jobs submitted")
        return None, list(ms_map.keys())
    
    # Wait for all jobs
    logger.substep(f"Waiting for {len(job_ids)} gaincal+bandpass jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Build new MS map and track failures
    new_ms_map = {field: [] for field in ms_map.keys()}
    field_failures = {field: 0 for field in ms_map.keys()}
    field_totals = {field: len(ms_list) for field, ms_list in ms_map.items()}
    
    for job_id, (job, log_result) in results.items():
        field, spw, input_ms, output_ms = job_map.get(job_id, ('unknown', 'unknown', '', ''))
        
        if log_result.success and os.path.exists(output_ms):
            new_ms_map[field].append(output_ms)
            logger.info(f"{spw}/{field}: OK")
        else:
            field_failures[field] += 1
            logger.warning(f"{spw}/{field}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
    
    # Check which fields completely failed
    failed_fields = []
    for field in ms_map.keys():
        if len(new_ms_map[field]) == 0:
            failed_fields.append(field)
            logger.error(f"Field {field}: ALL SPWs failed!")
            del new_ms_map[field]
        elif field_failures[field] > 0:
            logger.warning(f"Field {field}: {field_failures[field]}/{field_totals[field]} SPWs failed, continuing with {len(new_ms_map[field])}")
    
    if not new_ms_map:
        return None, failed_fields
    
    return new_ms_map, failed_fields


def run_catalog_calibration(hk: Housekeeper,
                            config,
                            ms_map: Dict[str, List[str]],
                            refant: str,
                            logger,
                            whitelist: List[str],
                            brotherhood: bool = True) -> Tuple[Optional[Dict[str, List[str]]], List[str]]:
    """
    Run catalog-based calibration using foresight + crystalball + quartical.
    
    Flow per MS:
    1. foresight: find sources, create mask
    2. crystalball: predict model visibilities
    3. quartical: phase calibration
    4. mstransform: extract corrected data
    """
    logger.substep("Running catalog-based calibration (foresight + crystalball + quartical)...")
    
    env = config.environment
    casa_path = env.get('casa_path', '')
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('selfcal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    # Get imaging params
    flow = config.flow
    selfcal_config = flow.get('imaging_selfcal', {}).get('selfcal', {})
    imaging_config = selfcal_config.get('imaging', {})
    
    imsize = imaging_config.get('imsize', 4096)
    cellsize = imaging_config.get('cellsize', '1asec').replace('asec', '').replace('arcsec', '')
    
    job_ids = []
    job_map = {}
    
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            if not os.path.exists(ms_path):
                continue
            
            parts = ms_path.split('/')
            spw = parts[0]
            field_dir = f"{spw}/{field}"
            
            input_ms = os.path.basename(ms_path)
            output_ms = 'catalog1.ms'
            
            mask_dir = f"{field_dir}/masks"
            os.makedirs(mask_dir, exist_ok=True)
            
            mask_file = f"{mask_dir}/{field}_mask.fits"
            source_list_file = f"{mask_dir}/{field}_sources.txt"
            
            # Get refant index
            script = f'''#!/bin/bash
cd {os.getcwd()}
{preamble}

echo "=== Catalog calibration for {spw}/{field} ==="

# 1. Foresight - find sources and create mask
echo "Running foresight..."
foresight {field_dir}/{input_ms} \\
    --imsize {imsize} \\
    --cellsize {cellsize} \\
    --source-types S,M,U,L,C,I \\
    -o {source_list_file} \\
    -m {mask_file}

# 2. Crystalball - predict model
echo "Running crystalball..."
crystalball {field_dir}/{input_ms} \\
    -sm {source_list_file} \\
    -j 10 \\
    -o MODEL_DATA

# 3. Quartical - calibrate
echo "Running quartical..."
cd {field_dir}
goquartical \\
    input_ms.path={input_ms} \\
    input_ms.data_column=DATA \\
    input_model.recipe=MODEL_DATA \\
    solver.terms=[G] \\
    G.type=phase \\
    solver.iter_recipe=[50] \\
    solver.convergence_fraction=0.95 \\
    G.time_interval=120 \\
    G.freq_interval=0 \\
    output.products=[corrected_data] \\
    output.columns=[CORRECTED_DATA] \\
    output.overwrite=True

# 4. Extract corrected data
echo "Extracting corrected data..."
{casa_path}/bin/casa --nologger --nogui -c "
mstransform(vis='{input_ms}',
          outputvis='{output_ms}',
          datacolumn='corrected')
import shutil
import os
if os.path.exists('{output_ms}'):
    shutil.rmtree('{input_ms}')
"

cd {os.getcwd()}
echo "Catalog calibration complete for {spw}/{field}"
'''
            
            script_file = f"catcal_{spw}_{field}.sh"
            with open(script_file, 'w') as f:
                f.write(script)
            os.chmod(script_file, 0o755)
            
            command = f"bash {os.getcwd()}/{script_file}"
            
            job = hk.submit(
                command=command,
                name=f"catcal_{spw}_{field}",
                job_subdir=field_dir,
                ppn=ppn,
                walltime=resources.get('walltime', '04:00:00')
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (field, spw, ms_path, f"{field_dir}/{output_ms}")
                logger.info(f"Submitted catcal {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        logger.error("No catalog cal jobs submitted")
        return ms_map, []  # Return original, continue without catalog
    
    # Wait
    logger.substep(f"Waiting for {len(job_ids)} catalog cal jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Build new MS map
    new_ms_map = {field: [] for field in ms_map.keys()}
    failed_fields = []
    
    for job_id, (job, log_result) in results.items():
        field, spw, input_ms, output_ms = job_map.get(job_id, ('unknown', 'unknown', '', ''))
        
        if log_result.success and os.path.exists(output_ms):
            new_ms_map[field].append(output_ms)
            logger.info(f"{spw}/{field}: OK")
        else:
            # Keep original MS if catalog cal fails
            if os.path.exists(input_ms):
                new_ms_map[field].append(input_ms)
            logger.warning(f"{spw}/{field}: Catalog cal failed, using original MS")
    
    # Check completely failed fields
    for field in list(new_ms_map.keys()):
        if len(new_ms_map[field]) == 0:
            failed_fields.append(field)
            del new_ms_map[field]
    
    return new_ms_map if new_ms_map else None, failed_fields


def run_residual_flagging(hk: Housekeeper,
                          config,
                          ms_map: Dict[str, List[str]],
                          round_name: str,
                          logger,
                          whitelist: List[str]) -> Dict[str, List[str]]:
    """
    Run residual flagging after selfcal.
    - Catboss on RESIDUAL_DATA (sigma=5.0, combinations=1,2,4,8)
    - Nami on CORRECTED_DATA (sigma=5.0, nknots=3)
    """
    logger.substep(f"Flagging residuals ({round_name})...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('flagging', config.resources.get('default', {}))
    ppn = resources.get('ppn', 8)
    
    job_ids = []
    job_map = {}
    
    for field, ms_list in ms_map.items():
        for ms_path in ms_list:
            if not os.path.exists(ms_path):
                continue
            
            parts = ms_path.split('/')
            spw = parts[0]
            field_dir = f"{spw}/{field}"
            
            script = f'''#!/usr/bin/env python3
# Residual flagging {round_name} for {spw}/{field}
import subprocess
import os

ms = '{ms_path}'

# Catboss on RESIDUAL_DATA
# sigma=5.0, combinations=1,2,4,8 (never more than 4 combos, never below sigma 5)
catboss_cmd = f"catboss --cat pooh {{ms}} --combinations 1,2,4,8 --sigma 5.0 --rho 1.5 --poly-degree 5 --deviation-threshold 3.0 --datacolumn RESIDUAL_DATA --apply-flags --max-threads {ppn} --max-memory-usage 0.8 --verbose"
print(f"Running: {{catboss_cmd}}")
result = subprocess.run(catboss_cmd, shell=True)
if result.returncode != 0:
    print("Catboss failed but continuing...")

# Nami on CORRECTED_DATA with nknots=3
# timebin is in MINUTES
nami_cmd = f"nami {{ms}} --datacolumn CORRECTED_DATA --sigma 5.0 --nknots 3 --timebin 10 --ncpu {ppn}"
print(f"Running: {{nami_cmd}}")
result = subprocess.run(nami_cmd, shell=True)
if result.returncode != 0:
    print("Nami failed but continuing...")

# Remove lock
lock_file = os.path.join(ms, 'table.lock')
if os.path.exists(lock_file):
    os.remove(lock_file)

print("Residual flagging complete")
'''
            
            script_file = f"resflag_{round_name}_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""
            
            job = hk.submit(
                command=command,
                name=f"resflag_{round_name}_{spw}_{field}",
                job_subdir=field_dir,
                ppn=ppn,
                walltime=resources.get('walltime', '02:00:00')
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (field, spw)
                logger.info(f"Submitted resflag {spw}/{field}: {job.job_id}")
            
            time.sleep(0.3)
    
    if not job_ids:
        return ms_map
    
    # Wait
    logger.substep(f"Waiting for {len(job_ids)} resflag jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    for job_id, (job, log_result) in results.items():
        field, spw = job_map.get(job_id, ('unknown', 'unknown'))
        if log_result.success:
            logger.info(f"{spw}/{field}: OK")
        else:
            logger.warning(f"{spw}/{field}: resflag issues (continuing)")
    
    return ms_map


def run_selfcal_loop(hk: Housekeeper,
                     config,
                     ms_map: Dict[str, List[str]],
                     refant: str,
                     logger,
                     whitelist: List[str]) -> Optional[Dict[str, List[str]]]:
    """
    Run full self-calibration loop.
    
    Flow:
    1. Phase cal rounds (gaincal+bandpass, calmode='p', solnorm=True)
       - Image -> Calibrate -> Flag residuals
    2. Catalog calibration (if use_catalogs=True)
       - foresight + crystalball + quartical
    3. Amp+phase cal rounds (gaincal+bandpass, calmode='ap', solnorm=False)
       - Image -> Calibrate -> Flag residuals
    4. Final image (checks MS correlations: 4 corrs -> I,Q,U,V, 2 corrs -> I only)
    
    Brotherhood: per-field. If any SPW fails for a field and brotherhood=True, STOP.
    """
    """
       - Image -> Calibrate -> Flag residuals
    4. Final image
    
    Brotherhood: per-field. If all SPWs fail for a field, drop it.
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
    use_catalogs = selfcal_config.get('imaging', {}).get('use_catalogs', False)
    flag_residuals = selfcal_config.get('flag_residuals', True)
    brotherhood = setup_config.get('brotherhood', True)
    
    start_iters = clean_config.get('start_iters', 1000)
    threshold = clean_config.get('threshold', 0.001)
    
    # Get sequences
    solint_sequence = get_solint_sequence(phase_rounds, ap_rounds, initial_solint)
    total_rounds = phase_rounds + ap_rounds + (1 if use_catalogs else 0)
    niter_sequence = get_niter_sequence(start_iters, total_rounds + 2)  # Extra for final
    
    logger.info(f"Selfcal: {phase_rounds} phase + {ap_rounds} ap rounds")
    logger.info(f"Solint sequence: {solint_sequence}")
    logger.info(f"Niter sequence: {niter_sequence[:total_rounds+1]}")
    logger.info(f"Use catalogs: {use_catalogs}")
    logger.info(f"Flag residuals: {flag_residuals}")
    logger.info(f"Brotherhood: {brotherhood}")
    
    all_failed_fields = []
    round_idx = 0
    
    # =========================================================================
    # PHASE CALIBRATION ROUNDS
    # =========================================================================
    for i in range(phase_rounds):
        round_num = i + 1
        solint = solint_sequence[i]
        niter = niter_sequence[round_idx]
        
        logger.substep(f"=== Phase cal round {round_num}/{phase_rounds} (solint={solint}, niter={niter}) ===")
        
        # 1. Image
        run_wsclean(
            hk=hk,
            config=config,
            ms_map=ms_map,
            niter=niter,
            prefix=f"pcal{round_num}",
            logger=logger,
            whitelist=whitelist,
            datacolumn='DATA',
            use_masks=(round_num > 2)
        )
        
        # 2. Gaincal + Bandpass (calmode='p', solnorm=True)
        input_suffix = 'sc.ms' if i == 0 else f'pcal{i}.ms'
        output_suffix = f'pcal{round_num}.ms'
        
        ms_map, failed = run_gaincal_bandpass(
            hk=hk,
            config=config,
            ms_map=ms_map,
            refant=refant,
            solint=solint,
            calmode='p',
            solnorm=True,
            round_name=f'pcal{round_num}',
            output_ms_suffix=output_suffix,
            logger=logger,
            whitelist=whitelist,
            brotherhood=brotherhood
        )
        
        if ms_map is None:
            logger.error("All fields failed at phase cal!")
            return None
        
        all_failed_fields.extend(failed)
        
        # Brotherhood: if TRUE and any SPW failed, STOP
        if brotherhood and failed:
            logger.error(f"Brotherhood=True, failures detected: {failed}, stopping!")
            return None
        
        # 3. Flag residuals
        if flag_residuals:
            ms_map = run_residual_flagging(
                hk=hk,
                config=config,
                ms_map=ms_map,
                round_name=f'pcal{round_num}',
                logger=logger,
                whitelist=whitelist
            )
        
        round_idx += 1
    
    # =========================================================================
    # CATALOG CALIBRATION (if enabled)
    # =========================================================================
    if use_catalogs:
        logger.substep("=== Catalog-based calibration ===")
        
        ms_map, failed = run_catalog_calibration(
            hk=hk,
            config=config,
            ms_map=ms_map,
            refant=refant,
            logger=logger,
            whitelist=whitelist,
            brotherhood=brotherhood
        )
        
        if ms_map is None:
            logger.error("All fields failed at catalog cal!")
            return None
        
        all_failed_fields.extend(failed)
        
        # Image post-catalog
        niter = niter_sequence[round_idx]
        run_wsclean(
            hk=hk,
            config=config,
            ms_map=ms_map,
            niter=niter,
            prefix='post_catalog',
            logger=logger,
            whitelist=whitelist,
            datacolumn='DATA',
            use_masks=True
        )
        
        round_idx += 1
    
    # =========================================================================
    # AMP+PHASE CALIBRATION ROUNDS
    # =========================================================================
    for i in range(ap_rounds):
        round_num = i + 1
        solint = solint_sequence[phase_rounds + i]
        niter = niter_sequence[round_idx]
        
        logger.substep(f"=== Amp+phase cal round {round_num}/{ap_rounds} (solint={solint}, niter={niter}) ===")
        
        # 1. Image
        run_wsclean(
            hk=hk,
            config=config,
            ms_map=ms_map,
            niter=niter,
            prefix=f"apcal{round_num}",
            logger=logger,
            whitelist=whitelist,
            datacolumn='DATA',
            use_masks=True
        )
        
        # 2. Gaincal + Bandpass (calmode='ap', solnorm=False)
        output_suffix = f'apcal{round_num}.ms'
        
        ms_map, failed = run_gaincal_bandpass(
            hk=hk,
            config=config,
            ms_map=ms_map,
            refant=refant,
            solint=solint,
            calmode='ap',
            solnorm=False,
            round_name=f'apcal{round_num}',
            output_ms_suffix=output_suffix,
            logger=logger,
            whitelist=whitelist,
            brotherhood=brotherhood
        )
        
        if ms_map is None:
            logger.error("All fields failed at amp+phase cal!")
            return None
        
        all_failed_fields.extend(failed)
        
        # Brotherhood: if TRUE and any SPW failed, STOP
        if brotherhood and failed:
            logger.error(f"Brotherhood=True, failures detected: {failed}, stopping!")
            return None
        
        # 3. Flag residuals
        if flag_residuals:
            ms_map = run_residual_flagging(
                hk=hk,
                config=config,
                ms_map=ms_map,
                round_name=f'apcal{round_num}',
                logger=logger,
                whitelist=whitelist
            )
        
        round_idx += 1
    
    # =========================================================================
    # FINAL IMAGE - This IS the final selfcal product
    # Check actual correlations in the MS:
    # 4 correlations (RR,RL,LR,LL or XX,XY,YX,YY) -> I, Q, U, V
    # 2 correlations (RR,LL or XX,YY) -> I only
    # =========================================================================
    logger.substep("=== Creating final selfcal images ===")
    final_niter = niter_sequence[min(round_idx, len(niter_sequence)-1)] * 2
    
    # Check number of correlations in one of the selfcal MS files
    num_corrs = 2  # Default to 2 (safe - only Stokes I)
    try:
        first_field = list(ms_map.keys())[0]
        first_ms = ms_map[first_field][0]
        
        from casatools import table
        tb = table()
        tb.open(first_ms + '/POLARIZATION')
        corr_types = tb.getcol('CORR_TYPE')
        tb.close()
        num_corrs = corr_types.shape[0]
        logger.info(f"Selfcal MS has {num_corrs} correlations")
    except Exception as e:
        logger.warning(f"Could not check correlations in MS: {e}")
        logger.warning("Defaulting to Stokes I only")
    
    if num_corrs >= 4:
        # Have all 4 correlations - can make I, Q, U, V
        stokes_list = ['I', 'Q', 'U', 'V']
        logger.info("4 correlations available - imaging I, Q, U, V")
    else:
        # Only parallel hands (2 corrs) - can only make Stokes I
        stokes_list = ['I']
        logger.info("2 correlations available - imaging only Stokes I")
    
    for stokes in stokes_list:
        save_sources = (stokes == 'I')  # Only save source list for Stokes I
        
        logger.info(f"Imaging Stokes {stokes} (save_sources={save_sources})...")
        
        run_wsclean(
            hk=hk,
            config=config,
            ms_map=ms_map,
            niter=final_niter,
            prefix=f'final_{stokes}',
            logger=logger,
            whitelist=whitelist,
            datacolumn='CORRECTED_DATA',
            use_masks=True,
            stokes=stokes,
            save_source_list=save_sources
        )
    
    # Summary
    if all_failed_fields:
        logger.warning(f"Fields that failed during selfcal: {list(set(all_failed_fields))}")
    
    logger.info(f"Selfcal complete. Remaining fields: {list(ms_map.keys())}")
    
    return ms_map
