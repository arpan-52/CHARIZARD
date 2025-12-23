# charizard/calibration/calibration.py
"""
Calibration - gains, polcal, applycal, diagnostic plots
Uses UV ranges from cal_plan for each calibrator.
"""

import os
import time
from typing import Dict, List, Optional
from housekeeper import Housekeeper

from ..utils.ms_utils import remove_lock
from ..utils.plotting import build_calibrator_plots_script, build_target_plots_script
from ..rfi_remover.flaggers import submit_nami_jobs


def get_uvrange(cal_plan: Dict, field: str) -> str:
    """Get UV range for a calibrator field, or empty string if none"""
    uvranges = cal_plan.get('calibrator_uvranges', {})
    return uvranges.get(field, '')


def build_gain_script(spw: str, cal_plan: Dict, refant: str, cal_round: int) -> str:
    """
    Build CASA gain calibration script.
    Applies UV ranges to each calibrator.
    """
    flux_cal = cal_plan['flux_cal']
    phase_cal = cal_plan['phase_cal']
    do_fluxscale = cal_plan['do_fluxscale']
    
    # Get UV ranges
    flux_uvrange = get_uvrange(cal_plan, flux_cal)
    
    all_cals = [flux_cal]
    if phase_cal and phase_cal != flux_cal:
        all_cals.append(phase_cal)
    
    transfer = ','.join([c for c in all_cals if c != flux_cal])
    
    # UV range parameter string
    flux_uv_param = f", uvrange='{flux_uvrange}'" if flux_uvrange else ""
    
    script = f"""
# Gain Calibration Round {cal_round} - {spw}
# Reference antenna: {refant}

# Set flux model
setjy(vis='{spw}/cal.ms', field='{flux_cal}')

# Delay calibration
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/delays.cal{cal_round}',
        field='{flux_cal}', refant='{refant}',
        gaintype='K', solint='inf', combine='scan', minsnr=3{flux_uv_param})

# Initial phase (short solint)
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/phase_int.cal{cal_round}',
        field='{flux_cal}', refant='{refant}',
        gaintype='G', calmode='p', solint='int', minsnr=3{flux_uv_param})

# Bandpass
bandpass(vis='{spw}/cal.ms',
         caltable='{spw}/caltables/bandpass.cal{cal_round}',
         field='{flux_cal}', refant='{refant}',
         solint='inf', combine='scan', solnorm=True, minsnr=3,
         gaintable=['{spw}/caltables/delays.cal{cal_round}',
                    '{spw}/caltables/phase_int.cal{cal_round}']{flux_uv_param})

# Amp+Phase calibration for all calibrators
"""
    
    for i, cal in enumerate(all_cals):
        cal_uvrange = get_uvrange(cal_plan, cal)
        uv_param = f", uvrange='{cal_uvrange}'" if cal_uvrange else ""
        append = ", append=True" if i > 0 else ""
        
        script += f"""
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/amp_phase.cal{cal_round}',
        field='{cal}', refant='{refant}',
        gaintype='G', calmode='ap', solint='120s', minsnr=3,
        gaintable=['{spw}/caltables/delays.cal{cal_round}',
                   '{spw}/caltables/bandpass.cal{cal_round}']{uv_param}{append})
"""
    
    if do_fluxscale and transfer:
        script += f"""
# Flux scale transfer
fluxscale(vis='{spw}/cal.ms',
          caltable='{spw}/caltables/amp_phase.cal{cal_round}',
          fluxtable='{spw}/caltables/flux.cal{cal_round}',
          reference='{flux_cal}', transfer='{transfer}')
"""
    
    return script


def build_polcal_script(spw: str, cal_plan: Dict, refant: str, cal_round: int) -> Optional[str]:
    """
    Build polcal script if needed.
    Uses polcal models from cal_plan.
    """
    if not cal_plan.get('do_leakage') and not cal_plan.get('do_polangle'):
        return None
    
    do_fluxscale = cal_plan['do_fluxscale']
    flux_table = f"'{spw}/caltables/flux.cal{cal_round}'" if do_fluxscale else f"'{spw}/caltables/amp_phase.cal{cal_round}'"
    
    script = f"\n# Polarization Calibration - Round {cal_round}\n"
    
    # Set polcal model and do cross-hand delay
    if cal_plan.get('do_polangle') and cal_plan.get('polangle_model'):
        polangle_cal = cal_plan['polangle_cal']
        model = cal_plan['polangle_model']
        polangle_uvrange = get_uvrange(cal_plan, polangle_cal)
        uv_param = f", uvrange='{polangle_uvrange}'" if polangle_uvrange else ""
        
        # Format spectral index
        spix = model.get('spectral_index', [-0.5])
        if isinstance(spix, list):
            spix_str = str(spix)
        else:
            spix_str = f"[{spix}]"
        
        # Format polarization parameters
        pol_frac = model.get('polarization_fraction', [0.1])
        pol_angle = model.get('polarization_angle', [33.0])
        
        script += f"""
# Set polarization model for {polangle_cal}
setjy(vis='{spw}/cal.ms', field='{polangle_cal}', standard='manual',
      fluxdensity=[{model.get('stokes_I', 10.0)}, 0, 0, 0],
      spix={spix_str}, reffreq="{model.get('reffreq', '1.4GHz')}",
      polindex={pol_frac},
      polangle={pol_angle})

# Cross-hand delay
gaincal(vis='{spw}/cal.ms',
        caltable='{spw}/caltables/kcross.cal{cal_round}',
        field='{polangle_cal}', refant='{refant}',
        gaintype='KCROSS', solint='inf', combine='scan',
        gaintable=['{spw}/caltables/delays.cal{cal_round}',
                   '{spw}/caltables/bandpass.cal{cal_round}', {flux_table}]{uv_param})
"""
    
    # Leakage calibration
    if cal_plan.get('do_leakage'):
        leakage_cal = cal_plan['leakage_cal']
        mode = cal_plan.get('leakage_mode', 'Df')
        leakage_uvrange = get_uvrange(cal_plan, leakage_cal)
        uv_param = f", uvrange='{leakage_uvrange}'" if leakage_uvrange else ""
        
        gaintables = [
            f"'{spw}/caltables/delays.cal{cal_round}'",
            f"'{spw}/caltables/bandpass.cal{cal_round}'",
            flux_table
        ]
        
        # Add kcross if we did polangle
        if cal_plan.get('do_polangle'):
            gaintables.append(f"'{spw}/caltables/kcross.cal{cal_round}'")
        
        # If leakage cal has a model, set it
        if cal_plan.get('leakage_model'):
            model = cal_plan['leakage_model']
            spix = model.get('spectral_index', [-0.5])
            if isinstance(spix, list):
                spix_str = str(spix)
            else:
                spix_str = f"[{spix}]"
            
            script += f"""
# Set model for leakage calibrator (if polarized)
setjy(vis='{spw}/cal.ms', field='{leakage_cal}', standard='manual',
      fluxdensity=[{model.get('stokes_I', 10.0)}, 0, 0, 0],
      spix={spix_str}, reffreq="{model.get('reffreq', '1.4GHz')}",
      polindex={model.get('polarization_fraction', [0.0])},
      polangle={model.get('polarization_angle', [0.0])})
"""
        
        script += f"""
# Leakage (D-terms) - mode={mode}
polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/leakage.cal{cal_round}',
       field='{leakage_cal}', refant='{refant}', poltype='{mode}',
       gaintable=[{', '.join(gaintables)}]{uv_param})
"""
    
    # Polarization angle
    if cal_plan.get('do_polangle'):
        polangle_cal = cal_plan['polangle_cal']
        polangle_uvrange = get_uvrange(cal_plan, polangle_cal)
        uv_param = f", uvrange='{polangle_uvrange}'" if polangle_uvrange else ""
        
        gaintables = [
            f"'{spw}/caltables/delays.cal{cal_round}'",
            f"'{spw}/caltables/bandpass.cal{cal_round}'",
            flux_table,
            f"'{spw}/caltables/kcross.cal{cal_round}'"
        ]
        if cal_plan.get('do_leakage'):
            gaintables.append(f"'{spw}/caltables/leakage.cal{cal_round}'")
        
        script += f"""
# Polarization angle (X-Y phase)
polcal(vis='{spw}/cal.ms',
       caltable='{spw}/caltables/polangle.cal{cal_round}',
       field='{polangle_cal}', refant='{refant}', poltype='Xf',
       gaintable=[{', '.join(gaintables)}]{uv_param})
"""
    
    return script


def build_applycal_script(spw: str, cal_plan: Dict, cal_round: int, 
                          target_type: str = 'calibrators') -> str:
    """Build applycal script"""
    do_fluxscale = cal_plan['do_fluxscale']
    
    gaintables = [
        f"'{spw}/caltables/delays.cal{cal_round}'",
        f"'{spw}/caltables/bandpass.cal{cal_round}'"
    ]
    
    if do_fluxscale:
        gaintables.append(f"'{spw}/caltables/flux.cal{cal_round}'")
    else:
        gaintables.append(f"'{spw}/caltables/amp_phase.cal{cal_round}'")
    
    if cal_plan.get('do_polangle'):
        gaintables.append(f"'{spw}/caltables/kcross.cal{cal_round}'")
    if cal_plan.get('do_leakage'):
        gaintables.append(f"'{spw}/caltables/leakage.cal{cal_round}'")
    if cal_plan.get('do_polangle'):
        gaintables.append(f"'{spw}/caltables/polangle.cal{cal_round}'")
    
    ms_name = 'cal.ms' if target_type == 'calibrators' else 'src.ms'
    
    return f"""
# Apply calibration to {target_type}
applycal(vis='{spw}/{ms_name}', field='',
         gaintable=[{', '.join(gaintables)}],
         applymode='calflag', flagbackup=True)
"""


def submit_calibration_jobs(hk: Housekeeper, config: Dict, cal_plan: Dict,
                             cal_round: int, active_spws: List[str],
                             refant: str) -> List[str]:
    """Submit calibration jobs"""
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    
    job_ids = []
    
    for spw in active_spws:
        script = build_gain_script(spw, cal_plan, refant, cal_round)
        polcal_script = build_polcal_script(spw, cal_plan, refant, cal_round)
        if polcal_script:
            script += polcal_script
        
        script_file = f"calibrate_{cal_round}_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(command=command, name=f"calibrate_{spw}_r{cal_round}",
                        job_subdir=spw, ppn=4, walltime="04:00:00")
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def submit_applycal_jobs(hk: Housekeeper, config: Dict, cal_plan: Dict,
                          cal_round: int, active_spws: List[str],
                          target_type: str) -> List[str]:
    """Submit applycal jobs"""
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    
    job_ids = []
    
    for spw in active_spws:
        script = build_applycal_script(spw, cal_plan, cal_round, target_type)
        script_file = f"applycal_{target_type}_{cal_round}_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(command=command, name=f"applycal_{target_type[:3]}_{spw}_r{cal_round}",
                        job_subdir=spw, ppn=4, walltime="02:00:00")
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def submit_diagnostic_plots(hk: Housekeeper, config: Dict, cal_plan: Dict,
                             active_spws: List[str], preamble: str = '') -> List[str]:
    """Submit diagnostic plot jobs"""
    do_polcal = cal_plan.get('do_leakage') or cal_plan.get('do_polangle')
    job_ids = []
    
    for spw in active_spws:
        # Calibrator plots
        script = build_calibrator_plots_script(spw, cal_plan, do_polcal)
        script_file = f"plots_cal_{spw}.sh"
        with open(script_file, 'w') as f:
            f.write(script)
        os.chmod(script_file, 0o755)
        
        command = f"cd {os.getcwd()}\n{preamble}\nbash {script_file}"
        job = hk.submit(command=command, name=f"plots_cal_{spw}",
                        job_subdir=spw, ppn=2, walltime="02:00:00")
        if job.job_id:
            job_ids.append(job.job_id)
        
        # Target plots
        if cal_plan['targets']:
            script = build_target_plots_script(spw, cal_plan)
            script_file = f"plots_tgt_{spw}.sh"
            with open(script_file, 'w') as f:
                f.write(script)
            os.chmod(script_file, 0o755)
            
            command = f"cd {os.getcwd()}\n{preamble}\nbash {script_file}"
            job = hk.submit(command=command, name=f"plots_tgt_{spw}",
                            job_subdir=spw, ppn=2, walltime="02:00:00")
            if job.job_id:
                job_ids.append(job.job_id)
        
        time.sleep(0.5)
    
    return job_ids


def wait_for_jobs(hk: Housekeeper, job_ids: List[str], logger, 
                  step_name: str, brotherhood: bool = True) -> bool:
    """Wait for jobs"""
    if not job_ids:
        return True
    
    logger.substep(f"Waiting for {len(job_ids)} {step_name} jobs...")
    results = hk.wait(job_ids)
    
    failed = [jid for jid, job in results.items() if job.state.value == 'failed']
    if failed:
        logger.error(f"{len(failed)}/{len(job_ids)} {step_name} jobs failed")
        if brotherhood:
            return False
    
    logger.success(f"{step_name} completed")
    return True


def run_calibration(hk: Housekeeper, config: Dict, cal_plan: Dict,
                    active_spws: List[str], logger) -> Dict:
    """
    Run full calibration:
    Round 1: solve, apply to cals, flag
    Round 2: solve, apply to cals, apply to targets
    Plots: diagnostic plots
    """
    logger.step("CALIBRATION")
    
    cal_config = config['flow']['initial_calibration_flagging']['calibration']
    num_rounds = cal_config.get('control', {}).get('rounds', 2)
    apply_to_cals = cal_config.get('apply', {}).get('calibrators', True)
    apply_to_targets = cal_config.get('apply', {}).get('targets', True)
    show_plots = cal_config.get('show_me_plots', False)
    brotherhood = config['flow']['initial_calibration_flagging']['setup'].get('brotherhood', True)
    
    refant = cal_plan.get('refant')
    if not refant:
        logger.error("No reference antenna set!")
        return {'success': False, 'error': 'No refant'}
    
    logger.info(f"Reference antenna: {refant}")
    
    # Log UV ranges being used
    uvranges = cal_plan.get('calibrator_uvranges', {})
    if uvranges:
        logger.info("UV ranges:")
        for cal, uvrange in uvranges.items():
            logger.info(f"  {cal}: {uvrange}")
    
    preamble = config['environment'].get('preamble', '')
    
    # ===== ROUND 1 =====
    cal_round = 1
    logger.substep(f"Calibration Round {cal_round}")
    
    job_ids = submit_calibration_jobs(hk, config, cal_plan, cal_round, active_spws, refant)
    if not wait_for_jobs(hk, job_ids, logger, f"calibration r{cal_round}", brotherhood):
        return {'success': False, 'error': f'Calibration round {cal_round} failed'}
    
    if apply_to_cals:
        job_ids = submit_applycal_jobs(hk, config, cal_plan, cal_round, active_spws, 'calibrators')
        if not wait_for_jobs(hk, job_ids, logger, f"applycal cals r{cal_round}", brotherhood):
            return {'success': False, 'error': f'Applycal cals round {cal_round} failed'}
    
    # Flag after round 1
    if cal_config.get('control', {}).get('flag_after', True) and cal_round < num_rounds:
        logger.substep("Flagging calibrators (NAMI)...")
        job_ids = submit_nami_jobs(hk, ['cal.ms'], active_spws, datacolumn='CORRECTED_DATA',
                                   preamble=preamble, prefix=f'r{cal_round}')
        wait_for_jobs(hk, job_ids, logger, "nami flagging", brotherhood=False)
    
    # ===== ROUND 2 =====
    if num_rounds >= 2:
        cal_round = 2
        logger.substep(f"Calibration Round {cal_round}")
        
        job_ids = submit_calibration_jobs(hk, config, cal_plan, cal_round, active_spws, refant)
        if not wait_for_jobs(hk, job_ids, logger, f"calibration r{cal_round}", brotherhood):
            return {'success': False, 'error': f'Calibration round {cal_round} failed'}
        
        if apply_to_cals:
            job_ids = submit_applycal_jobs(hk, config, cal_plan, cal_round, active_spws, 'calibrators')
            if not wait_for_jobs(hk, job_ids, logger, f"applycal cals r{cal_round}", brotherhood):
                return {'success': False, 'error': f'Applycal cals round {cal_round} failed'}
        
        if apply_to_targets and cal_plan['targets']:
            job_ids = submit_applycal_jobs(hk, config, cal_plan, cal_round, active_spws, 'targets')
            if not wait_for_jobs(hk, job_ids, logger, f"applycal targets r{cal_round}", brotherhood):
                return {'success': False, 'error': f'Applycal targets round {cal_round} failed'}
    
    # ===== DIAGNOSTIC PLOTS =====
    if show_plots:
        logger.substep("Generating diagnostic plots (shadems)...")
        job_ids = submit_diagnostic_plots(hk, config, cal_plan, active_spws, preamble)
        wait_for_jobs(hk, job_ids, logger, "diagnostic plots", brotherhood=False)
    
    # Remove locks
    for spw in active_spws:
        remove_lock(f"{spw}/cal.ms")
        if cal_plan['targets']:
            remove_lock(f"{spw}/src.ms")
    
    logger.success("Calibration completed successfully")
    return {'success': True}