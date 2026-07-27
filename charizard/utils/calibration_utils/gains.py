# charizard/utils/calibration_utils/gains.py
"""
Calibration - calculate gains, bandpass, polcal.
"""

import os
import time
from typing import List, Optional, Dict

from housekeeper import Housekeeper
from ..general.resources import submit_resources
from ..container import build_udocker_prefix


def get_fluxscale_lists(cal_plan: Dict, do_polcal: bool):
    """
    Build the amp / gain / transfer calibrator lists for fluxscale.

    applycal must agree with the solve about whether flux.cal was produced,
    so both gains.py and applycal.py derive do_fluxscale from this function.

    Returns:
        (amp_cal_list, all_cal_list, transfer_list)
    """
    flux_cal = cal_plan.get('flux_cal') or ''
    phase_cal = cal_plan.get('phase_cal') or ''
    gain_calibrators = cal_plan.get('gain_calibrators', [])

    amp_cal_list = [c.strip() for c in flux_cal.split(',') if c.strip()]
    phase_cal_list = [c.strip() for c in phase_cal.split(',') if c.strip()]

    if gain_calibrators:
        all_cal_list = gain_calibrators.copy()
    else:
        all_cal_list = list(set(amp_cal_list + phase_cal_list))
        if do_polcal:
            leakage_cal = cal_plan.get('leakage_cal')
            polangle_cal = cal_plan.get('polangle_cal')
            if leakage_cal and leakage_cal not in all_cal_list:
                all_cal_list.append(leakage_cal)
            if polangle_cal and polangle_cal not in all_cal_list:
                all_cal_list.append(polangle_cal)

    transfer_list = [c for c in all_cal_list if c not in amp_cal_list]
    return amp_cal_list, all_cal_list, transfer_list


def build_calibration_script(spw: str,
                             cal_plan: Dict,
                             refant: str,
                             cal_round: int,
                             do_polcal: bool = False) -> str:
    """
    Build CASA calibration script.

    Handles both circular and linear feeds:
    - Circular: gaintype=G, KCROSS, Df, Xf
    - Linear: gaintype=T, Dflls, XYf+QU with xyamb()

    Args:
        spw: SPW directory
        cal_plan: Calibration plan dict (must include pol_basis)
        refant: Reference antenna
        cal_round: Calibration round number
        do_polcal: Whether to do polarization calibration

    Returns:
        CASA script string
    """
    flux_cal = cal_plan.get('flux_cal', '')
    leakage_cal = cal_plan.get('leakage_cal')
    polangle_cal = cal_plan.get('polangle_cal')
    uvranges = cal_plan.get('calibrator_uvranges', {})
    polcal_models = cal_plan.get('polcal_models', {})
    flux_standards = cal_plan.get('flux_standards', {})
    manual_flux_models = cal_plan.get('manual_flux_models', {})
    pol_basis = cal_plan.get('pol_basis', 'circular')
    minblperant = cal_plan.get('minblperant', 4)

    linear = (pol_basis == 'linear')

    # Gaintype. Circular feeds always solve G. Linear feeds follow the MeerKAT
    # (IDIA processMeerKAT) convention: G when only parallel hands are in play,
    # T once cross-hands matter, so the X/Y ratio is preserved for the polcal.
    if linear:
        gaintype = 'T' if do_polcal else 'G'
    else:
        gaintype = 'G'

    # Parallactic angle. For linear feeds the P-Jones is a real X/Y rotation,
    # so it is only meaningful once cross-hands are present; applying it to an
    # XX,YY-only solve is undefined. Circular feeds keep the previous behaviour
    # (a diagonal phase that cancels in RR/LL).
    parang = 'True' if (not linear or do_polcal) else 'False'

    # minblperant / fillgaps are MeerKAT-recipe settings; leave the circular
    # path exactly as it was.
    minbl_param = f", minblperant={minblperant}" if linear else ""
    fillgaps_param = ", bandtype='B', fillgaps=8" if linear else ""

    # Circular keeps solnorm=True on the bandpass; the MeerKAT recipe carries
    # the scale in the B table and lets fluxscale bootstrap it.
    bp_solnorm = 'False' if linear else 'True'

    # Calibrator lists (shared with applycal so do_fluxscale always agrees)
    amp_cal_list, all_cal_list, transfer_list = get_fluxscale_lists(cal_plan, do_polcal)

    if not amp_cal_list:
        raise ValueError(
            f"No flux calibrator in cal_plan for {spw} - cannot build calibration script")

    do_fluxscale = len(transfer_list) > 0

    script = f"""# CASA Calibration Script
# Feed basis: {pol_basis}
# Gaintype:   {gaintype}
# parang:     {parang}
# polcal:     {do_polcal}

"""

    # setjy for flux calibrators.
    #
    # The default standard is Perley-Butler 2017, which contains no southern
    # sources - a MeerKAT primary would silently get no model at all and every
    # downstream solve would fit a default 1 Jy point source. So always be
    # explicit about the standard when we know which one applies.
    script += "# Set flux density model for flux calibrators\n"
    for amp_cal in amp_cal_list:
        standard = flux_standards.get(amp_cal)

        if standard == 'manual':
            manual = manual_flux_models.get(amp_cal)
            if not manual:
                raise ValueError(
                    f"{amp_cal} requires standard='manual' but no flux model was "
                    f"resolved - refusing to build a script that sets no model")
            s0, s1, s2 = manual['spix']
            script += f"""setjy(vis='{spw}/cal.ms',
    field='{amp_cal}',
    standard='manual',
    fluxdensity=[{manual['fluxdensity']:.6f}, 0, 0, 0],
    spix={[round(s0, 6), round(s1, 6), round(s2, 6), 0]},
    reffreq='{manual['reffreq_hz']:.1f}Hz',
    usescratch=True)
"""
        elif standard:
            script += (f"setjy(vis='{spw}/cal.ms', field='{amp_cal}', "
                       f"standard='{standard}', usescratch=True)\n")
        else:
            script += f"setjy(vis='{spw}/cal.ms', field='{amp_cal}')\n"

    # For polarized calibrators with full Stokes model, also run setjy
    if do_polcal and polangle_cal and polangle_cal in polcal_models:
        polcal_data = polcal_models[polangle_cal]
        stokes_I = polcal_data.get('stokes_I', 1.0)
        spectral_index = polcal_data.get('spectral_index', [0.0])
        reffreq = polcal_data.get('reffreq', '1.0GHz')
        pol_frac = polcal_data.get('polarization_fraction', [0.0])
        pol_angle = polcal_data.get('polarization_angle', [0.0])

        script += f"""
# Set polarization calibrator model (full Stokes)
setjy(vis='{spw}/cal.ms',
    field='{polangle_cal}',
    standard='manual',
    fluxdensity=[{stokes_I}, 0, 0, 0],
    spix={spectral_index},
    reffreq="{reffreq}",
    polindex={pol_frac},
    polangle={pol_angle})
"""

    script += "\n# Delay calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""

        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/delays.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    gaintype='K',
    solint='inf',
    combine='scan',
    parang={parang},
    minsnr=3{minbl_param}{uvrange_param}{append_param})
"""

    script += "\n# Initial phase calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""

        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/phase_int.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    gaintype='{gaintype}',
    calmode='p',
    solint='int',
    parang={parang},
    minsnr=3{minbl_param}{uvrange_param}{append_param})
"""

    script += "\n# Bandpass calibration\n"
    for i, amp_cal in enumerate(amp_cal_list):
        uvrange = uvranges.get(amp_cal)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""

        script += f"""bandpass(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/bandpass.cal{cal_round}',
    field='{amp_cal}',
    refant='{refant}',
    solint='inf',
    combine='scan',
    solnorm={bp_solnorm},
    parang={parang},
    minsnr=3{minbl_param}{fillgaps_param},
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/phase_int.cal{cal_round}']{uvrange_param}{append_param})
"""

    if linear:
        # The MeerKAT recipe rflags the bandpass table before it is used
        # downstream; a few bad channels otherwise propagate into every solve.
        script += f"""
# Flag outliers in bandpass table
flagdata(vis='{spw}/caltables/bandpass.cal{cal_round}',
    datacolumn='CPARAM',
    mode='rflag',
    timedevscale=5.0,
    freqdevscale=5.0,
    action='apply')
"""

    # =========================================================================
    # LINEAR FEEDS: Leakage BEFORE gain (using Dflls on bandpass field)
    # =========================================================================
    if pol_basis == 'linear' and do_polcal and leakage_cal:
        leakage_uvrange = uvranges.get(leakage_cal)
        leakage_uvrange_param = f", uvrange='{leakage_uvrange}'" if leakage_uvrange else ""

        script += f"""
# Leakage calibration (linear feeds - Dflls on bandpass field)
polcal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/leakage.cal{cal_round}',
    field='{amp_cal_list[0]}',
    refant='',
    solint='inf',
    combine='scan',
    poltype='Dflls',
    preavg=200.0,
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}'],
    gainfield=['{amp_cal_list[0]}', '{amp_cal_list[0]}']{leakage_uvrange_param})

# Flag outliers in leakage table
flagdata(vis='{spw}/caltables/leakage.cal{cal_round}',
    datacolumn='CPARAM',
    mode='rflag',
    timedevscale=5.0,
    freqdevscale=5.0,
    action='apply')
"""

    script += "\n# Amplitude and phase calibration\n"
    for i, cal_name in enumerate(all_cal_list):
        uvrange = uvranges.get(cal_name)
        uvrange_param = f", uvrange='{uvrange}'" if uvrange else ""
        append_param = ", append=True" if i > 0 else ""

        # For linear feeds with leakage already solved, include it
        if pol_basis == 'linear' and do_polcal and leakage_cal:
            gaintable_str = f"""gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               '{spw}/caltables/leakage.cal{cal_round}']"""
            gainfield_str = f"gainfield=['{amp_cal_list[0]}', '{amp_cal_list[0]}', '{amp_cal_list[0]}']"
        else:
            gaintable_str = f"""gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}']"""
            gainfield_str = ""

        script += f"""gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/amp_phase.cal{cal_round}',
    field='{cal_name}',
    refant='{refant}',
    gaintype='{gaintype}',
    calmode='ap',
    solint='inf',
    parang={parang},
    minsnr=3{minbl_param},
    {gaintable_str}{', ' + gainfield_str if gainfield_str else ''}{uvrange_param}{append_param})
"""

    # Flag outliers in gain table
    script += f"""
# Flag outliers in gain table
flagdata(vis='{spw}/caltables/amp_phase.cal{cal_round}',
    datacolumn='CPARAM',
    mode='rflag',
    timedevscale=5.0,
    freqdevscale=5.0,
    action='apply')
"""

    # Fluxscale
    if do_fluxscale:
        transfer_str = ','.join(transfer_list)
        script += f"""
# Flux calibration
fluxscale(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/amp_phase.cal{cal_round}',
    fluxtable='{spw}/caltables/flux.cal{cal_round}',
    reference='{flux_cal}',
    transfer='{transfer_str}')
"""
        flux_table = f"'{spw}/caltables/flux.cal{cal_round}'"
    else:
        flux_table = f"'{spw}/caltables/amp_phase.cal{cal_round}'"

    # =========================================================================
    # POLARIZATION CALIBRATION
    # =========================================================================
    if do_polcal and polangle_cal and polangle_cal in polcal_models:
        polang_uvrange = uvranges.get(polangle_cal)
        polang_uvrange_param = f", uvrange='{polang_uvrange}'" if polang_uvrange else ""

        leakage_uvrange = uvranges.get(leakage_cal) if leakage_cal else None
        leakage_uvrange_param = f", uvrange='{leakage_uvrange}'" if leakage_uvrange else ""

        if pol_basis == 'linear':
            # LINEAR FEEDS: XYf+QU with xyamb()
            script += f"""
# X-Y phase calibration (linear feeds)
from casarecipes.almapolhelpers import xyamb
import numpy as np

# Calculate Q, U from polarization model
polcal_data = {polcal_models[polangle_cal]}
pol_frac = polcal_data.get('polarization_fraction', [0.0])
pol_angle = polcal_data.get('polarization_angle', [0.0])

# Get mean frequency for Q, U calculation
from casatools import msmetadata
msmd = msmetadata()
msmd.open('{spw}/cal.ms')
meanfreq = msmd.meanfreq(0, unit='MHz')
msmd.done()

# Interpolate polarization at mean frequency (simplified)
# pol_angle is in radians (setjy convention) - used as provided, no conversion
p = pol_frac[0] if isinstance(pol_frac, list) else pol_frac
pa = pol_angle[0] if isinstance(pol_angle, list) else pol_angle
q = p * np.cos(2*pa)
u = p * np.sin(2*pa)
polqu = (q, u)
print(f"Polarization Q,U = {{polqu}}")

# X-Y phase with ambiguity
gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/xyamb.cal{cal_round}',
    field='{polangle_cal}',
    refant='{refant}',
    solint='inf',
    combine='scan',
    gaintype='XYf+QU',
    minblperant={minblperant},
    preavg=200.0,
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               '{spw}/caltables/leakage.cal{cal_round}',
               {flux_table}],
    gainfield=['{amp_cal_list[0]}', '{amp_cal_list[0]}', '{amp_cal_list[0]}', '{polangle_cal}']{polang_uvrange_param})

# Resolve X-Y phase ambiguity
S = xyamb(xytab='{spw}/caltables/xyamb.cal{cal_round}',
          qu=polqu,
          xyout='{spw}/caltables/xyphase.cal{cal_round}')

# Flag outliers
flagdata(vis='{spw}/caltables/xyphase.cal{cal_round}',
    datacolumn='CPARAM',
    mode='rflag',
    timedevscale=5.0,
    freqdevscale=5.0,
    action='apply')
"""
        else:
            # CIRCULAR FEEDS: KCROSS + Df + Xf
            script += f"""
# Cross-hand delay calibration (circular feeds)
gaincal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/delaycross.cal{cal_round}',
    field='{polangle_cal}',
    refant='{refant}',
    gaintype='KCROSS',
    solint='inf',
    parang=True,
    calmode='ap',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table}]{polang_uvrange_param})

# Leakage calibration (circular feeds)
polcal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/leakage.cal{cal_round}',
    field='{leakage_cal}',
    refant='{refant}',
    poltype='Df',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table},
               '{spw}/caltables/delaycross.cal{cal_round}']{leakage_uvrange_param})

# Polarization angle calibration (circular feeds)
polcal(vis='{spw}/cal.ms',
    caltable='{spw}/caltables/polangle.cal{cal_round}',
    field='{polangle_cal}',
    refant='{refant}',
    poltype='Xf',
    gaintable=['{spw}/caltables/delays.cal{cal_round}',
               '{spw}/caltables/bandpass.cal{cal_round}',
               {flux_table},
               '{spw}/caltables/delaycross.cal{cal_round}',
               '{spw}/caltables/leakage.cal{cal_round}']{polang_uvrange_param})
"""

    script += "\nprint('Calibration complete')\n"

    return script


def run_calibration(hk: Housekeeper,
                    config,
                    active_spws: List[str],
                    cal_plan: Dict,
                    refant: str,
                    cal_round: int,
                    logger,
                    whitelist: List[str],
                    wait: bool = True) -> Optional[List[str]]:
    """
    Run calibration.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        cal_plan: Calibration plan
        refant: Reference antenna
        cal_round: Calibration round number
        logger: Logger
        whitelist: Error whitelist
        wait: Whether to wait for completion
    
    Returns:
        List of successful SPWs or job_ids (if wait=False)
    """
    logger.substep(f"Running calibration round {cal_round}...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('crosscal', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    # Check if doing polcal
    do_polcal = (cal_plan.get('leakage_cal') and
                 cal_plan.get('polangle_cal') and 
                 cal_plan.get('polangle_cal') in cal_plan.get('polcal_models', {}))
    
    if do_polcal:
        logger.info("Polarization calibration enabled")
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        script = build_calibration_script(
            spw=spw,
            cal_plan=cal_plan,
            refant=refant,
            cal_round=cal_round,
            do_polcal=do_polcal
        )
        
        script_file = f"calibrate_{cal_round}_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"cal_r{cal_round}_{spw}",
            job_subdir=spw,
            **submit_resources(resources, '12:00:00', ppn=ppn)
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted calibration for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No calibration jobs submitted")
        return None
    
    if not wait:
        return job_ids
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} calibration jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    successful = []
    failed = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if log_result.success:
            # Verify caltables exist
            required = ['delays.cal', 'bandpass.cal', 'amp_phase.cal']
            missing = [t for t in required if not os.path.exists(f"{spw}/caltables/{t}{cal_round}")]
            
            if missing:
                failed.append(spw)
                logger.error(f"{spw}: Missing caltables: {missing}")
            else:
                successful.append(spw)
                logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.error(f"{spw}: FAILED")
    
    return successful if successful else None
