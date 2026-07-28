# charizard/utils/plotting_utils/plotting.py
"""
Plotting utilities using shadems.
Arpan's style - make plots for diagnostics.
"""

import os
import time
from typing import Optional, List, Dict

from housekeeper import Housekeeper
from ..general.jobs import wait_and_check
from ..general.resources import submit_resources
from ..container import build_udocker_prefix


def shadems_cmd(ms: str, xaxis: str, yaxis: str,
                field: Optional[str] = None,
                corr: Optional[str] = None,
                colour_by: Optional[str] = None,
                iter_corr: bool = False,
                out_dir: Optional[str] = None,
                suffix: Optional[str] = None,
                title: Optional[str] = None,
                xcanvas: int = 1200,
                ycanvas: int = 900) -> str:
    """Build shadems command"""
    cmd = ["shadems"]
    cmd += ["--xaxis", xaxis]
    cmd += ["--yaxis", yaxis]
    
    if field:
        cmd += ["--field", field]
    if corr:
        cmd += ["--corr", corr]
    if colour_by:
        cmd += ["--colour-by", colour_by]
    if iter_corr:
        cmd.append("--iter-corr")
    if out_dir:
        cmd += ["--dir", out_dir]
    if suffix:
        cmd += ["--suffix", suffix]
    if title:
        cmd += ["--title", f'"{title}"']
    
    cmd += ["--xcanvas", str(xcanvas)]
    cmd += ["--ycanvas", str(ycanvas)]
    cmd.append(ms)
    
    return " ".join(cmd)


def build_calibrator_plots_script(spw: str, cal_plan: Dict, do_polcal: bool = False) -> str:
    """Build diagnostic plots script for calibrators"""
    ms_path = f"{spw}/cal.ms"
    out_dir = f"{spw}/plots"
    calibrators = cal_plan.get('all_calibrators', [])
    
    script = f"""#!/bin/bash
# Diagnostic plots for {ms_path}
mkdir -p {out_dir}
echo "Generating calibrator diagnostic plots..."

"""
    
    for cal in calibrators:
        script += f"""
# ===== {cal} =====
echo "Plotting {cal}..."

# UV vs Amp
{shadems_cmd(ms_path, "UV", "CORRECTED_DATA:amp", field=cal, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_uv_amp", title=f"{cal} UV vs Amp")}

# UV vs Phase
{shadems_cmd(ms_path, "UV", "CORRECTED_DATA:phase", field=cal, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_uv_phase", title=f"{cal} UV vs Phase")}

# Freq vs Amp
{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:amp", field=cal, colour_by="SCAN_NUMBER",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_freq_amp", title=f"{cal} Freq vs Amp")}

# Freq vs Phase
{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:phase", field=cal, colour_by="SCAN_NUMBER",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_freq_phase", title=f"{cal} Freq vs Phase")}

# Time vs Amp
{shadems_cmd(ms_path, "TIME", "CORRECTED_DATA:amp", field=cal, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_time_amp", title=f"{cal} Time vs Amp")}

# Time vs Phase
{shadems_cmd(ms_path, "TIME", "CORRECTED_DATA:phase", field=cal, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{cal}_time_phase", title=f"{cal} Time vs Phase")}

"""
    
    # Cross-hand plots for polcal
    if do_polcal:
        # Cross-hand correlation names depend on feed basis
        crosshand_corr = 'XY,YX' if cal_plan.get('pol_basis') == 'linear' else 'RL,LR'
        pol_cals = list(set([c for c in [cal_plan.get('polangle_cal'),
                                          cal_plan.get('leakage_cal')] if c]))
        if pol_cals:
            script += """
# ===== CROSS-HAND POLARIZATION PLOTS =====
echo "Plotting cross-hand correlations..."

"""
            for cal in pol_cals:
                script += f"""
# {cal} Cross-hand
{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:amp", field=cal, corr=crosshand_corr,
             colour_by="SCAN_NUMBER", out_dir=out_dir, suffix=f"_{cal}_freq_crossamp",
             title=f"{cal} Freq vs Cross-Amp")}

{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:phase", field=cal, corr=crosshand_corr,
             colour_by="SCAN_NUMBER", out_dir=out_dir, suffix=f"_{cal}_freq_crossphase",
             title=f"{cal} Freq vs Cross-Phase")}

{shadems_cmd(ms_path, "TIME", "CORRECTED_DATA:amp", field=cal, corr=crosshand_corr,
             colour_by="ANTENNA1", out_dir=out_dir, suffix=f"_{cal}_time_crossamp",
             title=f"{cal} Time vs Cross-Amp")}

"""
    
    script += 'echo "Calibrator plots complete!"\n'
    return script


def build_target_plots_script(spw: str, cal_plan: Dict) -> str:
    """Build diagnostic plots script for targets"""
    ms_path = f"{spw}/src.ms"
    out_dir = f"{spw}/plots"
    targets = cal_plan.get('targets', [])
    
    script = f"""#!/bin/bash
# Diagnostic plots for {ms_path}
mkdir -p {out_dir}
echo "Generating target diagnostic plots..."

"""
    
    for target in targets:
        script += f"""
# ===== {target} =====
echo "Plotting {target}..."

{shadems_cmd(ms_path, "UV", "CORRECTED_DATA:amp", field=target, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{target}_uv_amp", title=f"{target} UV vs Amp")}

{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:amp", field=target, colour_by="SCAN_NUMBER",
             iter_corr=True, out_dir=out_dir, suffix=f"_{target}_freq_amp", title=f"{target} Freq vs Amp")}

{shadems_cmd(ms_path, "TIME", "CORRECTED_DATA:amp", field=target, colour_by="ANTENNA1",
             iter_corr=True, out_dir=out_dir, suffix=f"_{target}_time_amp", title=f"{target} Time vs Amp")}

"""
    
    script += 'echo "Target plots complete!"\n'
    return script


def run_diagnostic_plots(hk: Housekeeper,
                         config,
                         active_spws: List[str],
                         cal_plan: Dict,
                         do_polcal: bool,
                         logger,
                         whitelist: List[str],
                         plot_targets: bool = True) -> bool:
    """
    Run diagnostic plots for calibrators and targets using shadems.
    
    Args:
        hk: Housekeeper instance
        config: PipelineConfig
        active_spws: List of active SPW directories
        cal_plan: Calibration plan dict
        do_polcal: Whether polcal is enabled
        logger: Logger
        whitelist: Error whitelist
        plot_targets: Whether to also plot targets
    
    Returns:
        True if at least one plotting job succeeded (partial failures are
        logged as warnings); False if every job failed
    """
    logger.substep("Generating diagnostic plots with shadems...")
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('plotting', config.resources.get('default', {}))
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # Calibrator plots
        script = build_calibrator_plots_script(spw, cal_plan, do_polcal)
        script_file = f"plot_cal_{spw}.sh"
        with open(script_file, 'w') as f:
            f.write(script)
        os.chmod(script_file, 0o755)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} bash {script_file}
"""

        job = hk.submit(
            command=command,
            name=f"plot_cal_{spw}",
            job_subdir=spw,
            **submit_resources(resources, '01:00:00', ppn=ppn)
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = (spw, 'cal')
            logger.info(f"Submitted calibrator plots for {spw}: {job.job_id}")
        
        # Target plots
        if plot_targets and cal_plan.get('targets'):
            script = build_target_plots_script(spw, cal_plan)
            script_file = f"plot_src_{spw}.sh"
            with open(script_file, 'w') as f:
                f.write(script)
            os.chmod(script_file, 0o755)
            
            udocker = build_udocker_prefix(config)
            command = f"""cd {os.getcwd()}
{preamble}
{udocker} bash {script_file}
"""

            job = hk.submit(
                command=command,
                name=f"plot_src_{spw}",
                job_subdir=spw,
                **submit_resources(resources, '01:00:00', ppn=ppn)
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
                job_map[job.job_id] = (spw, 'src')
                logger.info(f"Submitted target plots for {spw}: {job.job_id}")
        
        time.sleep(0.3)
    
    if not job_ids:
        logger.warning("No plotting jobs submitted")
        return True
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} plotting jobs...")
    results = wait_and_check(hk, job_ids, whitelist=whitelist, logger=logger)
    
    success_count = 0
    for job_id, (job, log_result) in results.items():
        spw, plot_type = job_map.get(job_id, ('unknown', 'unknown'))
        
        if log_result.success:
            success_count += 1
            logger.info(f"{spw} ({plot_type}): OK")
        else:
            logger.warning(f"{spw} ({plot_type}): plotting had issues")

    if success_count == 0:
        logger.error("ALL plotting jobs failed")
        return False

    logger.info(f"Plots saved to {{spw}}/plots/ directories")
    return True
