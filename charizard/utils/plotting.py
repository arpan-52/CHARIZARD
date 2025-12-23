# charizard/utils/plotting.py
"""
Plotting utilities using shadems
"""

import os
from typing import Optional, List, Dict


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
    calibrators = cal_plan['all_calibrators']
    
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
{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:amp", field=cal, corr="RL,LR",
             colour_by="SCAN_NUMBER", out_dir=out_dir, suffix=f"_{cal}_freq_crossamp",
             title=f"{cal} Freq vs Cross-Amp")}

{shadems_cmd(ms_path, "FREQ", "CORRECTED_DATA:phase", field=cal, corr="RL,LR",
             colour_by="SCAN_NUMBER", out_dir=out_dir, suffix=f"_{cal}_freq_crossphase",
             title=f"{cal} Freq vs Cross-Phase")}

{shadems_cmd(ms_path, "TIME", "CORRECTED_DATA:amp", field=cal, corr="RL,LR",
             colour_by="ANTENNA1", out_dir=out_dir, suffix=f"_{cal}_time_crossamp",
             title=f"{cal} Time vs Cross-Amp")}

"""
    
    script += 'echo "Calibrator plots complete!"\n'
    return script


def build_target_plots_script(spw: str, cal_plan: Dict) -> str:
    """Build diagnostic plots script for targets"""
    ms_path = f"{spw}/src.ms"
    out_dir = f"{spw}/plots"
    targets = cal_plan['targets']
    
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
