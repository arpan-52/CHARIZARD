# charizard/selfcal/selfcal.py
"""
Self-calibration - imaging and phase/amp-phase loops
"""

import os
import time
from typing import Dict, List, Optional
from housekeeper import Housekeeper

from ..utils.ms_utils import remove_lock


def build_wsclean_command(ms_path: str, prefix: str, niter: int = 5000,
                          imsize: int = 4096, cellsize: str = "1asec",
                          robust: float = 0.0, datacolumn: str = "DATA",
                          mask: Optional[str] = None,
                          save_source_list: bool = False) -> str:
    """Build WSClean imaging command"""
    cmd = ["wsclean"]
    cmd += ["-name", prefix]
    cmd += ["-size", str(imsize), str(imsize)]
    cmd += ["-scale", cellsize]
    cmd += ["-niter", str(niter)]
    cmd += ["-weight", "briggs", str(robust)]
    cmd += ["-data-column", datacolumn]
    cmd += ["-pol", "I"]
    cmd += ["-channels-out", "4"]
    cmd += ["-join-channels"]
    cmd += ["-fit-spectral-pol", "3"]
    cmd += ["-auto-mask", "5"]
    cmd += ["-auto-threshold", "0.05"]
    cmd += ["-gain", "0.1"]
    cmd += ["-mgain", "0.9"]
    cmd += ["-parallel-deconvolution", "8192"]
    
    if mask:
        cmd += ["-fits-mask", mask]
    
    if save_source_list:
        cmd += ["-save-source-list"]
    
    cmd.append(ms_path)
    
    return " ".join(cmd)


def build_gaincal_script(spw: str, field: str, input_ms: str, output_ms: str,
                          prefix: str, solint: str, calmode: str, refant: str) -> str:
    """Build CASA selfcal gaincal script"""
    solnorm = "False" if calmode == "p" else "True"
    
    return f"""
# Self-cal: {prefix}
gaincal(vis='{spw}/{field}/{input_ms}',
        caltable='{spw}/{field}/selfcal-tables/{prefix}.g',
        field='', spw='', solint='{solint}', refant='{refant}',
        minsnr=2.0, gaintype='G', calmode='{calmode}', solnorm={solnorm})

applycal(vis='{spw}/{field}/{input_ms}',
         gaintable=['{spw}/{field}/selfcal-tables/{prefix}.g'],
         applymode='calflag', flagbackup=True)

mstransform(vis='{spw}/{field}/{input_ms}',
            outputvis='{spw}/{field}/{output_ms}',
            datacolumn='corrected')
"""


def submit_imaging_jobs(hk: Housekeeper, config: Dict, input_ms: str,
                        active_spws: List[str], field_list: List[str],
                        prefix: str, niter: int, preamble: str = '',
                        datacolumn: str = 'DATA') -> List[str]:
    """Submit WSClean imaging jobs"""
    selfcal_config = config['flow']['imaging_selfcal']['selfcal']
    imsize = selfcal_config['imaging']['imsize']
    cellsize = selfcal_config['imaging']['cellsize']
    
    job_ids = []
    
    for spw in active_spws:
        for field in field_list:
            ms_path = f"{spw}/{field}/{input_ms}"
            img_prefix = f"{spw}/{field}/{prefix}_{field}"
            
            wsclean_cmd = build_wsclean_command(
                ms_path, img_prefix, niter=niter,
                imsize=imsize, cellsize=cellsize, datacolumn=datacolumn
            )
            
            command = f"cd {os.getcwd()}\n{preamble}\n{wsclean_cmd}"
            
            job = hk.submit(command=command, name=f"img_{prefix}_{spw}_{field}",
                            job_subdir=f"{spw}/{field}", ppn=8, walltime="04:00:00")
            if job.job_id:
                job_ids.append(job.job_id)
            time.sleep(0.5)
    
    return job_ids


def submit_selfcal_solve_jobs(hk: Housekeeper, config: Dict, input_ms: str,
                               output_ms: str, active_spws: List[str],
                               field_list: List[str], solint: str,
                               calmode: str, prefix: str) -> List[str]:
    """Submit selfcal gaincal jobs"""
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    refant = config['flow']['imaging_selfcal']['selfcal']['loops']['refant']
    
    job_ids = []
    
    for spw in active_spws:
        for field in field_list:
            script = build_gaincal_script(spw, field, input_ms, output_ms,
                                          prefix, solint, calmode, refant)
            
            script_file = f"{prefix}_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
            
            job = hk.submit(command=command, name=f"{prefix}_{spw}_{field}",
                            job_subdir=f"{spw}/{field}", ppn=4, walltime="02:00:00")
            if job.job_id:
                job_ids.append(job.job_id)
            time.sleep(0.5)
    
    return job_ids


def prepare_selfcal_ms(hk: Housekeeper, config: Dict, cal_plan: Dict,
                        active_spws: List[str], logger) -> tuple:
    """Prepare MS for selfcal: split by field"""
    casa_path = config['environment']['casa_path']
    preamble = config['environment'].get('preamble', '')
    field_list = cal_plan['targets']
    
    job_ids = []
    
    for spw in active_spws:
        for field in field_list:
            os.makedirs(f"{spw}/{field}", exist_ok=True)
            os.makedirs(f"{spw}/{field}/selfcal-tables", exist_ok=True)
            
            script = f"""
mstransform(vis='{spw}/src.ms', outputvis='{spw}/{field}/pcal1.ms',
            field='{field}', datacolumn='corrected')
"""
            script_file = f"split_{spw}_{field}.py"
            with open(script_file, 'w') as f:
                f.write(script)
            
            command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
            
            job = hk.submit(command=command, name=f"split_{spw}_{field}",
                            job_subdir=f"{spw}/{field}", ppn=4, walltime="01:00:00")
            if job.job_id:
                job_ids.append(job.job_id)
            time.sleep(0.5)
    
    return job_ids, field_list


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


def run_selfcal(hk: Housekeeper, config: Dict, cal_plan: Dict,
                active_spws: List[str], logger) -> Dict:
    """
    Run self-calibration:
    1. Split by field
    2. Phase selfcal loops
    3. Amp+Phase selfcal loops
    4. Final imaging
    """
    logger.step("SELF-CALIBRATION")
    
    selfcal_config = config['flow']['imaging_selfcal']['selfcal']
    phase_loops = selfcal_config['loops'].get('phase', 4)
    ap_loops = selfcal_config['loops'].get('amp_phase', 2)
    solint = selfcal_config['loops'].get('solint', '4min')
    start_niter = selfcal_config['clean'].get('start_iters', 1000)
    brotherhood = config['flow']['initial_calibration_flagging']['setup'].get('brotherhood', True)
    preamble = config['environment'].get('preamble', '')
    
    # ----- PREPARE MS -----
    logger.substep("Preparing MS (split by field)...")
    prep_jobs, field_list = prepare_selfcal_ms(hk, config, cal_plan, active_spws, logger)
    
    if not wait_for_jobs(hk, prep_jobs, logger, "MS preparation", brotherhood):
        return {'success': False, 'error': 'MS preparation failed'}
    
    current_ms = 'pcal1.ms'
    
    # ----- PHASE SELFCAL LOOPS -----
    for loop in range(1, phase_loops + 1):
        logger.substep(f"Phase selfcal loop {loop}/{phase_loops}")
        
        niter = start_niter * loop
        
        # Image
        job_ids = submit_imaging_jobs(hk, config, current_ms, active_spws, field_list,
                                      prefix=f"pcal{loop}", niter=niter, preamble=preamble)
        if not wait_for_jobs(hk, job_ids, logger, f"imaging pcal{loop}", brotherhood):
            return {'success': False, 'error': f'Imaging pcal{loop} failed'}
        
        # Solve
        next_ms = f'pcal{loop+1}.ms'
        job_ids = submit_selfcal_solve_jobs(hk, config, current_ms, next_ms,
                                             active_spws, field_list, solint,
                                             calmode='p', prefix=f'pcal{loop}')
        if not wait_for_jobs(hk, job_ids, logger, f"solve pcal{loop}", brotherhood):
            return {'success': False, 'error': f'Solve pcal{loop} failed'}
        
        current_ms = next_ms
    
    # ----- AMP+PHASE SELFCAL LOOPS -----
    for loop in range(1, ap_loops + 1):
        logger.substep(f"Amp+Phase selfcal loop {loop}/{ap_loops}")
        
        niter = start_niter * (phase_loops + loop)
        
        # Image
        job_ids = submit_imaging_jobs(hk, config, current_ms, active_spws, field_list,
                                      prefix=f"apcal{loop}", niter=niter, preamble=preamble)
        if not wait_for_jobs(hk, job_ids, logger, f"imaging apcal{loop}", brotherhood):
            return {'success': False, 'error': f'Imaging apcal{loop} failed'}
        
        # Solve
        next_ms = f'apcal{loop+1}.ms' if loop < ap_loops else 'final.ms'
        job_ids = submit_selfcal_solve_jobs(hk, config, current_ms, next_ms,
                                             active_spws, field_list, solint,
                                             calmode='ap', prefix=f'apcal{loop}')
        if not wait_for_jobs(hk, job_ids, logger, f"solve apcal{loop}", brotherhood):
            return {'success': False, 'error': f'Solve apcal{loop} failed'}
        
        current_ms = next_ms
    
    # ----- FINAL IMAGE -----
    logger.substep("Final imaging...")
    niter = start_niter * (phase_loops + ap_loops + 1)
    job_ids = submit_imaging_jobs(hk, config, current_ms, active_spws, field_list,
                                  prefix="final", niter=niter, preamble=preamble)
    if not wait_for_jobs(hk, job_ids, logger, "final imaging", brotherhood):
        return {'success': False, 'error': 'Final imaging failed'}
    
    logger.success("Self-calibration completed")
    return {'success': True, 'final_ms': current_ms}
