# charizard/rfi_remover/flaggers.py
"""
Flagging job submission - initial, tfcrop, rflag, nami
"""

import os
import time
from typing import List, Optional, Dict
from housekeeper import Housekeeper


def submit_initial_flag_jobs(hk: Housekeeper, config: Dict, ms_names: List[str],
                              flag_file: str, active_spws: List[str],
                              casa_path: str, preamble: str = '') -> List[str]:
    """Submit initial flagging jobs using flag list files"""
    job_ids = []
    
    for spw in active_spws:
        casa_script = ""
        for ms in ms_names:
            flag_path = f"{spw}/{flag_file}"
            casa_script += f"""
flagdata(vis='{spw}/{ms}', mode='list', inpfile='{flag_path}')
"""
        
        script_file = f"flag_init_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(casa_script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"flag_init_{spw}",
            job_subdir=spw,
            ppn=4,
            walltime="02:00:00"
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def submit_tfcrop_jobs(hk: Housekeeper, config: Dict, ms_names: List[str],
                        active_spws: List[str], datacolumn: str = 'DATA',
                        t_sigma: float = 4.0, f_sigma: float = 4.0,
                        casa_path: str = '', preamble: str = '',
                        prefix: str = '') -> List[str]:
    """Submit tfcrop flagging jobs"""
    job_ids = []
    
    for spw in active_spws:
        casa_script = ""
        for ms in ms_names:
            casa_script += f"""
flagdata(vis='{spw}/{ms}',
        mode='tfcrop',
        datacolumn='{datacolumn}',
        ntime='scan',
        timecutoff={t_sigma},
        freqcutoff={f_sigma},
        timefit='line',
        freqfit='poly',
        flagdimension='freqtime',
        extendflags=True,
        timedevscale={t_sigma},
        freqdevscale={f_sigma},
        extendpols=True,
        action='apply',
        flagbackup=True)
"""
        
        job_name = f"tfcrop_{prefix}_{spw}" if prefix else f"tfcrop_{spw}"
        script_file = f"{job_name}.py"
        
        with open(script_file, 'w') as f:
            f.write(casa_script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=job_name,
            job_subdir=spw,
            ppn=4,
            walltime="02:00:00"
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def submit_rflag_jobs(hk: Housekeeper, config: Dict, ms_names: List[str],
                       active_spws: List[str], datacolumn: str = 'CORRECTED',
                       t_sigma: float = 4.0, f_sigma: float = 4.0,
                       casa_path: str = '', preamble: str = '',
                       prefix: str = '') -> List[str]:
    """Submit rflag jobs with UV-range based thresholds"""
    job_ids = []
    
    for spw in active_spws:
        casa_script = ""
        for ms in ms_names:
            # UV-range based rflag
            casa_script += f"""
# Short baselines - more lenient
flagdata(vis='{spw}/{ms}', mode='rflag', datacolumn='{datacolumn}',
         timedevscale=5.0, freqdevscale=5.0, uvrange='0~1000lambda',
         extendflags=True, flagbackup=True)

# Medium baselines
flagdata(vis='{spw}/{ms}', mode='rflag', datacolumn='{datacolumn}',
         timedevscale=4.5, freqdevscale=4.5, uvrange='1000~5000lambda',
         extendflags=True, flagbackup=True)

# Long baselines - strictest
flagdata(vis='{spw}/{ms}', mode='rflag', datacolumn='{datacolumn}',
         timedevscale={t_sigma}, freqdevscale={f_sigma}, uvrange='>5000lambda',
         extendflags=True, flagbackup=True)
"""
        
        job_name = f"rflag_{prefix}_{spw}" if prefix else f"rflag_{spw}"
        script_file = f"{job_name}.py"
        
        with open(script_file, 'w') as f:
            f.write(casa_script)
        
        command = f"""
cd {os.getcwd()}
{preamble}
{casa_path}/bin/casa --nologger --nogui -c {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=job_name,
            job_subdir=spw,
            ppn=4,
            walltime="02:00:00"
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
        time.sleep(0.5)
    
    return job_ids


def submit_nami_jobs(hk: Housekeeper, ms_names: List[str], active_spws: List[str],
                      datacolumn: str = 'CORRECTED_DATA', method: str = 'poly',
                      sigma: float = 4.0, degree: int = 3, iterations: int = 2,
                      timebin: int = 10, ncpu: int = 8, preamble: str = '',
                      prefix: str = '') -> List[str]:
    """Submit NAMI flagging jobs"""
    job_ids = []
    
    for spw in active_spws:
        for ms in ms_names:
            ms_path = f"{spw}/{ms}"
            
            nami_cmd = f"nami {ms_path} --method {method} --datacolumn {datacolumn}"
            nami_cmd += f" --degree {degree} --it {iterations} --sigma {sigma}"
            nami_cmd += f" --timebin {timebin} --ncpu {ncpu}"
            
            ms_short = ms.replace('.ms', '')
            job_name = f"nami_{prefix}_{spw}_{ms_short}" if prefix else f"nami_{spw}_{ms_short}"
            
            command = f"""
cd {os.getcwd()}
{preamble}
{nami_cmd}
"""
            
            job = hk.submit(
                command=command,
                name=job_name,
                job_subdir=spw,
                ppn=ncpu,
                walltime="02:00:00"
            )
            
            if job.job_id:
                job_ids.append(job.job_id)
            time.sleep(0.5)
    
    return job_ids
