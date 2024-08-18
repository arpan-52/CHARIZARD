import logging 
import os
import subprocess
import time

# Function to configure a logger
def configure_logger(name, log_file, level=logging.DEBUG):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create file handler which logs even debug messages
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)
    
    # Create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)
    
    return logger


def subbanding(ms_name, subband, spw, output_dir, casa_dir, logger, cal_name, src_name):
    """
    Create a PBS script for the given subband and submit it to the queue.
    """
    python_script_content = f"""ms_name = '{ms_name}'
spw = '{spw}'
output_dir = '{output_dir}'
cal_name = '{cal_name}'
src_name = '{src_name}'

# Call mstransform function from within CASA
mstransform(vis=ms_name, spw=spw, outputvis=f'{{output_dir}}/cal.ms', field=cal_name, datacolumn='DATA')
mstransform(vis=ms_name, spw=spw, outputvis=f'{{output_dir}}/src.ms', field=src_name, datacolumn='DATA')
"""

    python_script_file = f"run_mstransform_{subband}.py"
    with open(python_script_file, "w") as file:
        file.write(python_script_content)

    working_dir = os.getcwd()
    pbs_script_content = f"""#!/bin/bash
#PBS -N mstransform_{subband}
#PBS -l nodes=1:ppn=1
#PBS -l walltime=02:00:00
#PBS -j oe
#PBS -o {output_dir}/mstransform_{subband}.log
#PBS -q workq

cd {working_dir}
source ~/.bashrc
micromamba activate 38data
{casa_dir}/bin/casa --nologger --nogui --nologfile -c {python_script_file}
"""

    pbs_script_file = f"mstransform_{subband}.pbs"
    with open(pbs_script_file, "w") as file:
        file.write(pbs_script_content)

    submit_command = f"qsub {pbs_script_file}"
    logger.info(f"Submitting PBS script for {subband} with command: {submit_command}")
    try:
        result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        job_id = result.stdout.decode().strip()  # Job ID is the output from qsub
        logger.info(f"PBS script for {subband} submitted successfully with job ID: {job_id}")
        return job_id, subband
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit PBS script for {subband}: {e}")
        return None, subband

def extract_log_file_path(pbs_file):
    """
    Extract the log file path from the PBS script.
    """
    log_path = None
    try:
        with open(pbs_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if line.startswith("#PBS -o"):
                    # Extract the log path after the '-o' flag
                    parts = line.split()
                    if len(parts) > 1:
                        log_path = parts[2].strip()
                    break
    except Exception as e:
        print(f"Error reading {pbs_file}: {e}")
    return log_path

def wait_for_jobs_to_finish(job_info, base_output_dir, logger,prefix):
    """
    Wait for all jobs to finish, check their log files, and clean up files.
    job_info should be a list of tuples (job_id, subband).
    """
    all_successful = True
    failed_jobs = []

    while job_info:
        time.sleep(60)  # Check every minute
        for job_id, subband in job_info[:]:
            try:
                qstat_command = f"qstat {job_id}"
                result = subprocess.run(qstat_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                if result.returncode != 0:  # If qstat fails, job is probably finished
                    job_info.remove((job_id, subband))

                    # Locate the .pbs file for the subband
                    pbs_file = f"{prefix}_{subband}.pbs"
                    log_file_path = extract_log_file_path(pbs_file)
                    
                    # Construct the log file path relative to base_output_dir
                    if log_file_path:
                        log_file = os.path.join(base_output_dir,subband, os.path.basename(log_file_path))
                        print(log_file)
                    else:
                        log_file = os.path.join(base_output_dir,subband, f"{prefix}_{subband}.log")

                    if os.path.exists(log_file):
                        with open(log_file, 'r') as log:
                            log_content = log.read()
                            if "error" in log_content.lower():
                                logger.error(f"Job {job_id} (subband {subband}) failed. Check log file {log_file}.")
                                all_successful = False
                                failed_jobs.append(subband)
                            else:
                                logger.info(f"Job {job_id} (subband {subband}) completed successfully.")
                    else:
                        logger.error(f"Log file for job {job_id} (subband {subband}) not found or invalid path.")
                        all_successful = False
                        failed_jobs.append(subband)

            except subprocess.CalledProcessError as e:
                logger.error(f"Failed to check job status for {job_id}: {e}")
                all_successful = False
                failed_jobs.append(subband)

    return all_successful, failed_jobs

def cleanup_files(subband, logger,prefix):
    """
    Delete .py and .pbs files for the given subband.
    """
    try:
        python_file = f"run_{prefix}_{subband}.py"
        pbs_file = f"{prefix}_{subband}.pbs"
        
        if os.path.exists(python_file):
            os.remove(python_file)
            logger.info(f"Deleted {python_file}")
        else:
            logger.warning(f"{python_file} not found for deletion.")
        
        if os.path.exists(pbs_file):
            os.remove(pbs_file)
            logger.info(f"Deleted {pbs_file}")
        else:
            logger.warning(f"{pbs_file} not found for deletion.")
    except Exception as e:
        logger.error(f"Error during cleanup for {subband}: {e}")



def flag_cal(ms_name, subband, output_prefix, casa_dir, logger, amp_cal, phase_cal):
    """
    Create a PBS script for the given subband and submit it to the queue.
    """
    python_script_content = f"""ms_name = '{ms_name}'
output_pref = '{output_prefix}'
amp_cal = '{amp_cal}'
phase_cal = '{phase_cal}'

# Flag the calibrators

default(flagdata)
flagdata(vis=ms_name,mode="tfcrop",datacolumn="data",field=amp_cal+','+phase_cal,ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=5.0,
freqdevscale=5.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)


# Define calibration file names
initial_ap = '{output_prefix}.G0'
bp_file = '{output_prefix}.B1'
delay_file = '{output_prefix}.K1'
fluxtable = '{output_prefix}.fluxscale'
gainsol = '{output_prefix}.AP.G'

# Set the Jy source for calibration
setjy(vis=ms_name, field=amp_cal)

# Perform gain calibration for amplitude
default(gaincal)
gaincal(vis=ms_name, caltable=initial_ap, field=amp_cal, refant='C02', gaintype="G", solmode="L1R", calmode='ap', 
        solint='int', minsnr=3, interp=['nearest,nearestflag', 'nearest,nearestflag'], parang=True)

# Perform gain calibration for delay
default(gaincal)
gaincal(vis=ms_name, caltable=delay_file, field=amp_cal, solint='120s', refant='C02', gaintype='K', 
        gaintable=[initial_ap], parang=True)

# Perform bandpass calibration
default(bandpass)
bandpass(vis=ms_name, caltable=bp_file, field=amp_cal, solint='inf', refant='C02', solnorm=True, minsnr=2.0, 
         fillgaps=8, parang=True, gaintable=[delay_file, initial_ap], interp=['nearest,nearestflag', 'nearest,nearestflag'])

# Perform final gain calibration with the bandpass and delay corrections
default(gaincal)
gaincal(vis=ms_name, caltable=gainsol, solnorm=False, append=False, field=amp_cal, solint='120s', refant='C02', 
        minsnr=2.0, solmode='L1R', gaintype='G', calmode='ap', gaintable=[delay_file, bp_file], interp=['nearest,nearestflag', 'nearest,nearestflag'], parang=True)

default(gaincal)
gaincal(vis=ms_name, caltable=gainsol, solnorm=False, append=True, field=phase_cal, solint='120s', refant='C02', 
        minsnr=2.0, solmode='L1R', gaintype='G', calmode='ap', gaintable=[delay_file, bp_file], interp=['nearest,nearestflag', 'nearest,nearestflag'], parang=True)

# Perform flux scaling
scale = fluxscale(vis=ms_name, caltable=gainsol, fluxtable=fluxtable, reference=amp_cal, transfer=phase_cal, incremental=False, display=True)

"""

    python_script_file = f"run_flag_cal_{subband}.py"
    with open(python_script_file, "w") as file:
        file.write(python_script_content)

    working_dir = os.getcwd()
    pbs_script_content = f"""#!/bin/bash
#PBS -N flag_cal_{subband}
#PBS -l nodes=1:ppn=6
#PBS -l walltime=10:00:00
#PBS -j oe
#PBS -o {subband}/flag_cal_{subband}.log
#PBS -q workq

cd {working_dir}
source ~/.bashrc
micromamba activate 38data
{casa_dir}/bin/casa -n 6 {casa_dir}/bin/casa --nologger --nogui --nologfile -c {python_script_file}
"""

    pbs_script_file = f"flag_cal_{subband}.pbs"
    with open(pbs_script_file, "w") as file:
        file.write(pbs_script_content)

    submit_command = f"qsub {pbs_script_file}"
    logger.info(f"Submitting PBS script for {subband} with command: {submit_command}")
    try:
        result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        job_id = result.stdout.decode().strip()  # Job ID is the output from qsub
        logger.info(f"PBS flagcal script for {subband} submitted successfully with job ID: {job_id}")
        return job_id, subband
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit PBS script for {subband}: {e}")
        return None, subband
    




def flag_src(ms_name, subband,casa_dir, logger, src):
    """
    Create a PBS script for the given subband and submit it to the queue.
    """
    python_script_content = f"""ms_name = '{ms_name}'

src = '{src}'

# Flag the calibrators

default(flagdata)
flagdata(vis=ms_name,mode="tfcrop",datacolumn="data",field=src,ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=5.0,
freqdevscale=5.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

"""

    python_script_file = f"run_flag_src_{subband}.py"
    with open(python_script_file, "w") as file:
        file.write(python_script_content)

    working_dir = os.getcwd()
    pbs_script_content = f"""#!/bin/bash
#PBS -N flag_src_{subband}
#PBS -l nodes=1:ppn=6
#PBS -l walltime=10:00:00
#PBS -j oe
#PBS -o {subband}/flag_src_{subband}.log
#PBS -q workq

cd {working_dir}
source ~/.bashrc
micromamba activate 38data
{casa_dir}/bin/casa -n 6 {casa_dir}/bin/casa --nologger --nogui --nologfile -c {python_script_file}
"""

    pbs_script_file = f"flag_src_{subband}.pbs"
    with open(pbs_script_file, "w") as file:
        file.write(pbs_script_content)

    submit_command = f"qsub {pbs_script_file}"
    logger.info(f"Submitting PBS script for {subband} with command: {submit_command}")
    try:
        result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        job_id = result.stdout.decode().strip()  # Job ID is the output from qsub
        logger.info(f"PBS flag_src script for {subband} submitted successfully with job ID: {job_id}")
        return job_id, subband
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit PBS script for {subband}: {e}")
        return None, subband




def apply_cal(ms_name1,ms_name2, subband, output_prefix, casa_dir, logger, amp_cal, phase_cal,src):
    """
    Create a PBS script for the given subband and submit it to the queue.
    """
    python_script_content = f"""ms_name1 = '{ms_name1}'
ms_name2 = '{ms_name2}'
output_pref = '{output_prefix}'
amp_cal = '{amp_cal}'
phase_cal = '{phase_cal}'
src = '{src}'


# Define calibration file names
initial_ap = '{output_prefix}.G0'
bp_file = '{output_prefix}.B1'
delay_file = '{output_prefix}.K1'
fluxtable = '{output_prefix}.fluxscale'
gainsol = '{output_prefix}.AP.G'

default(applycal)
applycal(vis=ms_name1,field=amp_cal,gaintable=[fluxtable,delay_file,bp_file],
gainfield=[amp_cal,amp_cal,amp_cal],interp=['nearest',","],calwt=False,parang=True)

default(applycal)
applycal(vis=ms_name1,field=phase_cal,gaintable=[fluxtable,delay_file,bp_file],
gainfield=[phase_cal,amp_cal,amp_cal],interp=['nearest','nearest'],calwt=False,parang=True)
default(applycal)
applycal(vis=ms_name2,field=src,gaintable=[fluxtable,delay_file,bp_file],
gainfield=[phase_cal,amp_cal,amp_cal],interp=['nearest','linear'],calwt=False,parang=True)
"""

    python_script_file = f"run_apply_cal_{subband}.py"
    with open(python_script_file, "w") as file:
        file.write(python_script_content)

    working_dir = os.getcwd()
    pbs_script_content = f"""#!/bin/bash
#PBS -N apply_cal_{subband}
#PBS -l nodes=1:ppn=6
#PBS -l walltime=10:00:00
#PBS -j oe
#PBS -o {subband}/apply_{subband}.log
#PBS -q workq

cd {working_dir}
source ~/.bashrc
micromamba activate 38data
{casa_dir}/bin/casa -n 6 {casa_dir}/bin/casa --nologger --nogui --nologfile -c {python_script_file}
"""

    pbs_script_file = f"apply_cal_{subband}.pbs"
    with open(pbs_script_file, "w") as file:
        file.write(pbs_script_content)

    submit_command = f"qsub {pbs_script_file}"
    logger.info(f"Submitting PBS script for {subband} with command: {submit_command}")
    try:
        result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        job_id = result.stdout.decode().strip()  # Job ID is the output from qsub
        logger.info(f"PBS apply_cal script for {subband} submitted successfully with job ID: {job_id}")
        return job_id, subband
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit PBS script for {subband}: {e}")
        return None, subband
    


def flag_after_cal(ms_name1, ms_name2, subband,casa_dir, logger):
    """
    Create a PBS script for the given subband and submit it to the queue.
    """
    python_script_content = f"""ms_name1 = '{ms_name1}'

ms_name2 = '{ms_name2}'

# Flag the calibrators
default(flagdata)
flagdata(vis=ms_name1,mode="rflag",datacolumn="corrected",ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0,
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name1,mode="rflag",datacolumn="corrected",ntime='1min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0,
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

#Flag the source

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='0~1klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='1~3klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='3~5klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='2min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='>5klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

# ntime = 1min .....

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='1min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='0~1klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='1min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='1~3klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='1min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='3~5klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)

default(flagdata)
flagdata(vis=ms_name2,mode="rflag",datacolumn="corrected",ntime='1min',timecutoff=5.0,freqcutoff=5.0,timefit='line',freqfit='poly',flagdimension='freqtime',extendflags=False,timedevscale=4.0, uvrange='>5klambda',
freqdevscale=4.0,extendpols=False,growaround=False,action='apply',flagbackup=True,overwrite=True,writeflags=True)


"""

    python_script_file = f"run_flag_after_cal_{subband}.py"
    with open(python_script_file, "w") as file:
        file.write(python_script_content)

    working_dir = os.getcwd()
    pbs_script_content = f"""#!/bin/bash
#PBS -N flag_after_cal_{subband}
#PBS -l nodes=1:ppn=6
#PBS -l walltime=10:00:00
#PBS -j oe
#PBS -o {subband}/flag_after_cal_{subband}.log
#PBS -q workq

cd {working_dir}
source ~/.bashrc
micromamba activate 38data
{casa_dir}/bin/casa -n 6 {casa_dir}/bin/casa --nologger --nogui --nologfile -c {python_script_file}
"""

    pbs_script_file = f"flag_after_cal_{subband}.pbs"
    with open(pbs_script_file, "w") as file:
        file.write(pbs_script_content)

    submit_command = f"qsub {pbs_script_file}"
    logger.info(f"Submitting PBS script for {subband} with command: {submit_command}")
    try:
        result = subprocess.run(submit_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        job_id = result.stdout.decode().strip()  # Job ID is the output from qsub
        logger.info(f"PBS flag_after_cal script for {subband} submitted successfully with job ID: {job_id}")
        return job_id, subband
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit PBS script for {subband}: {e}")
        return None, subband
