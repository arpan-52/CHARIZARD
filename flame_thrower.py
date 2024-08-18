# Let me decide the coding strategy ....


# Name: CHARIZARD - Calibration and Highly Automated Radio Imaging with polariZation by Advanced Resource Distribution 

#import necessities 

import os 
import sys
from dragon_breath import subbanding,wait_for_jobs_to_finish,cleanup_files,flag_cal,flag_src,apply_cal,flag_after_cal
from datetime import datetime 

from dragon_breath import configure_logger


# Get some metadata from the file.....
n_chan = 2048
n_pol = 2
freq_0 = 550.048 # in MHz
n_bw = 200 # in MHz
chan_width = 200/2048
ms_name = 'rcs.ms'
casa_dir = '/home/apal/casa-6.6.4-34-py3.8.el8'

# Now the first step is to split the files in to four subbands .... I am writing it to make it hardcoded for now. Users will have two options, going 
# with default config or defining it itself.

# Let's setup some directories...

start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
tech_log = 'gripper_{}.log'.format(start_time)

# Configure the technical logger
logger_t = configure_logger('gripper_logger', tech_log)

# Setting up the directories....

dirs = ['spw0', 'spw1', 'spw2', 'spw3']

phase_cal='1634+627'
amp_cal = '3C286'
cal_name = phase_cal+','+amp_cal

src_name = 'RXCS'

# Create main directories
for directory in dirs:
    try:
        os.mkdir(directory)
        logger_t.info(f"Directory '{directory}' created successfully.")
    except FileExistsError:
        logger_t.warning(f"Directory '{directory}' already exists.")
    except Exception as e:
        logger_t.error(f"An error occurred while creating '{directory}': {e}")

# Create subdirectory 'caltables' in each main directory
for directory in dirs:
    try:
        logger_t.debug(f"Current working directory: {os.getcwd()}")
        os.chdir(directory)
        logger_t.debug(f"Changed to directory: {os.getcwd()}")
        os.mkdir('caltables')
        logger_t.info(f"'caltables' created in '{directory}'")
        os.chdir('..')
        logger_t.debug(f"Changed back to directory: {os.getcwd()}")
        logger_t.info(f"Subdirectories in '{directory}' created successfully.")
    except FileNotFoundError:
        logger_t.error(f"Directory '{directory}' not found. Please check the directory structure.")
        os.chdir('..')
    except FileExistsError:
        logger_t.warning(f"'caltables' in '{directory}' already exists.")
        os.chdir('..')
    except Exception as e:
        logger_t.error(f"An error occurred while creating subdirectory in '{directory}': {e}")
        os.chdir('..')

# Create subdirectory 'skepticals' in each main directory
for directory in dirs:
    try:
        logger_t.debug(f"Current working directory: {os.getcwd()}")
        os.chdir(directory)
        logger_t.debug(f"Changed to directory: {os.getcwd()}")
        os.mkdir('skepticals')
        logger_t.info(f"'caltables' created in '{directory}'")
        os.chdir('..')
        logger_t.debug(f"Changed back to directory: {os.getcwd()}")
        logger_t.info(f"Subdirectories in '{directory}' created successfully.")
    except FileNotFoundError:
        logger_t.error(f"Directory '{directory}' not found. Please check the directory structure.")
        os.chdir('..')
    except FileExistsError:
        logger_t.warning(f"'skepticals' in '{directory}' already exists.")
        os.chdir('..')
    except Exception as e:
        logger_t.error(f"An error occurred while creating subdirectory in '{directory}': {e}")
        os.chdir('..')

logger_t.info('Subbanding the MS and separating the calibrators.')


# Define subband ranges for splitting
subbands_dict = {
    "spw0": "0:124~573",
    "spw1": "0:574~1023",
    "spw2": "0:1024~1473",
    "spw3": "0:1474~1923"
}

logger_t.info('Subbanding the MS into 4 sub-MSs and splitting the source and calibrators.......')

job_info = []
for subband, spw in subbands_dict.items():
    job_id, subband_name = subbanding(ms_name, subband, spw, subband, '/home/apal/casa-6.6.4-34-py3.8.el8', logger_t, cal_name, src_name)
    if job_id:
        job_info.append((job_id, subband_name))

# Specify the base output directory
base_output_dir = '/home/apal/rcs'

# Wait for all jobs to finish
all_successful, failed_jobs = wait_for_jobs_to_finish(job_info, base_output_dir, logger_t,'mstransform')

if all_successful:
    logger_t.info('All PBS scripts have run successfully. Proceeding to next steps.')
    for subband, spw in subbands_dict.items():
        cleanup_files(subband,logger_t,'mstransform')

    # Proceed to next steps
else:
    logger_t.error('Some PBS scripts failed. The following subbands had issues: ' + ', '.join(failed_jobs))
    sys.exit(1)
    # Handle failure case


logger_t.info('Subbanding Done....')
logger_t.info('Proceeding with flagging and calibration.....')
logger_t.info('Flagging the calibrators....')

#submitting jobs to flag and get solutions of calibrators.....

job_info = []
for subband, spw in subbands_dict.items():
    caltable_pref = subband + '/caltables/cal'
    job_id, subband_name = flag_cal(subband+'/cal.ms', subband, caltable_pref,casa_dir , logger_t, amp_cal, phase_cal)
    if job_id:
        job_info.append((job_id, subband_name))

# Specify the base output directory
base_output_dir = '/home/apal/rcs'

#submitting jobs to flag source.....

job_info1= []
for subband, spw in subbands_dict.items():
    job_id1, subband_name1 = flag_src(subband+'/src.ms',subband,casa_dir, logger_t, src_name)
    if job_id1:
        job_info1.append((job_id1, subband_name))


# Wait for all jobs to finish
all_successful, failed_jobs = wait_for_jobs_to_finish(job_info, base_output_dir, logger_t,'flag_cal')

# Wait for all jobs to finish
all_successful1, failed_jobs1 = wait_for_jobs_to_finish(job_info1, base_output_dir, logger_t,'flag_src')

if all_successful:
    logger_t.info('All PBS scripts for calibrations have run successfully. Proceeding to next steps.')
    logger_t.info('Calibrators are flagged and calibration solutions are written......')
    for subband, spw in subbands_dict.items():
        cleanup_files(subband,logger_t,flag_cal)

    # Proceed to next steps
else:
    logger_t.error('Flagging and calibration solution scripts failed. The following subbands had issues: ' + ', '.join(failed_jobs))
    sys.exit(1)
    # Handle failure case


if all_successful1:
    logger_t.info('All PBS scripts for source have run successfully. Proceeding to next steps.')
    logger_t.info('Sources are flagged.....')
    for subband, spw in subbands_dict.items():
        cleanup_files(subband,logger_t,flag_src)

    # Proceed to next steps
else:
    logger_t.error('Source flagging has failed, check out.... ' + ', '.join(failed_jobs))
    sys.exit(1)
    # Handle failure case


if all_successful & all_successful1:
    logger_t.info('Proceeding with applying the solutions.....')
else:
    sys.exit(1)



job_info = []
for subband, spw in subbands_dict.items():
    caltable_pref = subband + '/caltables/cal'
    job_id, subband_name = apply_cal(subband+'/cal.ms', subband+'/src.ms',subband, caltable_pref,casa_dir , logger_t, amp_cal, phase_cal,src_name)
    if job_id:
        job_info.append((job_id, subband_name))

# Specify the base output directory
base_output_dir = '/home/apal/rcs'

# Wait for all jobs to finish
all_successful, failed_jobs = wait_for_jobs_to_finish(job_info, base_output_dir, logger_t,'mstransform')

if all_successful:
    logger_t.info('All PBS scripts to apply_cal have run successfully. Proceeding to next steps.')
    for subband, spw in subbands_dict.items():
        cleanup_files(subband,logger_t,'apply_cal')

    # Proceed to next steps
else:
    logger_t.error('Some PBS scripts failed. The following subbands had issues: ' + ', '.join(failed_jobs))
    sys.exit(1)
    # Handle failure case



job_info = []
for subband, spw in subbands_dict.items():
    job_id, subband_name = flag_after_cal(subband+'/cal.ms', subband+'/src.ms', subband,casa_dir, logger_t)
    if job_id:
        job_info.append((job_id, subband_name))

# Specify the base output directory
base_output_dir = '/home/apal/rcs'

# Wait for all jobs to finish
all_successful, failed_jobs = wait_for_jobs_to_finish(job_info, base_output_dir, logger_t,'flag_after_cal')

if all_successful:
    logger_t.info('All PBS scripts to apply_cal have run successfully. Proceeding to next steps.')
    for subband, spw in subbands_dict.items():
        cleanup_files(subband,logger_t,'flag_after_cal')

    # Proceed to next steps
else:
    logger_t.error('Some PBS scripts failed. The following subbands had issues: ' + ', '.join(failed_jobs))
    sys.exit(1)
    # Handle failure case



