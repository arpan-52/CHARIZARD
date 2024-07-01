# Let me decide the coding strategy ....


# Name: GMRT's Radio Interferometric Processing and Polarization Extraction Routine : GRIPPER


#import necessities 

import os 


# Get some metadata from the file.....
n_chan = 2048
n_pol = 4
freq_0 = 550.048 # in MHz
n_bw = 200 # in MHz
chan_width = 200/2048
ms_name = 'TEST.ms'


# Now the first step is to split the files in to four subbands .... I am writing it to make it hardcoded for now. Users will have two options, going 
# with default config or defining it itself.

# Let's setup some directories....
import os
import logging
from datetime import datetime 

from gripp_functions import configure_logger

start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
tech_log = 'tech_gr_{}.log'.format(start_time)
science_log = 'science_gr_{}.log'.format(start_time)

# Configure the technical logger
logger_t = configure_logger('tech_logger', tech_log)

# Configure the science logger
logger_s = configure_logger('science_logger', science_log)


# Setting up the directories....

dirs = ['spw0', 'spw1', 'spw2', 'spw3']

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


logger_s.info('No science till now, only tech part....')




