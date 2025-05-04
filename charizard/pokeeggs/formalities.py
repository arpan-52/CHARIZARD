import os 
from .utils import *
import sys
from .rfi_remover import *

def remove_locks(ms_path):
   lock_file = f"{ms_path}/table.lock"
   if os.path.exists(lock_file):
       os.remove(lock_file)

def calculate_job_resources(config, job_type):
   max_ppn = config['general']['max_ppn'] 
   job_types = {
       'splitting': {'nodes': 1, 'ppn': 1, 'walltime': "08:00:00"},
       'bad_antenna': {'nodes': 1, 'ppn': min(8, max_ppn), 'walltime': "04:00:00"},
   }
   return job_types.get(job_type, {'nodes': 1, 'ppn': 1, 'walltime': "00:30:00"})

def set_up_directories(num_spw, logger):
   """Create SPW directories with required subdirectories"""
   
   for i in range(num_spw):
       spw_dir = f'spw{i}'
       try:
           os.makedirs(spw_dir, exist_ok=True)
           os.makedirs(f'{spw_dir}/caltables', exist_ok=True) 
           os.makedirs(f'{spw_dir}/plots', exist_ok=True)
           logger.info(f"Created directory structure for {spw_dir}")
       except Exception as e:
           logger.error(f"Failed creating directories for {spw_dir}: {e}")
           raise



# def calculate_spw_ranges(total_spws, channels_per_spw, target_spws):
#     """Calculate SPW:channel ranges for target number of SPWs"""
#     total_channels = total_spws * channels_per_spw
#     channels_per_target = total_channels // target_spws
    
#     spw_ranges = {}
#     for i in range(target_spws):
#         start_chan = i * channels_per_target
#         end_chan = (i + 1) * channels_per_target - 1
#         start_spw = start_chan // channels_per_spw
#         end_spw = end_chan // channels_per_spw
        
#         range_str = []
#         for spw in range(start_spw, end_spw + 1):
#             s = max(0, start_chan - spw * channels_per_spw)
#             e = min(channels_per_spw - 1, end_chan - spw * channels_per_spw)
#             range_str.append(f"{spw}:{s}~{e}")
            
#         spw_ranges[f'spw{i}'] = ';'.join(range_str)
    
#     return spw_ranges


# def splitting_fields(msname, calibrator, source, casa_dir, channels_per_spw, total_spws, target_spws, scheduler, logger, config):
#     """Submit jobs to split and combine fields into target number of SPWs"""
#     # Calculate total channels
#     total_channels = channels_per_spw * total_spws

#     job_resources = calculate_job_resources(config, 'splitting')

#     spw_ranges = {}
#     channels_per_target = (total_spws * channels_per_spw) // target_spws

#     for i in range(target_spws):
#         start = i * channels_per_target
#         end = ((i + 1) * channels_per_target) - 1
#         ranges = []
        
#         for spw in range(total_spws):
#             spw_start = max(0, start - (spw * channels_per_spw))
#             spw_end = min(channels_per_spw - 1, end - (spw * channels_per_spw))
#             if spw_start <= spw_end:
#                 ranges.append(f"{spw}:{spw_start}~{spw_end}")
        
#         spw_ranges[f'spw{i}'] = ';'.join(ranges)

#     job_info = []
#     for subband, spw in spw_ranges.items():
#         casa_script = f"""
# mstransform(vis='{msname}', spw='{spw}',
# outputvis='{subband}/cal.ms', field='{calibrator}', datacolumn='data')
# mstransform(vis='{msname}', spw='{spw}',
# outputvis='{subband}/src.ms', field='{source}', datacolumn='data')
#     """
#         script_file = f"split_{subband}.py"
#         batch_file = f"split_{subband}{get_script_extension(scheduler)}"
        
#         with open(script_file, "w") as f:
#             f.write(casa_script)
        
#         batch_header = create_batch_header(
#             scheduler_type=scheduler, 
#             job_name=f"split_{subband}", 
#             nodes=job_resources['nodes'], 
#             ppn=job_resources['ppn'],
#             walltime=job_resources['walltime'], 
#             output_dir=f"{subband}/split.log", 
#             queue=config['general']['queue']
#         )
        
#         batch_content = f"""{batch_header}
#     cd {os.getcwd()}
#     source ~/.bashrc
#     micromamba activate 38data
#     {casa_dir}/bin/casa --nologger --nogui -c {script_file}
#     """
#         with open(batch_file, "w") as f:
#             f.write(batch_content)
        
#         job_id = submit_job(batch_file, scheduler, logger)
#         time.sleep(3)
#         if job_id:
#             job_info.append((job_id, subband))

#     return job_info

def calculate_spw_ranges(channels_per_spw, total_spws, target_spws):
    def get_trimmed_channels(n_channels):
        trim = int(round(n_channels * 0.05))
        start = trim
        end = n_channels - trim - 1
        usable = end - start + 1
        return start, end, usable
    
    start, end, usable = get_trimmed_channels(channels_per_spw)
    total_usable = usable * total_spws
    channels_per_target = total_usable // target_spws
    
    spw_ranges = {}
    channels_processed = 0
    
    for i in range(target_spws):
        ranges = []
        channels_needed = channels_per_target
        curr_spw = (channels_processed // usable)
        
        while channels_needed > 0 and curr_spw < total_spws:
            spw_pos = channels_processed % usable
            available = usable - spw_pos
            to_take = min(available, channels_needed)
            
            if to_take > 0:
                spw_start = start + spw_pos
                spw_end = spw_start + to_take - 1
                ranges.append(f"{curr_spw}:{spw_start}~{spw_end}")
                
            channels_needed -= to_take
            channels_processed += to_take
            
            if channels_processed % usable == 0:
                curr_spw += 1
                
        spw_ranges[f'spw{i}'] = ';'.join(ranges)
    
    return spw_ranges


def splitting_fields(msname, calibrator, source, casa_dir, channels_per_spw, total_spws, target_spws, scheduler, logger, config):
    """Submit jobs to split and combine fields into target number of SPWs"""
    
    job_resources = calculate_job_resources(config, 'splitting')
    spw_ranges = calculate_spw_ranges(channels_per_spw, total_spws, target_spws)
    
    job_info = []
    for subband, spw in spw_ranges.items():
        casa_script = f"""
mstransform(vis='{msname}', spw='{spw}',
    outputvis='{subband}/cal.ms', field='{calibrator}', datacolumn='data')
mstransform(vis='{msname}', spw='{spw}',
    outputvis='{subband}/src.ms', field='{source}', datacolumn='data')
"""
        script_file = f"split_{subband}.py"
        batch_file = f"split_{subband}{get_script_extension(scheduler)}"
        
        with open(script_file, "w") as f:
            f.write(casa_script)
            
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"split_{subband}",
            nodes=job_resources['nodes'],
            ppn=job_resources['ppn'],
            walltime=job_resources['walltime'],
            output_dir=f"{subband}/split.log",
            queue=config['general']['queue']
        )
        
        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{casa_dir}/bin/casa --nologger --nogui -c {script_file}
"""
        with open(batch_file, "w") as f:
            f.write(batch_content)
            
        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(3)
        if job_id:
            job_info.append((job_id, subband))
            
    return job_info


def find_BAs(config, logger, tracker):
    """Run find_bad_antenna on calibrator MSs"""
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []

    job_resources = calculate_job_resources(config, 'bad_antenna')
    ppn = job_resources['ppn']

    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.warning("No active SPWs available for bad antenna detection")
        return []

    logger.info(f"Running bad antenna detection for SPWs: {active_spws}")

    # Get user-specified bad antennas from config
    user_bad_antennas = config['pipeline'].get('bad_antenna_list', '')

    for subband in active_spws:
        script = f"""
from charizard.pokeeggs.utils import find_dead_antennas, write_flag_commands
import os

# Detect bad antennas
find_dead_antennas('{subband}/cal.ms', '{subband}/badants.txt', n_processes={ppn})

# Dynamically read the number of channels for edge flagging
def get_ms_spec(ms_path):
    from casacore import tables
    with tables.table(ms_path + '/SPECTRAL_WINDOW') as tb:
        nchan = tb.getcol('NUM_CHAN')
        num_spws = tb.nrows()
    return nchan[0], num_spws

num_channels, num_spws = get_ms_spec('{subband}/cal.ms')

# Append flag commands in 'append' mode
import os
bad_ant_list = '{user_bad_antennas}'

if os.path.exists('{subband}/badants.txt'):
    mode = 'a'
else:
    mode = 'w'

with open('{subband}/badants.txt', mode) as f:
    if bad_ant_list and bad_ant_list != 'None' and len(bad_ant_list) > 0:
        f.write('\\n')
        f.write(f"mode='manual' antenna= '{{bad_ant_list}}' reason='user_specified'")
        f.write('\\n')

write_flag_commands('{subband}/badants.txt',mode='a',
   flags_to_include=['edge', 'quack', 'clip', 'autocorr'],
   nchan=num_channels, nspws = num_spws,edge_percent=5,
   quack_interval=4.5
)

"""
        script_file = f"find_ba_{subband}.py"
        batch_file = f"find_ba_{subband}{get_script_extension(scheduler)}"
        
        with open(script_file, "w") as f:
            f.write(script)

        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"badant_{subband}",
            nodes=job_resources['nodes'],
            ppn=job_resources['ppn'],
            walltime=job_resources['walltime'],
            output_dir=f"{subband}/badant.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
python3 {script_file}
"""
        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(1)

        if job_id:
            job_info.append((job_id, subband))

    return job_info

def setup_battle(config, logger, tracker):
    if not config['pipeline']['initialization']:
        logger.info("You have everything in place (I was told), Moving onto next steps!")
        return True
    
    requested_steps = []
    completed_steps = []

    if config['pipeline']['make_the_structure']:
        requested_steps.append('make_the_structure')
        logger.info("Setting up file structure...")
        set_up_directories(config['msinfo']['number_of_processing_spw'], logger)

        msinfo = config['msinfo']
        
        # Get unique calibrators
        calibrators = set()
        for cal_type in ['amp_cal', 'phase_cal', 'leakage_cal', 'polang_cal']:
            if msinfo[cal_type]:
                calibrators.update(msinfo[cal_type].split(','))
        calibrator_string = ','.join(calibrators)

        # Submit split jobs
        job_split = splitting_fields(
            msname=msinfo['parent_ms'],
            calibrator=calibrator_string,
            source=msinfo['source_list'],
            casa_dir=config['general']['casa_dir'],
            channels_per_spw=msinfo['number_of_channels_per_spw'],
            total_spws=msinfo['number_of_actual_spws'],
            target_spws=msinfo['number_of_processing_spw'],
            scheduler=config['general']['PBS_or_SLURM'],
            logger=logger,
            config=config
        )


        job_split_copy = job_split

        # Track split jobs
        tracker.add_jobs('splitting', job_split)

        # Wait for splits to complete
        all_successful, failed = wait_for_jobs_to_finish(
            job_split_copy,
            config['general']['working_directory'],
            logger,
            'split',
            config['general']['PBS_or_SLURM']
        )

        if not tracker.check_brotherhood(failed):
            logger.error(f"Splitting failed for: {failed}")
            cleanup_and_exit([j[0] for j in job_split], config['general']['PBS_or_SLURM'], logger)
        else:
            logger.info(f"Split completed. Failed SPWs: {failed if failed else 'None'}")
            for spw in tracker.get_active_spws():
                tracker.handle_job_completion('splitting', spw, True)
                cleanup_files(spw, 'split', config['general']['PBS_or_SLURM'], logger)
            completed_steps.append('make_the_structure')

    if config['pipeline']['flagging_badants']:
        requested_steps.append('flagging_badants')
        logger.info("Let's find the lazy antennas.")
        # Check if we have any active SPWs
        active_spws = tracker.get_active_spws()
        if not active_spws:
            logger.error("No active SPWs available for bad antenna detection")
            return

        # Submit bad antenna jobs for active SPWs
        logger.info(f"Running bad antenna detection for SPWs: {active_spws}")
        job_ba = find_BAs(config, logger, tracker)
        tracker.add_jobs('bad_antenna', job_ba)
        job_ba_copy = job_ba
        # Wait for bad antenna jobs
        all_successful, failed = wait_for_jobs_to_finish(
            job_ba_copy,
            config['general']['working_directory'],
            logger,
            'find_ba',
            config['general']['PBS_or_SLURM']
        )

        if not tracker.check_brotherhood(failed):
            logger.error(f"Bad antenna detection failed for: {failed}")
            cleanup_and_exit([j[0] for j in job_ba], config['general']['PBS_or_SLURM'], logger)
        else:
            logger.info(f"Bad antenna detection and initial flagging range setup are completed. Failed SPWs: {failed if failed else 'None'}")
            # Only handle successful SPWs
            logger.info(f"Processing will continue with SPWs: {active_spws}")
            for spw in tracker.get_active_spws():
                ms_n = '{}/cal.ms'.format(spw)
                remove_locks(ms_n)
                tracker.handle_job_completion('bad_antenna', spw, True)
                cleanup_files(spw, 'find_ba', config['general']['PBS_or_SLURM'], logger)
                print(spw)
            job_initial_flag = initial_flagger(
                ms_names=['cal.ms'], 
                tracker=tracker, 
                logger=logger, 
                config=config
            )
            tracker.add_jobs('initial_flagging', job_initial_flag)

            job_initial_flag_copy = job_initial_flag
            # Wait for initial flagging jobs
            all_successful, failed = wait_for_jobs_to_finish(
                job_initial_flag_copy,
                config['general']['working_directory'],
                logger,
                'flag_init',
                config['general']['PBS_or_SLURM']
            )

            if not tracker.check_brotherhood(failed):
                logger.error(f"Initial flagging failed for: {failed}")
                cleanup_and_exit([j[0] for j in job_initial_flag], config['general']['PBS_or_SLURM'], logger)
            else:
                logger.info(f"Initial flagging completed. Failed SPWs: {failed if failed else 'None'}")
                
                active_spws = tracker.get_active_spws()
                logger.info(f"Initial flagging will continue with SPWs: {active_spws}")
                for spw in active_spws:
                    tracker.handle_job_completion('initial_flagging', spw, True)
                    cleanup_files(spw, 'flag_init', config['general']['PBS_or_SLURM'], logger)
                completed_steps.append('flagging_badants')

    if config['pipeline']['get_away_RFIs']:
        requested_steps.append('get_away_RFIs')
        # Launch calibrator flagging
        job_cal_flag = general_flagger(
            ms_names=['cal.ms'], 
            mode='tfcrop',
            tracker=tracker, 
            logger=logger, 
            config=config,
            t_sigma=4.0,
            f_sigma=4.0,
            datacolumn='DATA'
        )
        tracker.add_jobs('calibrator_flagging', job_cal_flag)
        job_cal_flag_copy = job_cal_flag
        # Wait for calibrator flagging jobs
        cal_successful, cal_failed = wait_for_jobs_to_finish(
            job_cal_flag_copy,
            config['general']['working_directory'],
            logger,
            'flag_tfcrop',
            config['general']['PBS_or_SLURM']
        )

        if not tracker.check_brotherhood(cal_failed):
            logger.error(f"Calibrator flagging failed for: {cal_failed}")
            cleanup_and_exit([j[0] for j in job_cal_flag], config['general']['PBS_or_SLURM'], logger)
        else:
            logger.info("Calibrators are ready for calibration")
            active_spws = tracker.get_active_spws()
            logger.info('Starting some basic flagging on the sources, clipping zeroes, auto-correlations, and quacks!!')
            for spw in active_spws:
                tracker.handle_job_completion('calibrator_flagging', spw, True)
                cleanup_files(spw, 'flag_tfcrop', config['general']['PBS_or_SLURM'], logger)
                for spw in active_spws:
                    write_flag_commands(f"{spw}/source_flags.txt", mode='w',
                                    flags_to_include=['autocorr', 'clip', 'quack'])

            # Apply source flags
            job_source_flag = initial_flagger(
                ms_names=['src.ms'],
                tracker=tracker,
                logger=logger, 
                config=config,
                flag_file='source_flags.txt'
            )
            logger.info("I have submitted a basic flagging job for the sources, let's not wait for them.")
            logger.info("I am thriving to see the calibration solutions..). Do you know about plotms ..|.. That's used to be my best buddy for a long time.")
            tracker.add_jobs('source_flagging', job_source_flag)

            job_source_flag_copy = job_source_flag

            source_successful, source_failed = wait_for_jobs_to_finish(
                job_source_flag_copy,
                config['general']['working_directory'],
                logger,
                'flag_init',
                config['general']['PBS_or_SLURM']
            )
            if not tracker.check_brotherhood(source_failed):
                logger.error(f"Source Initial flagging failed for: {source_failed}")
                cleanup_and_exit([j[0] for j in job_source_flag], config['general']['PBS_or_SLURM'], logger)
            else:
                logger.info(f"Source initial flagging completed. Failed SPWs: {source_failed if source_failed else 'None'}")
                
                active_spws = tracker.get_active_spws()
                logger.info(f"Source Initial flagging will continue with SPWs: {active_spws}")
                for spw in active_spws:
                    tracker.handle_job_completion('source_flagging', spw, True)
                    cleanup_files(spw, 'flag_init', config['general']['PBS_or_SLURM'], logger)
                completed_steps.append('get_away_RFIs')
    else:
        logger.info("You have everything in place (I was told), Moving onto next steps!")
        return True
    

    if set(requested_steps) == set(completed_steps):
        return True
    else:
        logger.error(f"Not all requested steps were completed. Requested: {requested_steps}, Completed: {completed_steps}")
        return False
