# rfi_remover.py
import os 
from .utils import *
import time

def calculate_job_resources(config, job_type):
    max_ppn = config['general']['max_ppn']
    job_types = {
        'initial_flagging': {'nodes': 2, 'ppn': min(4, max_ppn), 'walltime': "04:00:00"},
        'general_flagging': {'nodes': 2, 'ppn': min(2, max_ppn), 'walltime': "08:00:00"}
    }
    return job_types.get(job_type, {'nodes': 1, 'ppn': 1, 'walltime': "00:30:00"})

def initial_flagger(ms_names, tracker, logger, config,flag_file=None):
    """Run initial flagging using bad antenna list files"""
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []

    # Get job resources
    total_channels = config['msinfo']['number_of_channels_per_spw'] * (config['msinfo']['number_of_actual_spws'])
    job_resources = calculate_job_resources(config, 'initial_flagging')


    active_spws = tracker.get_active_spws()
    if not active_spws:
        logger.warning("No active SPWs available for initial flagging")
        return []

    logger.info(f"Running initial flagging for SPWs: {active_spws}")

    for spw in active_spws:
        casa_script = ""
        for ms in ms_names:
            casa_script += f"""
flagdata(
vis='{spw}/{ms}',
mode='list',
inpfile='{spw + '/' + flag_file if flag_file else spw + "/badants.txt"}'
)
    """
        script_file = f"flag_init_{spw}.py"
        batch_file = f"flag_init_{spw}{get_script_extension(scheduler)}"
        
        with open(script_file, "w") as f:
            f.write(casa_script)
        ppn = job_resources['ppn']
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"flag_init_{spw}",
            nodes=job_resources['nodes'],
            ppn=job_resources['ppn'],
            walltime=job_resources['walltime'],
            output_dir=f"{spw}/flag_init.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
    """
        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(5)

        if job_id:
            job_info.append((job_id, spw))
            
    return job_info

# def general_flagger(ms_names, mode, tracker, logger, config, **kwargs):
#    """Run automated flagging with configurable parameters"""
#    scheduler = config['general']['PBS_or_SLURM']
#    job_info = []
   
#    job_resources = calculate_job_resources(config, 'general_flagging')
#    active_spws = tracker.get_active_spws()
   
#    if not active_spws:
#        logger.warning(f"No active SPWs available for {mode} flagging")
#        return []

#    modes = mode.split(',')
#    logger.info(f"Running {modes} flagging for SPWs: {active_spws}")

#    for spw in active_spws:
#        casa_script = ""
       
#        for ms in ms_names:
#            for flag_mode in modes:
#                if flag_mode == 'tfcrop':
#                    casa_script += f"""
# # TFCrop flagging
# flagdata(vis='{spw}/{ms}',
#         mode='tfcrop',
#         datacolumn='{kwargs.get("datacolumn", "corrected")}',
#         field='{kwargs.get("field", "")}',
#         ntime='{kwargs.get("ntime", "scan")}',
#         timecutoff={kwargs.get("t_sigma", 4.0)},
#         freqcutoff={kwargs.get("f_sigma", 4.0)},
#         timefit='line',
#         freqfit='poly',
#         flagdimension='freqtime',
#         extendflags=True,
#         timedevscale={kwargs.get("t_sigma", 4.0)},
#         freqdevscale={kwargs.get("f_sigma", 4.0)},
#         extendpols=True,
#         growaround=False,
#         action='apply',
#         flagbackup=True)
# """
#                elif flag_mode == 'rflag':
#                    casa_script += f"""
# # Division in steps of 500λ up to 25000λ
# steps = list(range(0, 25000, 500))
# for i in range(len(steps)-1):
#     if i == 0:
#         uvr = f'0~{{steps[1]}}lambda'
#     else:
#         uvr = f'{{steps[i]}}~{{steps[i+1]}}lambda'
    
#     flagdata(vis='{spw}/{ms}',
#         mode='rflag',
#         datacolumn='{kwargs.get("datacolumn", "corrected")}',
#         field='{kwargs.get("field", "")}',
#         timedevscale={kwargs.get("t_sigma", 4.0)},
#         freqdevscale={kwargs.get("f_sigma", 4.0)},
#         uvrange=uvr,
#         flagbackup=True)

# # Final range above 25000λ
# flagdata(vis='{spw}/{ms}',
#     mode='rflag',
#     datacolumn='{kwargs.get("datacolumn", "corrected")}',
#     field='{kwargs.get("field", "")}',
#     timedevscale={kwargs.get("t_sigma", 4.0)},
#     freqdevscale={kwargs.get("f_sigma", 4.0)},
#     uvrange='>25000lambda',
#     flagbackup=True)
# """
#        job_mode = mode.replace(',', '_')
#        prefix = kwargs.get('prefix', '')  # Get prefix from kwargs, empty string if not provided
#        script_file = f"flag_{job_mode}_{prefix}_{spw}.py" if prefix else f"flag_{job_mode}_{spw}.py"
#        batch_file = f"flag_{job_mode}_{prefix}_{spw}{get_script_extension(scheduler)}" if prefix else f"flag_{job_mode}_{spw}{get_script_extension(scheduler)}"

#        with open(script_file, "w") as f:
#            f.write(casa_script)

#        ppn = job_resources['ppn']
#        batch_header = create_batch_header(
#            scheduler_type=scheduler,
#            job_name = f"flag_{job_mode}_{prefix}_{spw}" if prefix else f"flag_{job_mode}_{spw}",
#            nodes=job_resources['nodes'],
#            ppn=ppn,
#            walltime=job_resources['walltime'],
#            output_dir = f"{spw}/flag_{job_mode}_{prefix}.log" if prefix else f"{spw}/flag_{job_mode}.log",
#            queue=config['general']['queue']
#        )

#        batch_content = f"""{batch_header}
# cd {os.getcwd()}
# {config['general']['preamble']}
# {config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
# """
#        with open(batch_file, "w") as f:
#            f.write(batch_content)

#        job_id = submit_job(batch_file, scheduler, logger)
#        time.sleep(5)

#        if job_id:
#            job_info.append((job_id, spw))
           
#    return job_info


def general_flagger(ms_names, mode, tracker, logger, config, **kwargs):
   """Run automated flagging with configurable parameters"""
   scheduler = config['general']['PBS_or_SLURM']
   job_info = []
   
   job_resources = calculate_job_resources(config, 'general_flagging')
   active_spws = tracker.get_active_spws()
   
   if not active_spws:
       logger.warning(f"No active SPWs available for {mode} flagging")
       return []

   modes = mode.split(',')
   logger.info(f"Running {modes} flagging for SPWs: {active_spws}")

   for spw in active_spws:
       casa_script = ""
       
       for ms in ms_names:
           for flag_mode in modes:
               if flag_mode == 'tfcrop':
                   casa_script += f"""
# TFCrop flagging
flagdata(vis='{spw}/{ms}',
        mode='tfcrop',
        datacolumn='{kwargs.get("datacolumn", "corrected")}',
        field='{kwargs.get("field", "")}',
        ntime='{kwargs.get("ntime", "scan")}',
        timecutoff={kwargs.get("t_sigma", 4.0)},
        freqcutoff={kwargs.get("f_sigma", 4.0)},
        timefit='line',
        freqfit='poly',
        flagdimension='freqtime',
        extendflags=False,
        timedevscale={kwargs.get("t_sigma", 4.0)},
        freqdevscale={kwargs.get("f_sigma", 4.0)},
        extendpols=False,
        growaround=False,
        action='apply',
        flagbackup=True)
"""
               elif flag_mode == 'rflag':
                   casa_script += f"""
# UV-based rflag with baseline-dependent thresholds
# Short baselines (<1000λ) - use 5.0 sigma
flagdata(vis='{spw}/{ms}',
    mode='rflag',
    datacolumn='{kwargs.get("datacolumn", "corrected")}',
    field='{kwargs.get("field", "")}',
    timedevscale=5.0,
    freqdevscale=5.0,
    uvrange='0~1000lambda',
    extendflags=True,
    flagbackup=True)

# Medium baselines (1000-5000λ) - use 4.5 sigma
flagdata(vis='{spw}/{ms}',
    mode='rflag',
    datacolumn='{kwargs.get("datacolumn", "corrected")}',
    field='{kwargs.get("field", "")}',
    timedevscale=4.5,
    freqdevscale=4.5,
    uvrange='1000~5000lambda',
    extendflags=True,
    flagbackup=True)

# Long baselines (>5000λ) - use 4.0 sigma
flagdata(vis='{spw}/{ms}',
    mode='rflag',
    datacolumn='{kwargs.get("datacolumn", "corrected")}',
    field='{kwargs.get("field", "")}',
    timedevscale=4.0,
    freqdevscale=4.0,
    uvrange='>5000lambda',
    extendflags=True,
    flagbackup=True)
"""
       job_mode = mode.replace(',', '_')
       prefix = kwargs.get('prefix', '')  # Get prefix from kwargs, empty string if not provided
       script_file = f"flag_{job_mode}_{prefix}_{spw}.py" if prefix else f"flag_{job_mode}_{spw}.py"
       batch_file = f"flag_{job_mode}_{prefix}_{spw}{get_script_extension(scheduler)}" if prefix else f"flag_{job_mode}_{spw}{get_script_extension(scheduler)}"

       with open(script_file, "w") as f:
           f.write(casa_script)

       ppn = job_resources['ppn']
       batch_header = create_batch_header(
           scheduler_type=scheduler,
           job_name = f"flag_{job_mode}_{prefix}_{spw}" if prefix else f"flag_{job_mode}_{spw}",
           nodes=job_resources['nodes'],
           ppn=ppn,
           walltime=job_resources['walltime'],
           output_dir = f"{spw}/flag_{job_mode}_{prefix}.log" if prefix else f"{spw}/flag_{job_mode}.log",
           queue=config['general']['queue']
       )

       batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
       with open(batch_file, "w") as f:
           f.write(batch_content)

       job_id = submit_job(batch_file, scheduler, logger)
       time.sleep(5)

       if job_id:
           job_info.append((job_id, spw))
           
   return job_info




def nami_flagger(ms_names, tracker, logger, config, **kwargs):
    """Run NAMI flagging with configurable parameters using cluster submission
    
    Args:
        ms_names (list): List of measurement set names to process
        tracker: Tracker object for managing active SPWs
        logger: Logger object for logging messages
        config (dict): Configuration dictionary
        **kwargs: Additional keyword arguments for NAMI parameters
    """
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []
    
    job_resources = calculate_job_resources(config, 'general_flagging')
    active_spws = tracker.get_active_spws()
    
    if not active_spws:
        logger.warning("No active SPWs available for NAMI flagging")
        return []

    logger.info(f"Running NAMI flagging for SPWs: {active_spws}")

    for spw in active_spws:
        nami_script = ""
        
        for ms in ms_names:
            # Build NAMI command with options
            nami_cmd = f"nami {spw}/{ms}"
            nami_cmd += f" --method {kwargs.get('method', 'poly')}"
            nami_cmd += f" --datacolumn {kwargs.get('datacolumn', 'DATA')}"
            nami_cmd += f" --degree {kwargs.get('degree', 3)}"
            nami_cmd += f" --it {kwargs.get('it', 3)}"
            nami_cmd += f" --sigma {kwargs.get('sigma', 5.0)}"
            nami_cmd += f" --timebin {kwargs.get('timebin', 30.0)}"
            nami_cmd += f" --ncpu {kwargs.get('ncpu', 8)}"
            
            if kwargs.get('plot', False):
                nami_cmd += " --plot"
            if kwargs.get('plot_dir'):
                nami_cmd += f" --plot_dir {kwargs['plot_dir']}"
            if kwargs.get('field'):
                nami_cmd += f" --field {kwargs['field']}"
            if kwargs.get('spw'):
                nami_cmd += f" --spw {kwargs['spw']}"
            if kwargs.get('writemode'):
                nami_cmd += f" --writemode {kwargs['writemode']}"
            if kwargs.get('outfile'):
                nami_cmd += f" --outfile {kwargs['outfile']}"
                
            nami_script += nami_cmd + "\n"

        prefix = kwargs.get('prefix', '')
        batch_file = f"flag_nami_{prefix}_{spw}{get_script_extension(scheduler)}" if prefix else f"flag_nami_{spw}{get_script_extension(scheduler)}"

        ppn = job_resources['ppn']
        batch_header = create_batch_header(
            scheduler_type=scheduler,
            job_name=f"flag_nami_{prefix}_{spw}" if prefix else f"flag_nami_{spw}",
            nodes=job_resources['nodes'],
            ppn=ppn,
            walltime=job_resources['walltime'],
            output_dir=f"{spw}/flag_nami_{prefix}.log" if prefix else f"{spw}/flag_nami.log",
            queue=config['general']['queue']
        )

        batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{nami_script}
"""
        with open(batch_file, "w") as f:
            f.write(batch_content)

        job_id = submit_job(batch_file, scheduler, logger)
        time.sleep(5)

        if job_id:
            job_info.append((job_id, spw))
            
    return job_info


def nami_selfcal_flagger(ms_names, tracker, logger, config, **kwargs):
    """Run NAMI flagging with configurable parameters using cluster submission
    for multiple fields in self-calibration
    
    Args:
        ms_names (list): List of measurement set paths to process (includes field path)
        tracker: Tracker object for managing active SPWs
        logger: Logger object for logging messages
        config (dict): Configuration dictionary
        **kwargs: Additional keyword arguments for NAMI parameters
    """
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []
    job_resources = calculate_job_resources(config, 'general_flagging')
    
    if not ms_names:
        logger.warning("No MS names provided for NAMI flagging")
        return []
    
    logger.info(f"Running NAMI flagging for MS files: {ms_names}")
    
    # For each MS file in the list, submit a separate job
    for ms_path in ms_names:
        # Extract SPW and field from path
        path_parts = ms_path.split('/')
        if len(path_parts) >= 2:
            spw = path_parts[0]
            field = path_parts[1]
            
            # Build NAMI command with options
            nami_cmd = f"nami {ms_path}"
            nami_cmd += f" --method {kwargs.get('method', 'poly')}"
            nami_cmd += f" --datacolumn {kwargs.get('datacolumn', 'DATA')}"
            nami_cmd += f" --degree {kwargs.get('degree', 3)}"
            nami_cmd += f" --it {kwargs.get('it', 3)}"
            nami_cmd += f" --sigma {kwargs.get('sigma', 5.0)}"
            nami_cmd += f" --timebin {kwargs.get('timebin', 30.0)}"
            nami_cmd += f" --ncpu {kwargs.get('ncpu', 8)}"
            
            if kwargs.get('plot', False):
                nami_cmd += " --plot"
            if kwargs.get('plot_dir'):
                # Ensure plot_dir is field-specific if provided
                field_plot_dir = f"{kwargs['plot_dir']}/{field}"
                nami_cmd += f" --plot_dir {field_plot_dir}"
            if kwargs.get('spw'):
                nami_cmd += f" --spw {kwargs['spw']}"
            if kwargs.get('writemode'):
                nami_cmd += f" --writemode {kwargs['writemode']}"
            if kwargs.get('outfile'):
                # Ensure outfile is field-specific if provided
                field_outfile = f"{kwargs['outfile']}_{field}"
                nami_cmd += f" --outfile {field_outfile}"
                
            # We already have field from the path, no need to specify it again
            # if kwargs.get('field'): 
            #     nami_cmd += f" --field {kwargs['field']}"
            
            prefix = kwargs.get('prefix', '')
            
            # Include field in the batch file and job names
            batch_file = f"flag_nami_{prefix}_{spw}_{field}{get_script_extension(scheduler)}" if prefix else f"flag_nami_{spw}_{field}{get_script_extension(scheduler)}"
            
            ppn = job_resources['ppn']
            
            # Ensure log file goes to field-specific directory
            batch_header = create_batch_header(
                scheduler_type=scheduler,
                job_name=f"flag_nami_{prefix}_{spw}_{field}" if prefix else f"flag_nami_{spw}_{field}",
                nodes=job_resources['nodes'],
                ppn=ppn,
                walltime=job_resources['walltime'],
                output_dir=f"{spw}/{field}/flag_nami_{prefix}.log" if prefix else f"{spw}/{field}/flag_nami.log",
                queue=config['general']['queue']
            )
            
            batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{nami_cmd}
"""
            
            with open(batch_file, "w") as f:
                f.write(batch_content)
                
            job_id = submit_job(batch_file, scheduler, logger)
            time.sleep(5)
            if job_id:
                # Include field in job info
                job_info.append((job_id, spw, field))
    
    return job_info

def general_selfcal_flagger(ms_names, mode, tracker, logger, config, **kwargs):
    """Run automated flagging with configurable parameters"""
    scheduler = config['general']['PBS_or_SLURM']
    job_info = []
    job_resources = calculate_job_resources(config, 'general_flagging')
    
    if not ms_names:
        logger.warning(f"No MS names provided for {mode} flagging")
        return []
    
    modes = mode.split(',')
    logger.info(f"Running {modes} flagging for MS files: {ms_names}")
    
    # For each MS file in the list, submit a separate job
    for ms_path in ms_names:
        # Extract SPW and field from path
        path_parts = ms_path.split('/')
        if len(path_parts) >= 2:
            spw = path_parts[0]
            field = path_parts[1]
            
            casa_script = ""
            for flag_mode in modes:
                if flag_mode == 'tfcrop':
                    casa_script += f"""
# TFCrop flagging
flagdata(vis='{ms_path}',
         mode='tfcrop',
         datacolumn='{kwargs.get("datacolumn", "corrected")}',
         field='{kwargs.get("field", "")}',
         ntime='{kwargs.get("ntime", "scan")}',
         timecutoff={kwargs.get("t_sigma", 4.0)},
         freqcutoff={kwargs.get("f_sigma", 4.0)},
         timefit='line',
         freqfit='poly',
         flagdimension='freqtime',
         extendflags=False,
         timedevscale={kwargs.get("t_sigma", 4.0)},
         freqdevscale={kwargs.get("f_sigma", 4.0)},
         extendpols=False,
         growaround=False,
         action='apply',
         flagbackup=True)
"""
                elif flag_mode == 'rflag':
                    casa_script += f"""
# UV-based rflag with baseline-dependent thresholds
# Short baselines (<1000λ) - use 5.0 sigma
flagdata(vis='{ms_path}',
         mode='rflag',
         datacolumn='{kwargs.get("datacolumn", "corrected")}',
         field='{kwargs.get("field", "")}',
         timedevscale=5.0,
         freqdevscale=5.0,
         uvrange='0~1000lambda',
         extendflags=True,
         flagbackup=True)
# Medium baselines (1000-5000λ) - use 4.5 sigma
flagdata(vis='{ms_path}',
         mode='rflag',
         datacolumn='{kwargs.get("datacolumn", "corrected")}',
         field='{kwargs.get("field", "")}',
         timedevscale=4.5,
         freqdevscale=4.5,
         uvrange='1000~5000lambda',
         extendflags=True,
         flagbackup=True)
# Long baselines (>5000λ) - use 4.0 sigma
flagdata(vis='{ms_path}',
         mode='rflag',
         datacolumn='{kwargs.get("datacolumn", "corrected")}',
         field='{kwargs.get("field", "")}',
         timedevscale=4.0,
         freqdevscale=4.0,
         uvrange='>5000lambda',
         extendflags=True,
         flagbackup=True)
"""
            
            job_mode = mode.replace(',', '_')
            prefix = kwargs.get('prefix', '')
            
            # Include field in the script and job names
            script_file = f"flag_{job_mode}_{prefix}_{spw}_{field}.py" if prefix else f"flag_{job_mode}_{spw}_{field}.py"
            batch_file = f"flag_{job_mode}_{prefix}_{spw}_{field}{get_script_extension(scheduler)}" if prefix else f"flag_{job_mode}_{spw}_{field}{get_script_extension(scheduler)}"
            
            with open(script_file, "w") as f:
                f.write(casa_script)
            
            # Ensure field directory exists
            field_dir = f"{spw}/{field}"
            os.makedirs(field_dir, exist_ok=True)
            
            # Set up log file path that matches the path expected by wait_for_field_jobs_to_finish
            log_filename = f"flag_{job_mode}_{prefix}.log" if prefix else f"flag_{job_mode}.log"
            log_path = f"{spw}/{field}/{log_filename}"
            
            ppn = job_resources['ppn']
            batch_header = create_batch_header(
                scheduler_type=scheduler,
                job_name = f"flag_{job_mode}_{prefix}_{spw}_{field}" if prefix else f"flag_{job_mode}_{spw}_{field}",
                nodes=job_resources['nodes'],
                ppn=ppn,
                walltime=job_resources['walltime'],
                output_dir=log_path,
                queue=config['general']['queue']
            )
            
            batch_content = f"""{batch_header}
cd {os.getcwd()}
{config['general']['preamble']}
{config['general']['casa_dir']}/bin/mpicasa -n {ppn} {config['general']['casa_dir']}/bin/casa --nologger --nogui -c {script_file}
"""
            
            with open(batch_file, "w") as f:
                f.write(batch_content)
            
            job_id = submit_job(batch_file, scheduler, logger)
            time.sleep(5)
            if job_id:
                # Include full identification for the job to match wait_for_field_jobs_to_finish expectations
                job_info.append((job_id, spw, field))
    
    return job_info