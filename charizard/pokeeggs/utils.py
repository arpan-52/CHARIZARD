from casacore import tables
import numpy as np
import time
from multiprocessing import Pool, cpu_count
import os
from collections import defaultdict
import sys



def print_runtime(start_time, message=""):
    runtime = time.time() - start_time
    if message:
        print(f"{message}: {runtime:.2f} seconds")
    else:
        print(f"Runtime: {runtime:.2f} seconds")

def calculate_edge_channels(nchan, percent=5):
    """Calculate edge channels to flag based on total number of channels."""
    edge_width = max(1, int(nchan * (percent / 100)))
    edge_spw = []
    
    # Flag first and last edge_width channels
    if edge_width > 0:
        edge_spw.append(f"0~{edge_width-1}")  # First channels
        edge_spw.append(f"{nchan-edge_width}~{nchan-1}")  # Last channels
    
    return edge_spw


def get_ms_info(ms_path):
    with tables.table(ms_path) as tb:
        scans = np.unique(tb.getcol('SCAN_NUMBER'))
        fields = np.unique(tb.getcol('FIELD_ID'))
        spws = np.unique(tb.getcol('DATA_DESC_ID'))
        ant1 = tb.getcol('ANTENNA1', 0, min(10000, tb.nrows()))
        ant2 = tb.getcol('ANTENNA2', 0, min(10000, tb.nrows()))
        active_ants = np.unique(np.concatenate([ant1, ant2]))
    
    with tables.table(ms_path + '/ANTENNA') as tb:
        ant_names = tb.getcol('NAME')
        mock_mask = np.ones(len(ant_names), dtype=bool)
        mock_mask[active_ants] = False
        mock_ants = np.where(mock_mask)[0]

    with tables.table(ms_path + '/SPECTRAL_WINDOW') as tb:
        nchan = tb.getcol('NUM_CHAN')
    
    print(f"Found {len(fields)} fields, {len(spws)} SPWs, {len(ant_names)} antennas")
    print(f"Found {len(mock_ants)} mock antennas: {[ant_names[i] for i in mock_ants]}")
    return scans, fields, spws, ant_names, nchan, mock_ants

def write_flag_commands(output_file, mode='w', flags_to_include=None, **kwargs):
    """Generate flag commands for CASA.
    
    Args:
        output_file: Output file path
        flags_to_include: List of flag types to include ['shadow','autocorr','edge','clip','quack','badant']
        **kwargs: Parameters for specific flags
    """
    if flags_to_include is None:
        flags_to_include = ['shadow', 'autocorr', 'edge', 'clip', 'quack', 'badant']
    
    commands = []
    
    if 'shadow' in flags_to_include:
        commands.extend([
            "# Shadow flagging",
            "mode='shadow' reason='shadow'",
        ])
    
    if 'autocorr' in flags_to_include:
        commands.extend([
            "# Autocorrelations",
            "mode='manual' autocorr=True reason='autocorr'",
        ])
    
    if 'edge' in flags_to_include and 'nchan' in kwargs and 'nspws' in kwargs:
        edge_spw = []
        nchan = kwargs['nchan']
        nspws = kwargs['nspws']
        edge_width = max(1, int(nchan * (kwargs.get('edge_percent', 5) / 100)))
        
        for spw in range(nspws):
            edge_spw.append(f"{spw}:0~{edge_width-1}")  # First channels
            edge_spw.append(f"{spw}:{nchan-edge_width}~{nchan-1}")  # Last channels
        
        commands.extend([
            "# Edge channels",
            f"mode='manual' spw='{','.join(edge_spw)}' reason='edgespw' name='edgespw'",
        ])
    
    if 'clip' in flags_to_include:
        commands.extend([
            "# Clip zeros",
            "mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'",
        ])
    
    if 'quack' in flags_to_include:
        commands.extend([
            "# Quack flagging",
            f"mode='quack' quackinterval={kwargs.get('quack_interval', 4.5)} quackmode='beg' quackincrement=False reason='quackbeg' ",
            f"mode='quack' quackinterval={kwargs.get('quack_interval', 4.5)} quackmode='endb' quackincrement=False reason='quackend' "

        ])
    
    if 'badant' in flags_to_include and 'scan_spw_ants' in kwargs:
        commands.append("# Bad antenna commands")
        scan_spw_ants = kwargs['scan_spw_ants']
        for scan in sorted(scan_spw_ants.keys()):
            spw_list, ant_list = [], []
            for spw, antennas in sorted(scan_spw_ants[scan].items()):
                if antennas:
                    spw_list.append(str(spw))
                    ant_list.extend(list(antennas))
            
            if ant_list:
                ant_list = list(dict.fromkeys(ant_list))
                commands.extend([
                    f"mode='manual' scan='{scan}' spw='{','.join(spw_list)}' "
                    f"antenna='{','.join(ant_list)}' reason='bad_antenna' "
                    f"name='bad_antenna_{scan}'",
                ])
    
    # commands.append("mode='summary'\n")
    
    with open(output_file, mode) as f:
        f.write('\n'.join(commands))

def process_spw_field(params):
    """Enhanced processing with scan-by-scan bad antenna detection."""
    ms_path, spw, field, ant_names, mock_ants = params
    
    try:
        with tables.table(ms_path) as tb:
            sel = tb.query(f"DATA_DESC_ID = {spw} AND FIELD_ID = {field}", sortlist='SCAN_NUMBER')
            
            if sel.nrows() == 0:
                return None
            
            scans = sel.getcol('SCAN_NUMBER')
            ant1 = sel.getcol('ANTENNA1')
            ant2 = sel.getcol('ANTENNA2')
            data = sel.getcol('DATA')
            flags = sel.getcol('FLAG')
            
            unique_scans = np.unique(scans)
            num_ants = len(ant_names)
            
            # Handle different polarization setups
            npols = data.shape[-1]
            
            # Store bad antennas per scan
            scan_bad_antennas = {}
            
            # First pass: calculate global reference amplitude across all scans
            global_ant_stats = defaultdict(lambda: {
                'valid_amplitudes': [],
                'is_active': False
            })
            
            # Collect all amplitudes for global reference
            for i in range(len(data)):
                a1, a2 = ant1[i], ant2[i]
                
                # Calculate amplitude based on polarization setup
                if npols == 4:  # Full polarization
                    pol_flags = flags[i, :, 0] | flags[i, :, 3]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        amp = (np.abs(data[i, valid_mask, 0]) + 
                              np.abs(data[i, valid_mask, 3])) / 2
                elif npols == 2:  # Dual polarization
                    pol_flags = flags[i, :, 0] | flags[i, :, 1]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        amp = (np.abs(data[i, valid_mask, 0]) + 
                              np.abs(data[i, valid_mask, 1])) / 2
                else:  # Single polarization
                    valid_mask = ~flags[i, :, 0]
                    if np.any(valid_mask):
                        amp = np.abs(data[i, valid_mask, 0])
                
                # Update global statistics for both antennas
                for ant in [a1, a2]:
                    global_ant_stats[ant]['is_active'] = True
                    if np.any(valid_mask):
                        global_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
            
            # Calculate global reference amplitude
            good_amps = []
            for ant in range(num_ants):
                stats = global_ant_stats[ant]
                if stats['is_active'] and len(stats['valid_amplitudes']) > 0:
                    med_amp = np.median(stats['valid_amplitudes'])
                    if med_amp > 0:
                        good_amps.append(med_amp)
            
            if not good_amps:
                return None
            
            # Use 75th percentile as reference
            reference_amp = np.percentile(good_amps, 75)
            threshold = reference_amp * 0.05  # 5% threshold
            
            # Second pass: Process each scan separately
            for scan in unique_scans:
                # Get data for this specific scan
                scan_mask = scans == scan
                scan_indices = np.where(scan_mask)[0]
                
                if len(scan_indices) == 0:
                    continue
                
                # Initialize antenna statistics for this scan
                scan_ant_stats = defaultdict(lambda: {
                    'total_samples': 0,
                    'flagged_samples': 0,
                    'valid_amplitudes': [],
                    'is_active': False
                })
                
                # Process baselines for this scan
                for idx in scan_indices:
                    a1, a2 = ant1[idx], ant2[idx]
                    
                    # Calculate amplitude based on polarization setup
                    if npols == 4:  # Full polarization
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 3]
                        valid_mask = ~pol_flags
                        if np.any(valid_mask):
                            amp = (np.abs(data[idx, valid_mask, 0]) + 
                                  np.abs(data[idx, valid_mask, 3])) / 2
                    elif npols == 2:  # Dual polarization
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 1]
                        valid_mask = ~pol_flags
                        if np.any(valid_mask):
                            amp = (np.abs(data[idx, valid_mask, 0]) + 
                                  np.abs(data[idx, valid_mask, 1])) / 2
                    else:  # Single polarization
                        valid_mask = ~flags[idx, :, 0]
                        if np.any(valid_mask):
                            amp = np.abs(data[idx, valid_mask, 0])
                    
                    # Update statistics for both antennas in this scan
                    for ant in [a1, a2]:
                        scan_ant_stats[ant]['is_active'] = True
                        scan_ant_stats[ant]['total_samples'] += len(valid_mask)
                        scan_ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                        if np.any(valid_mask):
                            scan_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
                
                # Identify bad antennas in this scan
                scan_bad_ants = []
                for ant in range(num_ants):
                    stats = scan_ant_stats[ant]
                    if (stats['is_active'] and 
                        len(stats['valid_amplitudes']) > 0 and
                        ant not in mock_ants):
                        
                        # Calculate flag percentage for this scan
                        flag_percent = (stats['flagged_samples'] / 
                                      stats['total_samples'] * 100 if 
                                      stats['total_samples'] > 0 else 100)
                        
                        # Skip if heavily flagged in this scan
                        if flag_percent > 90:
                            continue
                        
                        med_amp = np.median(stats['valid_amplitudes'])
                        if med_amp < threshold:
                            scan_bad_ants.append({
                                'name': ant_names[ant],
                                'median_amplitude': med_amp,
                                'ratio_to_reference': med_amp/reference_amp,
                                'flag_percentage': flag_percent
                            })
                
                if scan_bad_ants:
                    scan_bad_antennas[scan] = scan_bad_ants
            
            # Create results structure
            results = {
                'field': field,
                'spw': spw,
                'mock_antennas': [ant_names[i] for i in mock_ants],
                'scan_bad_antennas': scan_bad_antennas,
                'reference_amplitude': reference_amp
            }
            
            sel.close()
            return results
            
    except Exception as e:
        print(f"Error processing Field {field}, SPW {spw}: {str(e)}")
        return None

def find_dead_antennas(ms_path, output_file, n_processes=None):
    if n_processes is None:
        n_processes = max(1, 12)
    
    start_total = time.time()
    print("Reading MS metadata...")
    start = time.time()
    scans, fields, spws, ant_names, nchan, mock_ants = get_ms_info(ms_path)
    print_runtime(start, "Metadata read")
    
    tasks = [(ms_path, spw, field, ant_names, mock_ants) 
             for field in fields 
             for spw in spws]
    
    print(f"\nProcessing {len(tasks)} field/SPW combinations using {n_processes} processes...")
    with Pool(n_processes) as pool:
        all_results = pool.map(process_spw_field, tasks)
    
    # Aggregate results by antenna across all field/spw combinations
    antenna_scan_map = defaultdict(lambda: defaultdict(set))  # antenna -> field -> set of scans
    
    for result in all_results:
        if result and result['scan_bad_antennas']:
            field = result['field']
            for scan, bad_ants in result['scan_bad_antennas'].items():
                for ant_info in bad_ants:
                    ant_name = ant_info['name']
                    antenna_scan_map[ant_name][field].add(scan)
    
    # Write results to badants.txt
    with open(output_file, 'w') as f:
        for ant_name, field_scans in antenna_scan_map.items():
            for field, scan_set in field_scans.items():
                if scan_set:
                    scan_list = sorted(list(scan_set))
                    scan_str = ','.join(map(str, scan_list))
                    f.write(f"mode='manual' antenna='{ant_name}' field='{field}' scan='{scan_str}' reason='dead_antenna'\n")
        
        if antenna_scan_map:
            f.write("mode='summary'\n\n")
    
    print(f"\nBad antenna results written to: {output_file}")
    print_runtime(start_total)
    return all_results

import os
import subprocess
import time
import logging
from typing import Tuple, List, Optional

def create_batch_header(scheduler_type: str, job_name: str, nodes: int, ppn: int, 
                       walltime: str, output_dir: str, queue: str) -> str:
    """Create batch script header based on scheduler type"""
    if scheduler_type.upper() == "PBS":
        return f"""#!/bin/bash
#PBS -N {job_name}
#PBS -l nodes={nodes}:ppn={ppn}
#PBS -l walltime={walltime}
#PBS -j oe
#PBS -o {output_dir}
#PBS -q {queue}
"""
    else:  # SLURM
        return f"""#!/bin/bash
#SBATCH --job-name={job_name}
#SBATCH --nodes={nodes}
#SBATCH --ntasks-per-node={ppn}
#SBATCH --time={walltime}
#SBATCH --output={output_dir}
#SBATCH --partition={queue}
"""

def get_script_extension(scheduler_type: str) -> str:
    """Get appropriate file extension based on scheduler"""
    return ".pbs" if scheduler_type.upper() == "PBS" else ".slurm"

def submit_job(script_path: str, scheduler_type: str, logger: logging.Logger) -> Optional[str]:
    """Submit job and return job ID"""
    try:
        command = "qsub" if scheduler_type.upper() == "PBS" else "sbatch"
        result = subprocess.run([command, script_path], 
                             stdout=subprocess.PIPE, 
                             stderr=subprocess.PIPE,
                             check=True,
                             text=True)
        job_id = result.stdout.strip()
        logger.info(f"Job submitted successfully with ID: {job_id}")
        return job_id
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to submit job: {e}")
        return None

def check_job_status(job_id: str, scheduler_type: str) -> bool:
    """Check if job is still running"""
    try:
        if scheduler_type.upper() == "PBS":
            command = ["qstat", job_id]
        else:
            command = ["squeue", "-j", job_id]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0
    except subprocess.CalledProcessError:
        return False

def extract_log_file_path(script_file: str, scheduler_type: str) -> Optional[str]:
    """Extract log file path from batch script"""
    try:
        with open(script_file, 'r') as file:
            lines = file.readlines()
            for line in lines:
                if scheduler_type.upper() == "PBS" and line.startswith("#PBS -o"):
                    return line.split()[2].strip()
                elif scheduler_type.upper() == "SLURM" and line.startswith("#SBATCH --output="):
                    return line.split("=")[1].strip()
    except Exception as e:
        print(f"Error reading {script_file}: {e}")
    return None

# def wait_for_jobs_to_finish(job_info: List[Tuple[str, str]], base_output_dir: str, 
#                           logger: logging.Logger, prefix: str, scheduler_type: str) -> Tuple[bool, List[str]]:
#     """Wait for jobs to finish and check their status"""
#     all_successful = True
#     failed_jobs = []
#     script_ext = get_script_extension(scheduler_type)

#     while job_info:
#         time.sleep(10)  # Check every minute
#         for job_id, subband in job_info[:]:
#             if not check_job_status(job_id, scheduler_type):
#                 job_info.remove((job_id, subband))
                
#                 script_file = f"{prefix}_{subband}{script_ext}"
#                 log_file_path = extract_log_file_path(script_file, scheduler_type)
                
#                 if log_file_path:
#                     log_file = os.path.join(base_output_dir, subband, os.path.basename(log_file_path))
#                 else:
#                     log_file = os.path.join(base_output_dir, subband, f"{prefix}_{subband}.log")

#                 if os.path.exists(log_file):
#                     with open(log_file, 'r') as log:
#                         log_content = log.read()
#                         if "error" in log_content.lower():
#                             logger.error(f"Job {job_id} (subband {subband}) failed. Check log file {log_file}.")
#                             all_successful = False
#                             failed_jobs.append(subband)
#                         else:
#                             logger.info(f"Job {job_id} (subband {subband}) completed successfully.")
#                 else:
#                     logger.error(f"Log file not found: {log_file}")
#                     all_successful = False
#                     failed_jobs.append(subband)

#     return all_successful, failed_jobs

ERROR_WHITELIST = [
   "the only error message you will receive",
   "prterun has exited",
   "[TerminalIPythonApp] ERROR | Failed to create history session",
   "sqlite3.OperationalError: database is locked",
   "getcell::TIME   Exception Reported: TableProxy::getCell: no such row",
   "TableProxy::getCell: no such row",
   "Error '0:0' does not overlap",
   "warnings.warn(errors[info][0], RuntimeWarning)"
   "Leap second table TAI_UTC seems out-of-date",
   "Until the table is updated (see the CASA documentation or your system admin)",
   "times and coordinates derived from UTC could be wrong by 1s or more."
]




def check_error_whitelist(error_line, whitelist, match_threshold=3):
   for white_error in whitelist:
       white_words = set(white_error.lower().split())
       error_words = set(error_line.lower().split())
       matches = len(white_words.intersection(error_words))
       if matches >= match_threshold:
           return True
   return False

def wait_for_jobs_to_finish(job_info: List[Tuple[str, str]], base_output_dir: str, 
                         logger: logging.Logger, prefix: str, scheduler_type: str) -> Tuple[bool, List[str]]:
   all_successful = True
   failed_jobs = []
   script_ext = get_script_extension(scheduler_type)

   while job_info:
       time.sleep(10)
       for job_id, subband in job_info[:]:
           if not check_job_status(job_id, scheduler_type):
               job_info.remove((job_id, subband))
               
               script_file = f"{prefix}_{subband}{script_ext}"
               log_file_path = extract_log_file_path(script_file, scheduler_type)
               
               if log_file_path:
                   log_file = os.path.join(base_output_dir, subband, os.path.basename(log_file_path))
               else:
                   log_file = os.path.join(base_output_dir, subband, f"{prefix}_{subband}.log")

               if os.path.exists(log_file):
                   with open(log_file, 'r') as log:
                       log_content = log.read()
                       error_lines = [line for line in log_content.split('\n') if 'error' in line.lower()]
                       error_found = any(not check_error_whitelist(line, ERROR_WHITELIST) for line in error_lines)
                       
                       if error_found:
                           logger.error(f"Job {job_id} (subband {subband}) failed. Check log file {log_file}.")
                           logger.error("Error lines found:")
                           for line in error_lines:
                               if not check_error_whitelist(line, ERROR_WHITELIST):
                                   logger.error(line)
                           all_successful = False
                           failed_jobs.append(subband)
                       else:
                           logger.info(f"Job {job_id} (subband {subband}) completed successfully.")
               else:
                   logger.error(f"Log file not found: {log_file}")
                   all_successful = False
                   failed_jobs.append(subband)

   return all_successful, failed_jobs

def wait_for_field_jobs_to_finish(job_info, base_output_dir, logger, prefix, scheduler_type, field_list=None):
    """
    Wait for field-specific jobs to finish and check their log files.
    
    Parameters:
    - job_info: List of (job_id, spw, field) or (job_id, spw) tuples
    - base_output_dir: Base output directory
    - logger: Logger instance
    - prefix: Prefix for log files
    - scheduler_type: PBS or SLURM
    - field_list: List of fields being processed (used if job_info doesn't contain field)
    
    Returns:
    - all_successful: Boolean indicating if all jobs were successful
    - failed_jobs: List of items that failed
    """
    all_successful = True
    failed_jobs = []
    script_ext = get_script_extension(scheduler_type)
    
    # Make a copy to avoid modifying during iteration
    pending_jobs = job_info.copy()
    
    while pending_jobs:
        time.sleep(10)
        for job_tuple in pending_jobs[:]:
            # Handle both 3-tuple (job_id, spw, field) and 2-tuple (job_id, spw) formats
            if len(job_tuple) == 3:
                job_id, spw, field = job_tuple
            else:
                job_id, spw = job_tuple
                field = field_list  # Use the provided field parameter
            
            if not check_job_status(job_id, scheduler_type):
                pending_jobs.remove(job_tuple)
                
                # Field-specific log file path
                if field:
                    log_file = os.path.join(base_output_dir, spw, field, f"{prefix}.log")
                else:
                    # Legacy path without field
                    log_file = os.path.join(base_output_dir, spw, f"{prefix}.log")
                
                if os.path.exists(log_file):
                    with open(log_file, 'r') as log:
                        log_content = log.read()
                        error_lines = [line for line in log_content.split('\n') if 'error' in line.lower()]
                        error_found = any(not check_error_whitelist(line, ERROR_WHITELIST) for line in error_lines)
                        
                        if error_found:
                            failure_id = f"{spw}_{field}" if field else spw
                            logger.error(f"Job {job_id} (spw {spw}, field {field if field else 'none'}) failed. Check log file {log_file}.")
                            logger.error("Error lines found:")
                            for line in error_lines:
                                if not check_error_whitelist(line, ERROR_WHITELIST):
                                    logger.error(line)
                            all_successful = False
                            failed_jobs.append(failure_id)
                        else:
                            logger.info(f"Job {job_id} (spw {spw}, field {field if field else 'none'}) completed successfully.")
                else:
                    failure_id = f"{spw}_{field}" if field else spw
                    logger.error(f"Log file not found: {log_file}")
                    all_successful = False
                    failed_jobs.append(failure_id)
    
    return all_successful, failed_jobs

def wait_for_wsclean_jobs(job_info_list, base_output_dir, logger, prefix, scheduler):
    """Wait for multiple wsclean jobs to finish and check their status."""
    all_successful = True
    failed_jobs = []
    
    # Make a copy to avoid modifying the list during iteration
    pending_jobs = job_info_list.copy()
    
    while pending_jobs:
        time.sleep(10)
        for job_info in pending_jobs[:]:  # Use a slice to safely modify during iteration
            job_id, field = job_info
            
            if not check_job_status(job_id, scheduler):
                pending_jobs.remove(job_info)
                
                # Field-specific log file paths
                field_output_dir = f"images/{field}"
                field_imagename = f"{prefix}_{field}" if prefix else f"wsclean_{field}"
                log_file = os.path.join(base_output_dir, field_output_dir, f"{field_imagename}.log")
                
                try:
                    if os.path.exists(log_file):
                        with open(log_file, 'r') as log:
                            log_content = log.read()
                            logger.debug(f"Checking WSClean log for field {field}...")
                            
                            wsclean_errors = [
                                "Could not parse value",
                                "exception occurred",
                                "An exception occured",
                                "error",
                                "Segmentation fault",
                                "killed",
                                "core dumped",
                                "Exception"
                            ]
                            
                            error_lines = []
                            for line in log_content.split('\n'):
                                for error in wsclean_errors:
                                    if error.lower() in line.lower():
                                        logger.debug(f"Found potential error line: {line}")
                                        if not check_error_whitelist(line, ERROR_WHITELIST):
                                            error_lines.append(line)
                            
                            if error_lines:
                                logger.error(f"WSClean job {job_id} for field {field} failed. Errors found:")
                                for line in error_lines:
                                    logger.error(line.strip())
                                all_successful = False
                                failed_jobs.append(field)
                            else:
                                logger.info(f"WSClean job {job_id} for field {field} completed successfully.")
                    else:
                        logger.error(f"WSClean log file not found: {log_file}")
                        all_successful = False
                        failed_jobs.append(field)
                except Exception as e:
                    logger.error(f"Error checking WSClean log file for field {field}: {str(e)}")
                    logger.debug(f"Exception details:", exc_info=True)
                    all_successful = False
                    failed_jobs.append(field)
    
    return all_successful, failed_jobs


def wait_for_combine_ms_job(job_id: str, base_output_dir: str, logger: logging.Logger, scheduler: str) -> Tuple[bool, List[str]]:
    """Wait for combine_ms job to finish and check its status with better error handling."""
    all_successful = True
    failed_jobs = []
    
    while True:
        time.sleep(10)
        if not check_job_status(job_id, scheduler):
            log_file = os.path.join(base_output_dir, "combine_ms.log")
            try:
                if os.path.exists(log_file):
                    with open(log_file, 'r') as log:
                        log_content = log.read()
                        logger.debug(f"Full combine_ms log content:\n{log_content}")
                        
                        # CASA-specific error patterns
                        casa_errors = [
                            "An error occurred",
                            "exception occurred",
                            "error",
                            "Segmentation fault",
                            "killed",
                            "core dumped",
                            "Exception",
                            "SEVERE"
                        ]
                        
                        error_lines = []
                        for line in log_content.split('\n'):
                            for error in casa_errors:
                                if error.lower() in line.lower():
                                    logger.debug(f"Found potential error line: {line}")
                                    if not check_error_whitelist(line, ERROR_WHITELIST):
                                        error_lines.append(line)
                        
                        if error_lines:
                            logger.error(f"Combine_ms job {job_id} failed. Errors found:")
                            for line in error_lines:
                                logger.error(line.strip())
                            all_successful = False
                            failed_jobs.append("all")
                        else:
                            logger.info(f"Combine_ms job {job_id} completed successfully.")
                else:
                    logger.error(f"Combine_ms log file not found: {log_file}")
                    all_successful = False
                    failed_jobs.append("all")
            except Exception as e:
                logger.error(f"Error checking combine_ms log file: {str(e)}")
                logger.debug("Exception details:", exc_info=True)
                all_successful = False
                failed_jobs.append("all")
            break
            
    return all_successful, failed_jobs

def wait_for_pybdsf_jobs(job_info, base_output_dir, logger, mask_name, scheduler):
    """
    Wait for PyBDSF jobs to finish and check their log files.
    
    Parameters:
    -----------
    job_info : list
        List of (job_id, field) tuples
    base_output_dir : str
        Base output directory
    logger : logging.Logger
        Logger object
    mask_name : str
        Name of the mask being created
    scheduler : str
        Scheduler type ('PBS' or 'SLURM')
        
    Returns:
    --------
    tuple
        (successful_fields, failed_fields) lists
    """
    all_successful = True
    failed_jobs = []
    
    # Make a copy to avoid modifying the list during iteration
    pending_jobs = job_info.copy()
    
    while pending_jobs:
        time.sleep(10)
        for job_info in pending_jobs[:]:  # Use a slice to safely modify during iteration
            job_id, field = job_info
            
            if not check_job_status(job_id, scheduler):
                pending_jobs.remove(job_info)
                
                # Field-specific log file paths
                field_output_dir = f"images/{field}"
                log_file = os.path.join(base_output_dir, field_output_dir, f"pybdsf_{mask_name}.log")
                
                try:
                    if os.path.exists(log_file):
                        with open(log_file, 'r') as log:
                            log_content = log.read()
                            logger.debug(f"Checking PyBDSF log for field {field}...")
                            
                            error_patterns = [
                                "exception occurred",
                                "error",
                                "Segmentation fault",
                                "killed",
                                "core dumped",
                                "Exception"
                            ]
                            
                            error_lines = []
                            for line in log_content.split('\n'):
                                for error in error_patterns:
                                    if error.lower() in line.lower():
                                        logger.debug(f"Found potential error line: {line}")
                                        if not check_error_whitelist(line, ERROR_WHITELIST):
                                            error_lines.append(line)
                            
                            # Check for successful completion marker
                            success_marker = f"Created WSClean mask: {field_output_dir}/masks/{mask_name}.fits"
                            if success_marker in log_content and not error_lines:
                                logger.info(f"PyBDSF job {job_id} for field {field} completed successfully.")
                            else:
                                if error_lines:
                                    logger.error(f"PyBDSF job {job_id} for field {field} failed. Errors found:")
                                    for line in error_lines:
                                        logger.error(line.strip())
                                else:
                                    logger.error(f"PyBDSF job {job_id} for field {field} failed. No completion marker found.")
                                all_successful = False
                                failed_jobs.append(field)
                    else:
                        logger.error(f"PyBDSF log file not found: {log_file}")
                        all_successful = False
                        failed_jobs.append(field)
                except Exception as e:
                    logger.error(f"Error checking PyBDSF log file for field {field}: {str(e)}")
                    logger.debug(f"Exception details:", exc_info=True)
                    all_successful = False
                    failed_jobs.append(field)
    
    return all_successful, failed_jobs

def wait_for_ddcal_jobs(job_info_list, base_output_dir, logger, prefix, scheduler):
    """
    Wait for DD calibration jobs to finish and check their log files.
    
    Parameters:
    - job_info_list: List of (job_id, field) tuples
    - base_output_dir: Base output directory
    - logger: Logger instance
    - prefix: Prefix for log files
    - scheduler: PBS or SLURM
    
    Returns:
    - all_successful: Boolean indicating if all jobs were successful
    - failed_jobs: List of fields that failed
    """
    all_successful = True
    failed_jobs = []
    
    # Make a copy to avoid modifying the list during iteration
    pending_jobs = job_info_list.copy()
    
    while pending_jobs:
        time.sleep(10)
        for job_info in pending_jobs[:]:
            job_id, field = job_info
            
            if not check_job_status(job_id, scheduler):
                pending_jobs.remove(job_info)
                
                # DD calibration specific log file path
                log_file = os.path.join(base_output_dir, f"ddcal_output/{field}/logs/{prefix}_{field}.log")
                
                if os.path.exists(log_file):
                    with open(log_file, 'r') as log:
                        log_content = log.read()
                        error_lines = [line for line in log_content.split('\n') if 'error' in line.lower()]
                        error_found = any(not check_error_whitelist(line, ERROR_WHITELIST) for line in error_lines)
                        
                        if error_found:
                            logger.error(f"{prefix} job {job_id} for field {field} failed. Check log file {log_file}.")
                            logger.error("Error lines found:")
                            for line in error_lines:
                                if not check_error_whitelist(line, ERROR_WHITELIST):
                                    logger.error(line)
                            all_successful = False
                            failed_jobs.append(field)
                        else:
                            logger.info(f"{prefix} job {job_id} for field {field} completed successfully.")
                else:
                    logger.error(f"{prefix} log file not found: {log_file}")
                    all_successful = False
                    failed_jobs.append(field)
    
    return all_successful, failed_jobs


def wait_for_ddcal_wsclean_jobs(job_info_list, base_output_dir, logger, prefix, scheduler):
    """
    Wait for DD calibration WSClean jobs to finish and check their log files.
    
    Parameters:
    - job_info_list: List of (job_id, field) tuples
    - base_output_dir: Base output directory
    - logger: Logger instance
    - prefix: Prefix for log files
    - scheduler: PBS or SLURM
    
    Returns:
    - all_successful: Boolean indicating if all jobs were successful
    - failed_jobs: List of fields that failed
    """
    all_successful = True
    failed_jobs = []
    
    # Make a copy to avoid modifying the list during iteration
    pending_jobs = job_info_list.copy()
    
    while pending_jobs:
        time.sleep(10)
        for job_info in pending_jobs[:]:
            job_id, field = job_info
            
            if not check_job_status(job_id, scheduler):
                pending_jobs.remove(job_info)
                
                # DD calibration WSClean specific log file path
                log_file = os.path.join(base_output_dir, f"ddcal_output/{field}/logs/wsclean_{prefix}_{field}.log")
                
                try:
                    if os.path.exists(log_file):
                        with open(log_file, 'r') as log:
                            log_content = log.read()
                            logger.debug(f"Checking WSClean log for field {field}...")
                            
                            wsclean_errors = [
                                "Could not parse value",
                                "exception occurred",
                                "An exception occured",
                                "error",
                                "Segmentation fault",
                                "killed",
                                "core dumped",
                                "Exception"
                            ]
                            
                            error_lines = []
                            for line in log_content.split('\n'):
                                for error in wsclean_errors:
                                    if error.lower() in line.lower():
                                        logger.debug(f"Found potential error line: {line}")
                                        if not check_error_whitelist(line, ERROR_WHITELIST):
                                            error_lines.append(line)
                            
                            if error_lines:
                                logger.error(f"WSClean job {job_id} for field {field} failed. Errors found:")
                                for line in error_lines:
                                    logger.error(line.strip())
                                all_successful = False
                                failed_jobs.append(field)
                            else:
                                logger.info(f"WSClean job {job_id} for field {field} completed successfully.")
                    else:
                        logger.error(f"WSClean log file not found: {log_file}")
                        all_successful = False
                        failed_jobs.append(field)
                except Exception as e:
                    logger.error(f"Error checking WSClean log file for field {field}: {str(e)}")
                    logger.debug(f"Exception details:", exc_info=True)
                    all_successful = False
                    failed_jobs.append(field)
    
    return all_successful, failed_jobs




def cleanup_files(subband: str, prefix: str, scheduler_type: str, logger: logging.Logger):
    """Clean up job files"""
    try:
        python_file = f"{prefix}_{subband}.py"
        batch_file = f"{prefix}_{subband}{get_script_extension(scheduler_type)}"
        
        for file_path in [python_file, batch_file]:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Deleted {file_path}")
            else:
                logger.warning(f"{file_path} not found for deletion.")
    except Exception as e:
        logger.error(f"Error during cleanup for {subband}: {e}")

def check_jobs_status(job_list: List[Tuple[str, str]], working_directory: str, 
                     logger: logging.Logger, prefix: str, scheduler_type: str) -> bool:
    """Check status of multiple jobs"""
    if not job_list:
        logger.info("No jobs to check.")
        return True
    
    all_successful, failed_jobs = wait_for_jobs_to_finish(
        job_list, working_directory, logger, prefix, scheduler_type
    )
    
    if all_successful:
        logger.info("All jobs completed successfully.")
        for _, spw in job_list:
            cleanup_files(spw, prefix, scheduler_type, logger)
    else:
        logger.error(f"Failed subbands: {', '.join(failed_jobs)}")
    
    return all_successful

def kill_jobs(job_ids, scheduler_type, logger):
    """Kill jobs in queue system"""
    command = "qdel" if scheduler_type.upper() == "PBS" else "scancel"
    for job_id in job_ids:
        try:
            subprocess.run([command, job_id], check=True)
            logger.info(f"Killed job {job_id}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to kill job {job_id}: {e}")

def cleanup_and_exit(job_ids, scheduler_type, logger, exit_code=1):
    """Kill all jobs and exit"""
    logger.info("Cleaning up jobs before exit...")
    kill_jobs(job_ids, scheduler_type, logger)
    sys.exit(exit_code)



def remove_lock(ms):
    os.remove('{}/table.lock'.format(ms))


