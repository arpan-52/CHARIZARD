# charizard/utils/flagging_utils/antenna_analysis.py
"""
Antenna analysis utilities.
- Find dead/bad antennas
- Find best reference antenna
"""

import os
import time
import json
import numpy as np
from typing import Dict, List, Tuple, Optional
from collections import defaultdict
from multiprocessing import Pool

from casacore import tables


def remove_table_lock(ms_path: str):
    """Remove table lock file if exists."""
    lock_file = os.path.join(ms_path, 'table.lock')
    if os.path.exists(lock_file):
        try:
            os.remove(lock_file)
        except:
            pass


def get_ms_info_for_antenna(ms_path: str):
    """Get basic MS info for antenna analysis."""
    try:
        with tables.table(ms_path, ack=False) as tb:
            scans = np.unique(tb.getcol('SCAN_NUMBER'))
            fields = np.unique(tb.getcol('FIELD_ID'))
            spws = np.unique(tb.getcol('DATA_DESC_ID'))
            ant1 = tb.getcol('ANTENNA1', 0, min(10000, tb.nrows()))
            ant2 = tb.getcol('ANTENNA2', 0, min(10000, tb.nrows()))
            active_ants = np.unique(np.concatenate([ant1, ant2]))
        
        with tables.table(ms_path + '/ANTENNA', ack=False) as tb:
            ant_names = list(tb.getcol('NAME'))
            mock_mask = np.ones(len(ant_names), dtype=bool)
            mock_mask[active_ants] = False
            mock_ants = np.where(mock_mask)[0]
        
        with tables.table(ms_path + '/SPECTRAL_WINDOW', ack=False) as tb:
            nchan = tb.getcol('NUM_CHAN')
        
        return scans, fields, spws, ant_names, nchan, mock_ants
    finally:
        remove_table_lock(ms_path)


def process_spw_field(params):
    """Process one SPW/field combination for bad antenna detection."""
    ms_path, spw, field, ant_names, mock_ants = params
    
    try:
        with tables.table(ms_path, ack=False) as tb:
            sel = tb.query(f"DATA_DESC_ID = {spw} AND FIELD_ID = {field}", sortlist='SCAN_NUMBER')
            
            if sel.nrows() == 0:
                sel.close()
                return None
            
            scans = sel.getcol('SCAN_NUMBER')
            ant1 = sel.getcol('ANTENNA1')
            ant2 = sel.getcol('ANTENNA2')
            data = sel.getcol('DATA')
            flags = sel.getcol('FLAG')
            sel.close()
            
            unique_scans = np.unique(scans)
            num_ants = len(ant_names)
            npols = data.shape[-1]
            
            scan_bad_antennas = {}
            
            # First pass: calculate global reference amplitude
            global_ant_stats = defaultdict(lambda: {'valid_amplitudes': [], 'is_active': False})
            
            for i in range(len(data)):
                a1, a2 = ant1[i], ant2[i]
                
                if npols == 4:
                    pol_flags = flags[i, :, 0] | flags[i, :, 3]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 3])) / 2
                    else:
                        continue
                elif npols == 2:
                    pol_flags = flags[i, :, 0] | flags[i, :, 1]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
                    else:
                        continue
                else:
                    valid_mask = ~flags[i, :, 0]
                    if np.any(valid_mask):
                        amp = np.abs(data[i, valid_mask, 0])
                    else:
                        continue
                
                for ant in [a1, a2]:
                    global_ant_stats[ant]['is_active'] = True
                    global_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
            
            # Calculate global reference
            good_amps = []
            for ant in range(num_ants):
                stats = global_ant_stats[ant]
                if stats['is_active'] and len(stats['valid_amplitudes']) > 0:
                    med_amp = np.median(stats['valid_amplitudes'])
                    if med_amp > 0:
                        good_amps.append(med_amp)
            
            if not good_amps:
                return None
            
            reference_amp = np.percentile(good_amps, 75)
            threshold = reference_amp * 0.05
            
            # Second pass: per-scan analysis
            for scan in unique_scans:
                scan_mask = scans == scan
                scan_indices = np.where(scan_mask)[0]
                
                if len(scan_indices) == 0:
                    continue
                
                scan_ant_stats = defaultdict(lambda: {
                    'total_samples': 0,
                    'flagged_samples': 0,
                    'valid_amplitudes': [],
                    'is_active': False
                })
                
                for idx in scan_indices:
                    a1, a2 = ant1[idx], ant2[idx]
                    
                    if npols == 4:
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 3]
                        valid_mask = ~pol_flags
                        if np.any(valid_mask):
                            amp = (np.abs(data[idx, valid_mask, 0]) + np.abs(data[idx, valid_mask, 3])) / 2
                        else:
                            amp = np.array([])
                    elif npols == 2:
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 1]
                        valid_mask = ~pol_flags
                        if np.any(valid_mask):
                            amp = (np.abs(data[idx, valid_mask, 0]) + np.abs(data[idx, valid_mask, 1])) / 2
                        else:
                            amp = np.array([])
                    else:
                        valid_mask = ~flags[idx, :, 0]
                        if np.any(valid_mask):
                            amp = np.abs(data[idx, valid_mask, 0])
                        else:
                            amp = np.array([])
                    
                    for ant in [a1, a2]:
                        scan_ant_stats[ant]['is_active'] = True
                        scan_ant_stats[ant]['total_samples'] += len(valid_mask)
                        scan_ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                        if len(amp) > 0:
                            scan_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
                
                scan_bad_ants = []
                for ant in range(num_ants):
                    stats = scan_ant_stats[ant]
                    if stats['is_active'] and len(stats['valid_amplitudes']) > 0 and ant not in mock_ants:
                        flag_percent = (stats['flagged_samples'] / stats['total_samples'] * 100 
                                       if stats['total_samples'] > 0 else 100)
                        
                        if flag_percent > 90:
                            continue
                        
                        med_amp = np.median(stats['valid_amplitudes'])
                        if med_amp < threshold:
                            scan_bad_ants.append({
                                'name': ant_names[ant],
                                'median_amplitude': float(med_amp),
                                'ratio_to_reference': float(med_amp / reference_amp),
                                'flag_percentage': float(flag_percent)
                            })
                
                if scan_bad_ants:
                    scan_bad_antennas[int(scan)] = scan_bad_ants
            
            return {
                'field': int(field),
                'spw': int(spw),
                'mock_antennas': [ant_names[i] for i in mock_ants],
                'scan_bad_antennas': scan_bad_antennas,
                'reference_amplitude': float(reference_amp)
            }
            
    except Exception as e:
        print(f"Error processing Field {field}, SPW {spw}: {str(e)}")
        return None
    finally:
        remove_table_lock(ms_path)


def find_dead_antennas(ms_path: str, output_file: str, n_processes: int = None, 
                       user_bad_ants: List[str] = None):
    """
    Find dead/bad antennas in MS and write to badants.txt.
    
    Args:
        ms_path: Path to measurement set
        output_file: Path to output file (badants.txt)
        n_processes: Number of parallel processes
        user_bad_ants: List of user-specified bad antennas
    """
    if n_processes is None:
        n_processes = max(1, 8)
    
    if user_bad_ants is None:
        user_bad_ants = []
    
    print(f"Analyzing {ms_path} for dead antennas...")
    scans, fields, spws, ant_names, nchan, mock_ants = get_ms_info_for_antenna(ms_path)
    
    tasks = [(ms_path, spw, field, ant_names, mock_ants) 
             for field in fields 
             for spw in spws]
    
    print(f"Processing {len(tasks)} field/SPW combinations using {n_processes} processes...")
    with Pool(n_processes) as pool:
        all_results = pool.map(process_spw_field, tasks)
    
    # Aggregate results
    antenna_scan_map = defaultdict(lambda: defaultdict(set))
    
    for result in all_results:
        if result and result['scan_bad_antennas']:
            field = result['field']
            for scan, bad_ants in result['scan_bad_antennas'].items():
                for ant_info in bad_ants:
                    ant_name = ant_info['name']
                    antenna_scan_map[ant_name][field].add(scan)
    
    # Write results
    with open(output_file, 'w') as f:
        # Dead antennas
        for ant_name, field_scans in antenna_scan_map.items():
            for field, scan_set in field_scans.items():
                if scan_set:
                    scan_list = sorted(list(scan_set))
                    scan_str = ','.join(map(str, scan_list))
                    f.write(f"mode='manual' antenna='{ant_name}' field='{field}' scan='{scan_str}' reason='dead_antenna'\n")
        
        # User bad antennas
        if user_bad_ants:
            for ant in user_bad_ants:
                f.write(f"mode='manual' antenna='{ant}' reason='user_specified'\n")
        
        # Standard flags
        f.write("# Standard flags\n")
        f.write("mode='manual' autocorr=True reason='autocorr'\n")
        f.write("mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'\n")
        f.write("mode='quack' quackinterval=10 quackmode='beg' quackincrement=False reason='quackbeg'\n")
        f.write("mode='quack' quackinterval=10 quackmode='endb' quackincrement=False reason='quackend'\n")
    
    print(f"Bad antenna results written to: {output_file}")
    
    # Clean up lock
    remove_table_lock(ms_path)
    
    return len(antenna_scan_map)


def find_best_refant(ms_path: str, n_processes: int = None) -> Tuple[str, Dict]:
    """
    Find the best reference antenna based on SNR metric.
    
    Best refant = highest (median_amp / std_amp) * (1 - flag_fraction)
    
    Should be called AFTER flagging for best results.
    
    Args:
        ms_path: Path to measurement set
        n_processes: Number of parallel processes
    
    Returns:
        (best_refant_name, antenna_stats_dict)
    """
    if n_processes is None:
        n_processes = max(1, 8)
    
    print(f"Finding best reference antenna in {ms_path}...")
    
    try:
        with tables.table(ms_path + '/ANTENNA', ack=False) as tb:
            ant_names = list(tb.getcol('NAME'))
        
        with tables.table(ms_path, ack=False) as tb:
            # Sample subset for speed
            nrows = min(tb.nrows(), 100000)
            ant1 = tb.getcol('ANTENNA1', 0, nrows)
            ant2 = tb.getcol('ANTENNA2', 0, nrows)
            data = tb.getcol('DATA', 0, nrows)
            flags = tb.getcol('FLAG', 0, nrows)
        
        npols = data.shape[-1]
        
        # Collect antenna statistics
        ant_stats = defaultdict(lambda: {
            'valid_amplitudes': [],
            'total_samples': 0,
            'flagged_samples': 0
        })
        
        for i in range(len(data)):
            a1, a2 = ant1[i], ant2[i]
            
            if npols == 4:
                pol_flags = flags[i, :, 0] | flags[i, :, 3]
                valid_mask = ~pol_flags
                if np.any(valid_mask):
                    amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 3])) / 2
                else:
                    amp = np.array([])
            elif npols == 2:
                pol_flags = flags[i, :, 0] | flags[i, :, 1]
                valid_mask = ~pol_flags
                if np.any(valid_mask):
                    amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
                else:
                    amp = np.array([])
            else:
                valid_mask = ~flags[i, :, 0]
                if np.any(valid_mask):
                    amp = np.abs(data[i, valid_mask, 0])
                else:
                    amp = np.array([])
            
            for ant in [a1, a2]:
                ant_stats[ant]['total_samples'] += len(valid_mask)
                ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                if len(amp) > 0:
                    ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
        
        # Calculate SNR metric for each antenna
        refant_scores = {}
        for ant_idx, stats in ant_stats.items():
            if len(stats['valid_amplitudes']) < 100:
                continue
            
            amps = np.array(stats['valid_amplitudes'])
            median_amp = np.median(amps)
            std_amp = np.std(amps)
            
            if std_amp == 0 or median_amp == 0:
                continue
            
            flag_fraction = stats['flagged_samples'] / stats['total_samples'] if stats['total_samples'] > 0 else 1.0
            
            # SNR metric: (median/std) * (1 - flag_fraction)
            snr_metric = (median_amp / std_amp) * (1 - flag_fraction)
            
            refant_scores[ant_names[ant_idx]] = {
                'snr_metric': float(snr_metric),
                'median_amp': float(median_amp),
                'std_amp': float(std_amp),
                'flag_fraction': float(flag_fraction)
            }
        
        if not refant_scores:
            print("WARNING: Could not determine best refant, using first antenna")
            return ant_names[0], {}
        
        best_refant = max(refant_scores, key=lambda x: refant_scores[x]['snr_metric'])
        print(f"Best reference antenna: {best_refant} (SNR metric: {refant_scores[best_refant]['snr_metric']:.3f})")
        
        # Save to file
        refant_file = os.path.join(os.path.dirname(ms_path), 'refant.json')
        with open(refant_file, 'w') as f:
            json.dump({'best_refant': best_refant, 'scores': refant_scores}, f, indent=2)
        
        return best_refant, refant_scores
        
    finally:
        remove_table_lock(ms_path)


# =============================================================================
# PIPELINE STEP FUNCTIONS - called from charizard.py
# =============================================================================

def run_bad_antenna_detection(hk, config, active_spws: List[str], logger, 
                               whitelist: List[str]) -> Optional[List[str]]:
    """
    Run bad antenna detection step.
    
    Generates SHORT scripts that import from charizard.
    Does NOT find refant here.
    """
    
    env = config.environment
    flow = config.flow
    init_cal = flow.get('initial_calibration_flagging', {})
    flagging = init_cal.get('flagging', {})
    bad_ant_config = flagging.get('bad_antennas', {})
    
    user_bad_ants = bad_ant_config.get('list', [])
    if isinstance(user_bad_ants, str):
        user_bad_ants = [a.strip() for a in user_bad_ants.split(',') if a.strip()]
    
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('default', {})
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # SHORT script - just imports and calls
        script = f'''#!/usr/bin/env python3
# Bad antenna detection for {spw}
from charizard.utils.flagging_utils.antenna_analysis import find_dead_antennas

find_dead_antennas(
    ms_path='{spw}/cal.ms',
    output_file='{spw}/badants.txt',
    n_processes={ppn},
    user_bad_ants={repr(user_bad_ants)}
)
print("Done!")
'''
        
        script_file = f"badant_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} python3 {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"badant_{spw}",
            job_subdir=spw,
            ppn=ppn,
            walltime=resources.get('walltime', '02:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted bad antenna detection for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No bad antenna jobs submitted")
        return None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} bad antenna jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Process results
    successful = []
    failed = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if not log_result.success:
            failed.append(spw)
            logger.error(f"{spw}: FAILED")
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            continue
        
        # Check output exists
        if os.path.exists(f"{spw}/badants.txt"):
            successful.append(spw)
            logger.info(f"{spw}: OK")
        else:
            failed.append(spw)
            logger.error(f"{spw}: Missing badants.txt")
    
    if not successful:
        return None
    
    return successful


def run_find_refant(hk, config, active_spws: List[str], logger,
                    whitelist: List[str]) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Find best reference antenna.
    
    Should be called AFTER initial flagging and catboss.
    """
    
    env = config.environment
    preamble = env.get('shell_preamble', '')
    resources = config.resources.get('default', {})
    ppn = resources.get('ppn', 4)
    
    job_ids = []
    job_map = {}
    
    for spw in active_spws:
        # SHORT script
        script = f'''#!/usr/bin/env python3
# Find best refant for {spw}
from charizard.utils.flagging_utils.antenna_analysis import find_best_refant

refant, scores = find_best_refant(
    ms_path='{spw}/cal.ms',
    n_processes={ppn}
)
print(f"Best refant: {{refant}}")
'''
        
        script_file = f"refant_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        udocker = build_udocker_prefix(config)
        command = f"""cd {os.getcwd()}
{preamble}
{udocker} python3 {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"refant_{spw}",
            job_subdir=spw,
            ppn=ppn,
            walltime="01:00:00"
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted refant finding for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No refant jobs submitted")
        return None, None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} refant jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Process results
    successful = []
    refants = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        if not log_result.success:
            logger.warning(f"{spw}: Refant finding failed")
            if log_result.error_lines:
                for err in log_result.error_lines[:3]:
                    logger.error(f"  >> {err}")
            continue
        
        # Read refant
        refant_file = f"{spw}/refant.json"
        if os.path.exists(refant_file):
            with open(refant_file, 'r') as f:
                data = json.load(f)
                refants.append(data['best_refant'])
                successful.append(spw)
                logger.info(f"{spw}: OK (refant: {data['best_refant']})")
        else:
            logger.warning(f"{spw}: Missing refant.json")
    
    if not refants:
        return successful if successful else None, None
    
    # Pick most common refant
    from collections import Counter
    best_refant = Counter(refants).most_common(1)[0][0]
    logger.info(f"Selected refant: {best_refant}")
    
    return successful if successful else active_spws, best_refant
