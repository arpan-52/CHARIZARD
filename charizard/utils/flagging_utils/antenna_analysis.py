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


def get_ms_info_for_antenna(ms_path: str):
    """Get basic MS info for antenna analysis."""
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


def process_spw_field(params):
    """Process one SPW/field combination for bad antenna detection."""
    ms_path, spw, field, ant_names, mock_ants = params
    
    try:
        with tables.table(ms_path, ack=False) as tb:
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
                elif npols == 2:
                    pol_flags = flags[i, :, 0] | flags[i, :, 1]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
                else:
                    valid_mask = ~flags[i, :, 0]
                    if np.any(valid_mask):
                        amp = np.abs(data[i, valid_mask, 0])
                
                for ant in [a1, a2]:
                    global_ant_stats[ant]['is_active'] = True
                    if np.any(valid_mask):
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
                sel.close()
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
                    elif npols == 2:
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 1]
                        valid_mask = ~pol_flags
                        if np.any(valid_mask):
                            amp = (np.abs(data[idx, valid_mask, 0]) + np.abs(data[idx, valid_mask, 1])) / 2
                    else:
                        valid_mask = ~flags[idx, :, 0]
                        if np.any(valid_mask):
                            amp = np.abs(data[idx, valid_mask, 0])
                    
                    for ant in [a1, a2]:
                        scan_ant_stats[ant]['is_active'] = True
                        scan_ant_stats[ant]['total_samples'] += len(valid_mask)
                        scan_ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                        if np.any(valid_mask):
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
            
            sel.close()
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


def find_dead_antennas(ms_path: str, output_file: str, n_processes: int = None):
    """Find dead/bad antennas and write to file."""
    if n_processes is None:
        n_processes = max(1, 12)
    
    print("Reading MS metadata...")
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
        for ant_name, field_scans in antenna_scan_map.items():
            for field, scan_set in field_scans.items():
                if scan_set:
                    scan_list = sorted(list(scan_set))
                    scan_str = ','.join(map(str, scan_list))
                    f.write(f"mode='manual' antenna='{ant_name}' field='{field}' scan='{scan_str}' reason='dead_antenna'\n")
        
        if antenna_scan_map:
            f.write("mode='summary'\n\n")
    
    print(f"Bad antenna results written to: {output_file}")
    return all_results


def find_best_refant(ms_path: str, n_processes: int = None) -> Tuple[str, Dict]:
    """Find the best reference antenna based on SNR metric."""
    if n_processes is None:
        n_processes = max(1, 8)
    
    print("Finding best reference antenna...")
    scans, fields, spws, ant_names, nchan, mock_ants = get_ms_info_for_antenna(ms_path)
    
    ant_stats = defaultdict(lambda: {
        'valid_amplitudes': [],
        'total_samples': 0,
        'flagged_samples': 0
    })
    
    with tables.table(ms_path, ack=False) as tb:
        nrows = min(tb.nrows(), 100000)
        ant1 = tb.getcol('ANTENNA1', 0, nrows)
        ant2 = tb.getcol('ANTENNA2', 0, nrows)
        data = tb.getcol('DATA', 0, nrows)
        flags = tb.getcol('FLAG', 0, nrows)
        
        npols = data.shape[-1]
        
        for i in range(len(data)):
            a1, a2 = ant1[i], ant2[i]
            
            if npols == 4:
                pol_flags = flags[i, :, 0] | flags[i, :, 3]
                valid_mask = ~pol_flags
                if np.any(valid_mask):
                    amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 3])) / 2
            elif npols == 2:
                pol_flags = flags[i, :, 0] | flags[i, :, 1]
                valid_mask = ~pol_flags
                if np.any(valid_mask):
                    amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
            else:
                valid_mask = ~flags[i, :, 0]
                if np.any(valid_mask):
                    amp = np.abs(data[i, valid_mask, 0])
            
            for ant in [a1, a2]:
                ant_stats[ant]['total_samples'] += len(valid_mask)
                ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                if np.any(valid_mask):
                    ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
    
    refant_scores = {}
    for ant_idx, stats in ant_stats.items():
        if ant_idx in mock_ants:
            continue
        
        if len(stats['valid_amplitudes']) < 100:
            continue
        
        amps = np.array(stats['valid_amplitudes'])
        median_amp = np.median(amps)
        std_amp = np.std(amps)
        
        if std_amp == 0:
            continue
        
        flag_fraction = stats['flagged_samples'] / stats['total_samples'] if stats['total_samples'] > 0 else 1.0
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
    
    return best_refant, refant_scores


def run_antenna_analysis(hk, config, active_spws: List[str], ms_info: Dict, 
                         logger, whitelist: List[str]) -> Tuple[Optional[List[str]], Optional[str]]:
    """
    Run antenna analysis step.
    
    Creates self-contained Python scripts that don't depend on charizard imports.
    """
    from .flag_commands import write_flag_commands
    
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
        # Self-contained script - no charizard imports!
        script = f'''#!/usr/bin/env python3
# Antenna analysis for {spw}
# Self-contained - no external imports

import os
import json
import numpy as np
from collections import defaultdict
from multiprocessing import Pool
from casacore import tables

ms_path = '{spw}/cal.ms'
badants_file = '{spw}/badants.txt'
refant_file = '{spw}/refant.json'
n_processes = {ppn}
user_bad_ants = {repr(user_bad_ants)}

print(f"Analyzing {{ms_path}}...")

# ============================================================================
# GET MS INFO
# ============================================================================
def get_ms_info_local(ms_path):
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

# ============================================================================
# PROCESS SPW/FIELD
# ============================================================================
def process_spw_field(params):
    ms_path, spw, field, ant_names, mock_ants = params
    try:
        with tables.table(ms_path, ack=False) as tb:
            sel = tb.query(f"DATA_DESC_ID = {{spw}} AND FIELD_ID = {{field}}", sortlist='SCAN_NUMBER')
            if sel.nrows() == 0:
                return None
            
            scans = sel.getcol('SCAN_NUMBER')
            ant1 = sel.getcol('ANTENNA1')
            ant2 = sel.getcol('ANTENNA2')
            data = sel.getcol('DATA')
            flags = sel.getcol('FLAG')
            
            unique_scans = np.unique(scans)
            num_ants = len(ant_names)
            npols = data.shape[-1]
            scan_bad_antennas = {{}}
            
            global_ant_stats = defaultdict(lambda: {{'valid_amplitudes': [], 'is_active': False}})
            
            for i in range(len(data)):
                a1, a2 = ant1[i], ant2[i]
                if npols == 4:
                    pol_flags = flags[i, :, 0] | flags[i, :, 3]
                elif npols == 2:
                    pol_flags = flags[i, :, 0] | flags[i, :, 1]
                else:
                    pol_flags = flags[i, :, 0]
                valid_mask = ~pol_flags
                if np.any(valid_mask):
                    if npols == 4:
                        amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 3])) / 2
                    elif npols == 2:
                        amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
                    else:
                        amp = np.abs(data[i, valid_mask, 0])
                    for ant in [a1, a2]:
                        global_ant_stats[ant]['is_active'] = True
                        global_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
            
            good_amps = []
            for ant in range(num_ants):
                stats = global_ant_stats[ant]
                if stats['is_active'] and len(stats['valid_amplitudes']) > 0:
                    med_amp = np.median(stats['valid_amplitudes'])
                    if med_amp > 0:
                        good_amps.append(med_amp)
            
            if not good_amps:
                sel.close()
                return None
            
            reference_amp = np.percentile(good_amps, 75)
            threshold = reference_amp * 0.05
            
            for scan in unique_scans:
                scan_mask = scans == scan
                scan_indices = np.where(scan_mask)[0]
                if len(scan_indices) == 0:
                    continue
                
                scan_ant_stats = defaultdict(lambda: {{
                    'total_samples': 0, 'flagged_samples': 0,
                    'valid_amplitudes': [], 'is_active': False
                }})
                
                for idx in scan_indices:
                    a1, a2 = ant1[idx], ant2[idx]
                    if npols == 4:
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 3]
                    elif npols == 2:
                        pol_flags = flags[idx, :, 0] | flags[idx, :, 1]
                    else:
                        pol_flags = flags[idx, :, 0]
                    valid_mask = ~pol_flags
                    if np.any(valid_mask):
                        if npols == 4:
                            amp = (np.abs(data[idx, valid_mask, 0]) + np.abs(data[idx, valid_mask, 3])) / 2
                        elif npols == 2:
                            amp = (np.abs(data[idx, valid_mask, 0]) + np.abs(data[idx, valid_mask, 1])) / 2
                        else:
                            amp = np.abs(data[idx, valid_mask, 0])
                        for ant in [a1, a2]:
                            scan_ant_stats[ant]['is_active'] = True
                            scan_ant_stats[ant]['total_samples'] += len(valid_mask)
                            scan_ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                            scan_ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())
                
                scan_bad_ants = []
                for ant in range(num_ants):
                    stats = scan_ant_stats[ant]
                    if stats['is_active'] and len(stats['valid_amplitudes']) > 0 and ant not in mock_ants:
                        flag_percent = stats['flagged_samples'] / stats['total_samples'] * 100 if stats['total_samples'] > 0 else 100
                        if flag_percent > 90:
                            continue
                        med_amp = np.median(stats['valid_amplitudes'])
                        if med_amp < threshold:
                            scan_bad_ants.append({{'name': ant_names[ant], 'median_amplitude': float(med_amp)}})
                
                if scan_bad_ants:
                    scan_bad_antennas[int(scan)] = scan_bad_ants
            
            sel.close()
            return {{'field': int(field), 'spw': int(spw), 'scan_bad_antennas': scan_bad_antennas}}
    except Exception as e:
        print(f"Error: {{e}}")
        return None

# ============================================================================
# MAIN
# ============================================================================
print("Reading MS metadata...")
scans, fields, spws, ant_names, nchan, mock_ants = get_ms_info_local(ms_path)
print(f"Found {{len(ant_names)}} antennas, {{len(fields)}} fields, {{len(spws)}} SPWs")

tasks = [(ms_path, spw, field, ant_names, mock_ants) for field in fields for spw in spws]
print(f"Processing {{len(tasks)}} combinations with {{n_processes}} processes...")

with Pool(n_processes) as pool:
    all_results = pool.map(process_spw_field, tasks)

# Aggregate bad antennas
antenna_scan_map = defaultdict(lambda: defaultdict(set))
for result in all_results:
    if result and result['scan_bad_antennas']:
        field = result['field']
        for scan, bad_ants in result['scan_bad_antennas'].items():
            for ant_info in bad_ants:
                antenna_scan_map[ant_info['name']][field].add(scan)

# Write badants.txt
with open(badants_file, 'w') as f:
    for ant_name, field_scans in antenna_scan_map.items():
        for field, scan_set in field_scans.items():
            if scan_set:
                scan_str = ','.join(map(str, sorted(list(scan_set))))
                f.write(f"mode='manual' antenna='{{ant_name}}' field='{{field}}' scan='{{scan_str}}' reason='dead_antenna'\\n")
    if antenna_scan_map:
        f.write("mode='summary'\\n\\n")

print(f"Bad antennas written to {{badants_file}}")

# Find best refant
print("Finding best reference antenna...")
ant_stats = defaultdict(lambda: {{'valid_amplitudes': [], 'total_samples': 0, 'flagged_samples': 0}})

with tables.table(ms_path, ack=False) as tb:
    nrows = min(tb.nrows(), 100000)
    ant1 = tb.getcol('ANTENNA1', 0, nrows)
    ant2 = tb.getcol('ANTENNA2', 0, nrows)
    data = tb.getcol('DATA', 0, nrows)
    flags = tb.getcol('FLAG', 0, nrows)
    npols = data.shape[-1]
    
    for i in range(len(data)):
        a1, a2 = ant1[i], ant2[i]
        if npols == 4:
            pol_flags = flags[i, :, 0] | flags[i, :, 3]
        elif npols == 2:
            pol_flags = flags[i, :, 0] | flags[i, :, 1]
        else:
            pol_flags = flags[i, :, 0]
        valid_mask = ~pol_flags
        if np.any(valid_mask):
            if npols == 4:
                amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 3])) / 2
            elif npols == 2:
                amp = (np.abs(data[i, valid_mask, 0]) + np.abs(data[i, valid_mask, 1])) / 2
            else:
                amp = np.abs(data[i, valid_mask, 0])
            for ant in [a1, a2]:
                ant_stats[ant]['total_samples'] += len(valid_mask)
                ant_stats[ant]['flagged_samples'] += np.sum(~valid_mask)
                ant_stats[ant]['valid_amplitudes'].extend(amp.tolist())

refant_scores = {{}}
for ant_idx, stats in ant_stats.items():
    if ant_idx in mock_ants or len(stats['valid_amplitudes']) < 100:
        continue
    amps = np.array(stats['valid_amplitudes'])
    median_amp = np.median(amps)
    std_amp = np.std(amps)
    if std_amp == 0:
        continue
    flag_fraction = stats['flagged_samples'] / stats['total_samples'] if stats['total_samples'] > 0 else 1.0
    snr_metric = (median_amp / std_amp) * (1 - flag_fraction)
    refant_scores[ant_names[ant_idx]] = {{'snr_metric': float(snr_metric), 'flag_fraction': float(flag_fraction)}}

best_refant = ant_names[0]
if refant_scores:
    best_refant = max(refant_scores, key=lambda x: refant_scores[x]['snr_metric'])
    print(f"Best refant: {{best_refant}} (SNR: {{refant_scores[best_refant]['snr_metric']:.3f}})")
else:
    print(f"Warning: Could not determine refant, using {{best_refant}}")

with open(refant_file, 'w') as f:
    json.dump({{'best_refant': best_refant, 'scores': refant_scores}}, f, indent=2)

# Append user bad antennas
if user_bad_ants:
    with open(badants_file, 'a') as f:
        for ant in user_bad_ants:
            f.write(f"mode='manual' antenna='{{ant}}' reason='user_specified'\\n")
    print(f"Added user bad antennas: {{user_bad_ants}}")

# Append standard flags
with open(badants_file, 'a') as f:
    f.write("# Standard flags\\n")
    f.write("mode='manual' autocorr=True reason='autocorr'\\n")
    f.write("mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'\\n")
    f.write("mode='quack' quackinterval=10 quackmode='beg' quackincrement=False reason='quackbeg'\\n")
    f.write("mode='quack' quackinterval=10 quackmode='endb' quackincrement=False reason='quackend'\\n")

print("Antenna analysis complete!")
'''
        
        script_file = f"antenna_{spw}.py"
        with open(script_file, 'w') as f:
            f.write(script)
        
        command = f"""cd {os.getcwd()}
{preamble}
python3 {script_file}
"""
        
        job = hk.submit(
            command=command,
            name=f"antenna_{spw}",
            job_subdir=spw,
            ppn=ppn,
            walltime=resources.get('walltime', '02:00:00')
        )
        
        if job.job_id:
            job_ids.append(job.job_id)
            job_map[job.job_id] = spw
            logger.info(f"Submitted antenna analysis for {spw}: {job.job_id}")
        
        time.sleep(0.5)
    
    if not job_ids:
        logger.error("No antenna analysis jobs submitted")
        return None, None
    
    # Wait for jobs
    logger.substep(f"Waiting for {len(job_ids)} antenna analysis jobs...")
    results = hk.wait_and_check(job_ids, whitelist=whitelist)
    
    # Process results
    successful = []
    failed = []
    refants = []
    
    for job_id, (job, log_result) in results.items():
        spw = job_map.get(job_id, 'unknown')
        
        # Find the job log file
        job_log = None
        for ext in ['.log', '.out', '.err']:
            for pattern in [f"{spw}/antenna_{spw}{ext}", f"jobs/antenna_{spw}{ext}", f"antenna_{spw}{ext}"]:
                if os.path.exists(pattern):
                    job_log = pattern
                    break
            if job_log:
                break
        
        if not log_result.success:
            failed.append(spw)
            if job_log:
                logger.error(f"{spw}: FAILED - see log: {os.path.abspath(job_log)}")
            else:
                logger.error(f"{spw}: FAILED")
            
            # Show actual errors from log
            if log_result.error_lines:
                for err in log_result.error_lines[:5]:
                    logger.error(f"  >> {err}")
            continue
        
        # Check outputs
        if os.path.exists(f"{spw}/badants.txt") and os.path.exists(f"{spw}/refant.json"):
            successful.append(spw)
            
            with open(f"{spw}/refant.json", 'r') as f:
                refant_data = json.load(f)
                refants.append(refant_data['best_refant'])
            
            logger.info(f"{spw}: OK (refant: {refant_data['best_refant']})")
        else:
            failed.append(spw)
            missing = []
            if not os.path.exists(f"{spw}/badants.txt"):
                missing.append("badants.txt")
            if not os.path.exists(f"{spw}/refant.json"):
                missing.append("refant.json")
            
            if job_log:
                logger.error(f"{spw}: Missing {missing} - see log: {os.path.abspath(job_log)}")
            else:
                logger.error(f"{spw}: Missing {missing}")
    
    if not successful:
        return None, None
    
    # Pick most common refant
    if refants:
        from collections import Counter
        best_refant = Counter(refants).most_common(1)[0][0]
        logger.info(f"Selected refant: {best_refant}")
    else:
        best_refant = None
    
    return successful, best_refant
