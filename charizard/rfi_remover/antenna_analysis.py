# charizard/rfi_remover/antenna_analysis.py
"""
Antenna Analysis:
1. Find bad/dead antennas
2. Find best reference antenna (highest SNR, least flagged)
"""

import os
import numpy as np
from collections import defaultdict
from multiprocessing import Pool
from typing import Dict, List, Tuple, Optional
from casacore import tables


def analyze_antennas(ms_path: str, n_processes: int = 4, 
                     logger=None) -> Dict:
    """
    Analyze all antennas to find:
    1. Bad antennas (dead, low signal)
    2. Best reference antenna (highest SNR, least flagged)
    
    Args:
        ms_path: Path to measurement set
        n_processes: Number of parallel processes
        logger: Optional logger
    
    Returns:
        dict with:
            - bad_antennas: list of bad antenna names
            - refant: best reference antenna name
            - refant_candidates: ranked list of good antennas
            - antenna_stats: per-antenna statistics
    """
    if logger:
        logger.substep(f"Analyzing antennas in {ms_path}")
    
    # Get antenna names
    with tables.table(f"{ms_path}/ANTENNA", readonly=True) as tb:
        ant_names = list(tb.getcol('NAME'))
    
    num_ants = len(ant_names)
    
    # Read data
    with tables.table(ms_path, readonly=True) as tb:
        ant1 = tb.getcol('ANTENNA1')
        ant2 = tb.getcol('ANTENNA2')
        data = tb.getcol('DATA')
        flags = tb.getcol('FLAG')
    
    npols = data.shape[-1]
    
    # Per-antenna statistics
    antenna_stats = {i: {
        'name': ant_names[i],
        'amplitudes': [],
        'total_visibilities': 0,
        'flagged_visibilities': 0
    } for i in range(num_ants)}
    
    # Collect statistics per antenna
    for i in range(len(data)):
        a1, a2 = ant1[i], ant2[i]
        
        # Skip autocorrelations
        if a1 == a2:
            continue
        
        # Get parallel-hand correlations
        if npols == 4:
            pol_flags = flags[i, :, 0] | flags[i, :, 3]  # RR and LL
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
        
        # Update stats for both antennas in baseline
        for ant in [a1, a2]:
            antenna_stats[ant]['total_visibilities'] += len(flags[i])
            antenna_stats[ant]['flagged_visibilities'] += np.sum(pol_flags if npols >= 2 else flags[i, :, 0])
            if len(amp) > 0:
                antenna_stats[ant]['amplitudes'].extend(amp.tolist())
    
    # Calculate metrics
    bad_antennas = []
    antenna_metrics = []
    
    # Calculate global reference amplitude
    all_amps = []
    for stats in antenna_stats.values():
        if stats['amplitudes']:
            all_amps.extend(stats['amplitudes'])
    
    if not all_amps:
        return {
            'bad_antennas': [],
            'refant': ant_names[0] if ant_names else None,
            'refant_candidates': ant_names[:3] if ant_names else [],
            'antenna_stats': {}
        }
    
    global_median = np.median(all_amps)
    global_std = np.std(all_amps)
    
    for ant_idx, stats in antenna_stats.items():
        name = stats['name']
        
        if not stats['amplitudes']:
            # No valid data - bad antenna
            bad_antennas.append(name)
            continue
        
        amps = np.array(stats['amplitudes'])
        median_amp = np.median(amps)
        std_amp = np.std(amps)
        
        # Flag fraction
        if stats['total_visibilities'] > 0:
            flag_fraction = stats['flagged_visibilities'] / stats['total_visibilities']
        else:
            flag_fraction = 1.0
        
        # SNR metric: mean / spread (higher is better)
        if std_amp > 0:
            snr = median_amp / std_amp
        else:
            snr = 0
        
        # Check if bad antenna
        # Bad if: amplitude too low/high OR too many flags
        is_bad = False
        
        if median_amp < global_median * 0.1:
            is_bad = True
            if logger:
                logger.warning(f"Antenna {name}: low amplitude ({median_amp:.3f} vs global {global_median:.3f})")
        
        if median_amp > global_median * 10:
            is_bad = True
            if logger:
                logger.warning(f"Antenna {name}: high amplitude ({median_amp:.3f} vs global {global_median:.3f})")
        
        if flag_fraction > 0.8:
            is_bad = True
            if logger:
                logger.warning(f"Antenna {name}: highly flagged ({flag_fraction*100:.1f}%)")
        
        if is_bad:
            bad_antennas.append(name)
        else:
            # Good antenna - calculate combined metric for refant selection
            # Higher SNR is better, lower flag fraction is better
            # Combined metric: SNR * (1 - flag_fraction)
            metric = snr * (1 - flag_fraction)
            antenna_metrics.append((name, metric, snr, flag_fraction, median_amp))
    
    # Sort by metric (descending)
    antenna_metrics.sort(key=lambda x: x[1], reverse=True)
    
    # Best reference antenna
    if antenna_metrics:
        refant = antenna_metrics[0][0]
        refant_candidates = [m[0] for m in antenna_metrics[:5]]
    else:
        refant = ant_names[0] if ant_names else None
        refant_candidates = []
    
    # Build detailed stats
    detailed_stats = {}
    for name, metric, snr, flag_frac, med_amp in antenna_metrics:
        detailed_stats[name] = {
            'metric': metric,
            'snr': snr,
            'flag_fraction': flag_frac,
            'median_amplitude': med_amp
        }
    
    if logger:
        logger.info(f"Found {len(bad_antennas)} bad antennas")
        logger.info(f"Best refant: {refant} (SNR={antenna_metrics[0][2]:.2f}, flags={antenna_metrics[0][3]*100:.1f}%)" if antenna_metrics else "No good antennas found")
        if refant_candidates:
            logger.info(f"Refant candidates: {', '.join(refant_candidates)}")
    
    return {
        'bad_antennas': bad_antennas,
        'refant': refant,
        'refant_candidates': refant_candidates,
        'antenna_stats': detailed_stats
    }


def write_flag_commands(output_file: str, mode: str = 'w',
                        bad_antennas: Optional[List[str]] = None,
                        flags_to_include: Optional[List[str]] = None,
                        **kwargs):
    """
    Write flag commands for CASA flagdata
    
    Args:
        output_file: Output file path
        mode: 'w' for write, 'a' for append
        bad_antennas: List of bad antenna names
        flags_to_include: List of flag types: ['shadow', 'autocorr', 'clip', 'quack', 'badant']
        **kwargs: Additional parameters
    """
    if flags_to_include is None:
        flags_to_include = ['shadow', 'autocorr', 'clip', 'quack']
    
    commands = []
    
    if 'shadow' in flags_to_include:
        commands.append("mode='shadow' reason='shadow'")
    
    if 'autocorr' in flags_to_include:
        commands.append("mode='manual' autocorr=True reason='autocorr'")
    
    if 'clip' in flags_to_include:
        commands.append("mode='clip' correlation='ABS_ALL' clipzeros=True reason='clip_zeros'")
    
    if 'quack' in flags_to_include:
        quack_interval = kwargs.get('quack_interval', 10.0)
        commands.append(f"mode='quack' quackinterval={quack_interval} quackmode='beg' reason='quackbeg'")
        commands.append(f"mode='quack' quackinterval={quack_interval} quackmode='endb' reason='quackend'")
    
    if 'badant' in flags_to_include and bad_antennas:
        ant_str = ','.join(bad_antennas)
        commands.append(f"mode='manual' antenna='{ant_str}' reason='bad_antenna'")
    
    with open(output_file, mode) as f:
        f.write('\n'.join(commands))
        f.write('\n')


def add_user_bad_antennas(output_file: str, bad_list: List[str], mode: str = 'a'):
    """Add user-specified bad antennas"""
    if not bad_list:
        return
    
    ant_str = ','.join(bad_list) if isinstance(bad_list, list) else str(bad_list)
    
    if ant_str and ant_str.lower() != 'none':
        with open(output_file, mode) as f:
            f.write(f"mode='manual' antenna='{ant_str}' reason='user_specified'\n")
