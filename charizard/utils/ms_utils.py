# charizard/utils/ms_utils.py
"""
Measurement Set utilities
"""

import os
import numpy as np
from casacore import tables
from typing import Dict, List, Optional, Any


def get_ms_info(ms_path: str) -> Dict[str, Any]:
    """
    Get comprehensive MS metadata
    
    Returns:
        dict with fields, spws, correlations, antennas, scans, etc.
    """
    info = {}
    
    # Field info with coordinates
    info['fields'] = {}
    with tables.table(f"{ms_path}/FIELD", readonly=True) as tb:
        names = tb.getcol('NAME')
        phase_dir = tb.getcol('PHASE_DIR')
        for i, name in enumerate(names):
            ra = phase_dir[i, 0, 0]
            dec = phase_dir[i, 0, 1]
            info['fields'][i] = {
                'name': name,
                'ra_rad': float(ra),
                'dec_rad': float(dec)
            }
    
    # Spectral window info
    with tables.table(f"{ms_path}/SPECTRAL_WINDOW", readonly=True) as tb:
        num_chan = tb.getcol('NUM_CHAN')
        chan_freq = tb.getcol('CHAN_FREQ')
        info['num_spws'] = tb.nrows()
        info['num_channels'] = int(num_chan[0])
        info['channels_per_spw'] = [int(n) for n in num_chan]
        
        all_freqs = chan_freq.flatten()
        info['central_freq_hz'] = float(np.mean(all_freqs))
        c = 2.998e8
        wavelength_m = c / info['central_freq_hz']
        info['wavelength_cm'] = float(wavelength_m * 100)
    
    # Polarization info
    with tables.table(f"{ms_path}/POLARIZATION", readonly=True) as tb:
        corr_type = tb.getcol('CORR_TYPE')[0]
        info['num_corrs'] = len(corr_type)
        info['corr_types'] = [int(c) for c in corr_type]
        
        if 5 in corr_type or 8 in corr_type:
            info['pol_basis'] = 'circular'
            info['corr_names'] = _corr_names(corr_type, 'circular')
        elif 9 in corr_type or 12 in corr_type:
            info['pol_basis'] = 'linear'
            info['corr_names'] = _corr_names(corr_type, 'linear')
        else:
            info['pol_basis'] = 'unknown'
            info['corr_names'] = [str(c) for c in corr_type]
    
    # Antenna info
    with tables.table(f"{ms_path}/ANTENNA", readonly=True) as tb:
        info['antennas'] = list(tb.getcol('NAME'))
        info['num_antennas'] = len(info['antennas'])
    
    # Scan info
    info['scans'] = {}
    with tables.table(ms_path, readonly=True) as tb:
        scan_col = tb.getcol('SCAN_NUMBER')
        field_col = tb.getcol('FIELD_ID')
        
        unique_scans = np.unique(scan_col)
        for scan in unique_scans:
            mask = scan_col == scan
            field_id = int(field_col[mask][0])
            field_name = info['fields'][field_id]['name']
            info['scans'][int(scan)] = {
                'field_id': field_id,
                'field_name': field_name
            }
    
    return info


def _corr_names(corr_types, basis):
    """Convert correlation type numbers to names"""
    circular = {5: 'RR', 6: 'RL', 7: 'LR', 8: 'LL'}
    linear = {9: 'XX', 10: 'XY', 11: 'YX', 12: 'YY'}
    mapping = circular if basis == 'circular' else linear
    return [mapping.get(c, str(c)) for c in corr_types]


def get_field_names(ms_info: Dict) -> List[str]:
    """Get list of field names"""
    return [f['name'] for f in ms_info['fields'].values()]


def get_active_spws(base_dir: str = '.') -> List[str]:
    """Get list of active SPW directories"""
    spws = []
    for item in os.listdir(base_dir):
        if item.startswith('spw') and os.path.isdir(os.path.join(base_dir, item)):
            spws.append(item)
    return sorted(spws)


def remove_lock(ms_path: str):
    """Remove table lock file"""
    lock_file = f"{ms_path}/table.lock"
    if os.path.exists(lock_file):
        os.remove(lock_file)
