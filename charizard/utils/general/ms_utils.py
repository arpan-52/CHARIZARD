# charizard/utils/general/ms_utils.py
"""
Measurement Set utilities
"""

import os
import shutil
import numpy as np
from typing import Dict, List, Any, Optional

try:
    from casacore import tables
except ImportError:
    tables = None


def get_ms_info(ms_path: str) -> Dict[str, Any]:
    """
    Get comprehensive MS metadata.
    
    Args:
        ms_path: Path to measurement set
    
    Returns:
        dict with fields, spws, correlations, antennas, wavelength, etc.
    """
    if tables is None:
        raise ImportError("casacore not available - install with: pip install python-casacore")
    
    if not os.path.exists(ms_path):
        raise FileNotFoundError(f"MS not found: {ms_path}")
    
    info = {'ms_path': ms_path}
    
    # Field info with coordinates
    info['fields'] = {}
    with tables.table(f"{ms_path}/FIELD", readonly=True, ack=False) as tb:
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
    with tables.table(f"{ms_path}/SPECTRAL_WINDOW", readonly=True, ack=False) as tb:
        num_chan = tb.getcol('NUM_CHAN')
        chan_freq = tb.getcol('CHAN_FREQ')
        info['num_spws'] = tb.nrows()
        info['num_channels'] = int(num_chan[0])
        info['channels_per_spw'] = [int(n) for n in num_chan]
        
        # Central frequency
        all_freqs = chan_freq.flatten()
        info['central_freq_hz'] = float(np.mean(all_freqs))
        info['min_freq_hz'] = float(np.min(all_freqs))
        info['max_freq_hz'] = float(np.max(all_freqs))
        
        # Wavelength
        c = 2.998e8
        wavelength_m = c / info['central_freq_hz']
        info['wavelength_cm'] = float(wavelength_m * 100)
    
    # Polarization info
    with tables.table(f"{ms_path}/POLARIZATION", readonly=True, ack=False) as tb:
        corr_type = tb.getcol('CORR_TYPE')[0]
        info['num_corrs'] = len(corr_type)
        info['corr_types'] = [int(c) for c in corr_type]
        
        # Determine polarization basis
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
    with tables.table(f"{ms_path}/ANTENNA", readonly=True, ack=False) as tb:
        info['antennas'] = list(tb.getcol('NAME'))
        info['num_antennas'] = len(info['antennas'])
    
    return info


def _corr_names(corr_types: List[int], basis: str) -> List[str]:
    """Convert correlation type numbers to names"""
    circular = {5: 'RR', 6: 'RL', 7: 'LR', 8: 'LL'}
    linear = {9: 'XX', 10: 'XY', 11: 'YX', 12: 'YY'}
    mapping = circular if basis == 'circular' else linear
    return [mapping.get(c, str(c)) for c in corr_types]


def get_field_names(ms_info: Dict) -> List[str]:
    """Get list of field names from ms_info"""
    return [f['name'] for f in ms_info['fields'].values()]


def remove_lock(ms_path: str) -> bool:
    """
    Remove table lock from MS.
    
    Args:
        ms_path: Path to measurement set
    
    Returns:
        True if lock removed or didn't exist
    """
    lock_path = os.path.join(ms_path, "table.lock")
    if os.path.exists(lock_path):
        try:
            os.remove(lock_path)
            return True
        except OSError:
            return False
    return True


def check_ms_exists(ms_path: str) -> bool:
    """Check if MS exists and has required tables"""
    if not os.path.isdir(ms_path):
        return False
    
    required = ['table.dat', 'FIELD', 'SPECTRAL_WINDOW', 'ANTENNA']
    for item in required:
        if not os.path.exists(os.path.join(ms_path, item)):
            return False
    
    return True
