# charizard/utils/source_utils.py
"""
Calibrator matching and classification by POSITION (RA/DEC)

Flow:
1. Read MS field positions
2. Match to known calibrators by position (< 2 arcsec)
3. Get UV ranges from XML based on MS frequency
4. User overrides take priority
5. Report assumptions (e.g., "using X for leakage, assuming unpolarized")
"""

import os
import re
import yaml
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from astropy.coordinates import SkyCoord
import astropy.units as u
import numpy as np


def get_data_dir():
    """Get path to charizard/data directory"""
    return Path(__file__).parent.parent / 'data'


# =============================================================================
# Internal Models (with RA/DEC for position matching)
# =============================================================================

def load_internal_models():
    """Load internal calibrator models"""
    path = get_data_dir() / 'internal_models.yaml'
    if not path.exists():
        return {}
    with open(path, 'r') as f:
        return yaml.safe_load(f) or {}


def load_user_models(user_models_path: Optional[str]) -> Dict:
    """Load and merge user models with internal"""
    internal = load_internal_models()
    
    if user_models_path and os.path.exists(user_models_path):
        with open(user_models_path, 'r') as f:
            user = yaml.safe_load(f) or {}
        internal = _deep_merge(internal, user)
    
    return internal


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Deep merge dicts - override takes priority"""
    if override is None:
        return base
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


# =============================================================================
# Calibrator Database (from XML)
# =============================================================================

class CalibratorDatabase:
    """
    Load calibrator positions and UV ranges from VLA calibrator XML.
    Matches sources by position (RA/DEC) only.
    """
    
    def __init__(self, xml_path: Optional[str] = None, logger=None):
        self.logger = logger
        self.calibrators = []  # List of {name, coord, bands}
        
        if xml_path is None:
            xml_path = get_data_dir() / 'vla_calibrators.xml'
        
        self.xml_path = str(xml_path)
        self._load_xml()
    
    def _log(self, msg: str, level: str = 'info'):
        if self.logger:
            getattr(self.logger, level, self.logger.info)(msg)
    
    def _parse_coord(self, ra_str: str, dec_str: str) -> Optional[SkyCoord]:
        """Parse RA/DEC strings from XML format"""
        if not ra_str or not dec_str:
            return None
        try:
            # Handle hms/dms format: "13h31m08.28s" / "+30d30m32.9s"
            ra_clean = ra_str.replace("h", " ").replace("m", " ").replace("s", "")
            dec_clean = dec_str.replace("d", " ").replace("'", " ").replace('"', " ").replace("s", "")
            return SkyCoord(f"{ra_clean} {dec_clean}", unit=(u.hourangle, u.deg))
        except Exception as e:
            return None
    
    def _parse_band_wavelength(self, band_str: str) -> Optional[float]:
        """Parse band string to wavelength in cm"""
        if not band_str:
            return None
        try:
            match = re.match(r'([0-9.]+)\s*(cm|mm)', band_str.lower())
            if match:
                value = float(match.group(1))
                unit = match.group(2)
                return value / 10.0 if unit == 'mm' else value
        except:
            pass
        return None
    
    def _load_xml(self):
        """Load calibrator data from XML"""
        if not os.path.exists(self.xml_path):
            self._log(f"Calibrator XML not found: {self.xml_path}", 'warning')
            return
        
        try:
            tree = ET.parse(self.xml_path)
            root = tree.getroot()
            
            for cal in root.findall('calibrator'):
                j2000 = cal.find('header/j2000')
                if j2000 is None:
                    continue
                
                name = j2000.findtext('IAU_NAME')
                ra = j2000.findtext('RA')
                dec = j2000.findtext('DEC')
                coord = self._parse_coord(ra, dec)
                
                if coord is None or not name:
                    continue
                
                # Extract bands with UV ranges
                bands = []
                bands_elem = cal.find('bands')
                if bands_elem is not None:
                    for band in bands_elem.findall('band'):
                        band_name = band.findtext('BAND', '').strip()
                        uvmin = band.findtext('UVMIN_KLAMBDA', '').strip()
                        uvmax = band.findtext('UVMAX_KLAMBDA', '').strip()
                        
                        wavelength_cm = self._parse_band_wavelength(band_name)
                        if wavelength_cm is not None:
                            bands.append({
                                'band': band_name,
                                'wavelength_cm': wavelength_cm,
                                'uvmin': uvmin or None,
                                'uvmax': uvmax or None
                            })
                
                self.calibrators.append({
                    'name': name,
                    'coord': coord,
                    'bands': bands
                })
            
            self._log(f"Loaded {len(self.calibrators)} calibrators from XML")
            
        except Exception as e:
            self._log(f"Error loading XML: {e}", 'error')
    
    def match_by_position(self, ra_rad: float, dec_rad: float, 
                          threshold_arcsec: float = 2.0) -> Optional[Dict]:
        """
        Match position to known calibrator.
        
        Args:
            ra_rad: RA in radians
            dec_rad: DEC in radians
            threshold_arcsec: Match threshold (default 2 arcsec - tight!)
        
        Returns:
            Matched calibrator dict or None
        """
        source_coord = SkyCoord(ra=ra_rad*u.radian, dec=dec_rad*u.radian)
        
        best_match = None
        best_sep = float('inf')
        
        for cal in self.calibrators:
            sep = source_coord.separation(cal['coord']).arcsec
            if sep <= threshold_arcsec and sep < best_sep:
                best_match = cal
                best_sep = sep
        
        return best_match
    
    def get_uvrange(self, calibrator: Dict, ms_wavelength_cm: float) -> Optional[str]:
        """
        Get UV range for calibrator based on closest band to MS frequency.
        
        Returns string like "0~50klambda" or ">10klambda" or None
        """
        if not calibrator or not calibrator.get('bands'):
            return None
        
        # Find closest band
        closest_band = None
        min_diff = float('inf')
        
        for band in calibrator['bands']:
            diff = abs(band['wavelength_cm'] - ms_wavelength_cm)
            if diff < min_diff:
                min_diff = diff
                closest_band = band
        
        if not closest_band:
            return None
        
        uvmin_str = closest_band.get('uvmin', '')
        uvmax_str = closest_band.get('uvmax', '')
        
        uvmin = None
        uvmax = None
        
        if uvmin_str:
            try:
                uvmin = float(uvmin_str)
            except ValueError:
                pass
        
        if uvmax_str:
            try:
                uvmax = float(uvmax_str)
            except ValueError:
                pass
        
        # Generate UV range string
        if uvmin is not None and uvmax is not None:
            return f"{uvmin}~{uvmax}klambda"
        elif uvmin is not None:
            return f">{uvmin}klambda"
        elif uvmax is not None:
            return f"<{uvmax}klambda"
        
        return None


# =============================================================================
# Main Calibrator Matcher
# =============================================================================

class CalibratorMatcher:
    """
    Match MS fields to known calibrators by POSITION.
    Combines XML database + internal models.
    """
    
    # Standard CASA flux calibrators
    CASA_FLUX_CALS = ['3C286', '3C48', '3C147', '3C138']
    
    def __init__(self, user_models_path: Optional[str] = None, logger=None):
        self.logger = logger
        self.models = load_user_models(user_models_path)
        self.db = CalibratorDatabase(logger=logger)
    
    def _log(self, msg: str, level: str = 'info'):
        if self.logger:
            getattr(self.logger, level, self.logger.info)(msg)
    
    def classify_field(self, field_name: str, ra_rad: float, dec_rad: float,
                       ms_wavelength_cm: float) -> Dict:
        """
        Classify a single field by its position.
        
        Returns dict with:
            - canonical_name: matched calibrator name (or None)
            - uvrange: UV range string (or None)
            - is_calibrator: True if matched to calibrator
            - is_casa_flux_cal: True if CASA standard flux cal
            - is_known_unpolarized: True if known unpolarized
            - is_known_polarized: True if known polarized
            - polcal_model: polarization model dict (or None)
        """
        result = {
            'field_name': field_name,
            'canonical_name': None,
            'uvrange': None,
            'is_calibrator': False,
            'is_casa_flux_cal': False,
            'is_known_unpolarized': False,
            'is_known_polarized': False,
            'polcal_model': None
        }
        
        # Match by position (2 arcsec threshold)
        match = self.db.match_by_position(ra_rad, dec_rad, threshold_arcsec=2.0)
        
        if match:
            result['canonical_name'] = match['name']
            result['is_calibrator'] = True
            result['uvrange'] = self.db.get_uvrange(match, ms_wavelength_cm)
            
            self._log(f"Position match: {field_name} → {match['name']}")
            if result['uvrange']:
                self._log(f"  UV range: {result['uvrange']}")
        
        # Check against internal models (using canonical name if matched)
        check_name = result['canonical_name'] or field_name
        
        # CASA flux cal? check both MS field name and canonical name
        check_names = [field_name]
        if result['canonical_name']:
            check_names.append(result['canonical_name'])

        if any(name.upper() in (fc.upper() for fc in self.CASA_FLUX_CALS) for name in check_names):
            result['is_casa_flux_cal'] = True

        
        # Known unpolarized?
        for source, data in self.models.get('known_unpolarized', {}).items():
            if self._matches_source(check_name, ra_rad, dec_rad, source, data):
                result['is_known_unpolarized'] = True
                break
        
        # Known polarized?
        for source, data in self.models.get('known_polarized', {}).items():
            if self._matches_source(check_name, ra_rad, dec_rad, source, data):
                result['is_known_polarized'] = True
                break
        
        # Polcal model?
        for source, model in self.models.get('polcal_models', {}).items():
            if self._matches_source(check_name, ra_rad, dec_rad, source, model):
                result['polcal_model'] = model
                break
        
        return result
    
    def _matches_source(self, field_name: str, ra_rad: float, dec_rad: float,
                        source_name: str, source_data: Dict) -> bool:
        """Check if field matches a source from internal models"""
        # First try position match if RA/DEC available
        if 'ra' in source_data and 'dec' in source_data:
            try:
                source_coord = SkyCoord(source_data['ra'], source_data['dec'])
                field_coord = SkyCoord(ra=ra_rad*u.radian, dec=dec_rad*u.radian)
                if field_coord.separation(source_coord).arcsec < 2.0:
                    return True
            except:
                pass
        
        # Fallback: check canonical name
        check_name = field_name.upper().strip()
        if check_name == source_name.upper():
            return True
        
        # Check aliases
        for alias in source_data.get('names', []):
            if check_name == alias.upper().strip():
                return True
        
        return False
    
    def analyze_ms_fields(self, ms_info: Dict) -> Dict[str, Dict]:
        """Analyze all fields in MS"""
        wavelength_cm = ms_info.get('wavelength_cm', 20.0)
        
        results = {}
        for field_id, field_data in ms_info['fields'].items():
            classification = self.classify_field(
                field_data['name'],
                field_data['ra_rad'],
                field_data['dec_rad'],
                wavelength_cm
            )
            results[field_data['name']] = classification
        
        return results


# =============================================================================
# Field Categorization
# =============================================================================

def categorize_fields(field_classifications: Dict, config: Dict, logger=None) -> Dict:
    """
    Categorize fields into calibrators and targets.
    User overrides take priority!
    
    Returns:
        flux_cal, phase_cal, leakage_cal, polangle_cal, targets, all_calibrators
    """
    result = {
        'flux_cal': None,
        'phase_cal': None,
        'leakage_cal': None,
        'polangle_cal': None,
        'targets': [],
        'all_calibrators': [],
        'calibrator_uvranges': {}  # field -> uvrange
    }
    
    # Get user overrides
    overrides = config.get('sources', {}).get('overrides', {}).get('calibrators', {})
    
    # User-specified calibrators take priority
    if overrides.get('flux'):
        flux = overrides['flux']
        result['flux_cal'] = flux[0] if isinstance(flux, list) else flux
        if logger:
            logger.info(f"Flux cal (user specified): {result['flux_cal']}")
    
    if overrides.get('phase'):
        result['phase_cal'] = overrides['phase']
        if logger:
            logger.info(f"Phase cal (user specified): {result['phase_cal']}")
    
    if overrides.get('leakage'):
        result['leakage_cal'] = overrides['leakage']
        if logger:
            logger.info(f"Leakage cal (user specified): {result['leakage_cal']}")
    
    if overrides.get('pol_angle'):
        result['polangle_cal'] = overrides['pol_angle']
        if logger:
            logger.info(f"Pol angle cal (user specified): {result['polangle_cal']}")
    
    # Auto-detect if not specified
    for name, info in field_classifications.items():
        # Store UV ranges
        if info.get('uvrange'):
            result['calibrator_uvranges'][name] = info['uvrange']
        
        # Auto-detect flux cal
        if result['flux_cal'] is None and info['is_casa_flux_cal']:
            result['flux_cal'] = name
            if logger:
                logger.info(f"Flux cal (auto-detected): {name}")
        
        # Auto-detect phase cal (calibrator but not flux cal)
        if result['phase_cal'] is None and info['is_calibrator'] and not info['is_casa_flux_cal']:
            result['phase_cal'] = name
            if logger:
                logger.info(f"Phase cal (auto-detected): {name}")
    
    # Build calibrator and target lists
    calibrators = set()
    for cal in [result['flux_cal'], result['phase_cal'], 
                result['leakage_cal'], result['polangle_cal']]:
        if cal:
            calibrators.add(cal)
    
    # Classify remaining fields
    for name, info in field_classifications.items():
        if name in calibrators:
            continue
        
        if info['is_calibrator']:
            calibrators.add(name)
            if logger:
                logger.info(f"Additional calibrator: {name}")
        else:
            result['targets'].append(name)
            if logger:
                logger.info(f"Target: {name}")
    
    result['all_calibrators'] = list(calibrators)
    
    # Report assumptions for polcal
    if result['leakage_cal']:
        leakage_info = field_classifications.get(result['leakage_cal'], {})
        if leakage_info.get('is_known_unpolarized'):
            if logger:
                logger.info(f"Leakage cal {result['leakage_cal']}: known unpolarized source")
        elif leakage_info.get('polcal_model'):
            if logger:
                logger.info(f"Leakage cal {result['leakage_cal']}: has polcal model")
        else:
            if logger:
                logger.warning(f"Leakage cal {result['leakage_cal']}: ASSUMING UNPOLARIZED (no model provided)")
    
    if result['polangle_cal']:
        polangle_info = field_classifications.get(result['polangle_cal'], {})
        if polangle_info.get('polcal_model'):
            if logger:
                logger.info(f"Pol angle cal {result['polangle_cal']}: has polcal model")
        else:
            if logger:
                logger.error(f"Pol angle cal {result['polangle_cal']}: NO MODEL - cannot do polangle calibration!")
    
    return result