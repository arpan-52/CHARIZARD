# charizard/utils/general/source_utils.py
"""
Source and calibrator utilities.

- CalibratorMatcher: matches fields to known calibrators by position
- build_calibration_plan: creates cal_plan dict with all calibrator info
"""

import os
import math
import yaml
import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from pathlib import Path


# Known CASA flux calibrators (setjy knows these)
CASA_FLUX_CALS = ["3C286", "3C48", "3C147", "3C138"]


@dataclass
class CalibratorInfo:
    """Information about a calibrator"""
    name: str
    ra_rad: float
    dec_rad: float
    is_calibrator: bool = False
    canonical_name: Optional[str] = None
    uvrange: Optional[str] = None
    bands: Dict[str, Dict] = field(default_factory=dict)
    polcal_model: Optional[Dict] = None


class CalibratorMatcher:
    """
    Match MS fields to known calibrators by position.
    Uses VLA calibrator catalog (XML) and internal models (YAML).
    """
    
    def __init__(self, 
                 xml_path: Optional[str] = None,
                 models_path: Optional[str] = None,
                 logger=None):
        """
        Initialize matcher with calibrator databases.
        
        Args:
            xml_path: Path to vla_calibrators.xml
            models_path: Path to internal_models.yaml
            logger: Optional logger
        """
        self.logger = logger
        self.calibrators = {}  # canonical_name -> CalibratorInfo
        self.polcal_models = {}  # name -> model dict
        self.known_unpolarized = []
        
        # Find data files
        data_dir = Path(__file__).parent.parent.parent / "data"
        
        if xml_path is None:
            xml_path = data_dir / "vla_calibrators.xml"
        if models_path is None:
            models_path = data_dir / "internal_models.yaml"
        
        # Load databases
        if os.path.exists(xml_path):
            self._load_xml_catalog(xml_path)
        
        if os.path.exists(models_path):
            self._load_internal_models(models_path)
    
    def _load_xml_catalog(self, xml_path: str):
        """Load VLA calibrator catalog from XML"""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            for source in root.findall('.//source'):
                name = source.get('name')
                if not name:
                    continue
                
                # Parse coordinates
                ra_str = source.get('ra', '')
                dec_str = source.get('dec', '')
                
                try:
                    ra_rad = self._parse_ra(ra_str)
                    dec_rad = self._parse_dec(dec_str)
                except:
                    continue
                
                # Parse bands and UV ranges
                bands = {}
                for band in source.findall('.//band'):
                    band_name = band.get('name', '')
                    flux = band.get('flux')
                    uvmin = band.get('uvmin')
                    uvmax = band.get('uvmax')
                    
                    if band_name:
                        bands[band_name] = {
                            'flux': float(flux) if flux else None,
                            'uvmin': float(uvmin) if uvmin else None,
                            'uvmax': float(uvmax) if uvmax else None,
                        }
                
                self.calibrators[name] = CalibratorInfo(
                    name=name,
                    ra_rad=ra_rad,
                    dec_rad=dec_rad,
                    is_calibrator=True,
                    canonical_name=name,
                    bands=bands
                )
            
            if self.logger:
                self.logger.info(f"Loaded {len(self.calibrators)} calibrators from XML")
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to load XML catalog: {e}")
    
    def _load_internal_models(self, models_path: str):
        """Load internal polcal models from YAML"""
        try:
            with open(models_path, 'r') as f:
                data = yaml.safe_load(f)
            
            # Polcal models
            self.polcal_models = data.get('polcal_models', {})
            
            # Add polcal sources to calibrators if not already present
            for name, model in self.polcal_models.items():
                if name not in self.calibrators:
                    ra_rad = model.get('ra_rad', 0)
                    dec_rad = model.get('dec_rad', 0)
                    self.calibrators[name] = CalibratorInfo(
                        name=name,
                        ra_rad=ra_rad,
                        dec_rad=dec_rad,
                        is_calibrator=True,
                        canonical_name=name,
                        polcal_model=model
                    )
                else:
                    self.calibrators[name].polcal_model = model
            
            # Known unpolarized sources
            self.known_unpolarized = data.get('known_unpolarized', [])
            
            # CASA flux calibrators (add if not present)
            for name in data.get('casa_flux_cals', CASA_FLUX_CALS):
                if name in self.calibrators:
                    self.calibrators[name].is_calibrator = True
            
            if self.logger:
                self.logger.info(f"Loaded {len(self.polcal_models)} polcal models")
                
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Failed to load internal models: {e}")
    
    def _parse_ra(self, ra_str: str) -> float:
        """Parse RA string (HH:MM:SS.sss) to radians"""
        parts = ra_str.replace('h', ':').replace('m', ':').replace('s', '').split(':')
        hours = float(parts[0])
        minutes = float(parts[1]) if len(parts) > 1 else 0
        seconds = float(parts[2]) if len(parts) > 2 else 0
        
        hours_total = hours + minutes/60 + seconds/3600
        return hours_total * (math.pi / 12)  # hours to radians
    
    def _parse_dec(self, dec_str: str) -> float:
        """Parse Dec string (+DD:MM:SS.ss) to radians"""
        dec_str = dec_str.replace('d', ':').replace("'", ':').replace('"', '')
        
        sign = 1
        if dec_str.startswith('-'):
            sign = -1
            dec_str = dec_str[1:]
        elif dec_str.startswith('+'):
            dec_str = dec_str[1:]
        
        parts = dec_str.split(':')
        degrees = float(parts[0])
        minutes = float(parts[1]) if len(parts) > 1 else 0
        seconds = float(parts[2]) if len(parts) > 2 else 0
        
        degrees_total = degrees + minutes/60 + seconds/3600
        return sign * degrees_total * (math.pi / 180)  # degrees to radians
    
    def match_by_position(self, ra_rad: float, dec_rad: float, 
                          threshold_arcsec: float = 2.0) -> Optional[CalibratorInfo]:
        """
        Find calibrator matching position within threshold.
        
        Args:
            ra_rad: RA in radians
            dec_rad: Dec in radians
            threshold_arcsec: Match threshold in arcseconds
        
        Returns:
            CalibratorInfo if matched, None otherwise
        """
        threshold_rad = threshold_arcsec * (math.pi / 648000)  # arcsec to radians
        
        best_match = None
        best_sep = float('inf')
        
        for cal in self.calibrators.values():
            # Angular separation (small angle approximation)
            d_ra = (ra_rad - cal.ra_rad) * math.cos(dec_rad)
            d_dec = dec_rad - cal.dec_rad
            sep = math.sqrt(d_ra**2 + d_dec**2)
            
            if sep < threshold_rad and sep < best_sep:
                best_sep = sep
                best_match = cal
        
        return best_match
    
    def get_uvrange(self, calibrator_name: str, wavelength_cm: float) -> Optional[str]:
        """
        Get UV range for calibrator at given wavelength.
        
        Args:
            calibrator_name: Name of calibrator
            wavelength_cm: Observing wavelength in cm
        
        Returns:
            UV range string like "0~50klambda" or None
        """
        if calibrator_name not in self.calibrators:
            return None
        
        cal = self.calibrators[calibrator_name]
        if not cal.bands:
            return None
        
        # Find closest band by wavelength
        band_wavelengths = {
            'L': 20.0, 'S': 10.0, 'C': 6.0, 'X': 3.5, 
            'Ku': 2.0, 'K': 1.3, 'Ka': 0.9, 'Q': 0.7,
            '4': 75.0, 'P': 90.0
        }
        
        best_band = None
        best_diff = float('inf')
        
        for band_name in cal.bands.keys():
            if band_name in band_wavelengths:
                diff = abs(band_wavelengths[band_name] - wavelength_cm)
                if diff < best_diff:
                    best_diff = diff
                    best_band = band_name
        
        if best_band and best_band in cal.bands:
            band_info = cal.bands[best_band]
            uvmax = band_info.get('uvmax')
            if uvmax:
                return f"0~{int(uvmax)}klambda"
        
        return None
    
    def classify_field(self, field_name: str, ra_rad: float, dec_rad: float,
                       wavelength_cm: float) -> Dict[str, Any]:
        """
        Classify a field as calibrator or target.
        
        Args:
            field_name: Name of field from MS
            ra_rad: RA in radians
            dec_rad: Dec in radians
            wavelength_cm: Observing wavelength
        
        Returns:
            Dict with: canonical_name, is_calibrator, uvrange, polcal_model
        """
        # Try position match
        match = self.match_by_position(ra_rad, dec_rad)
        
        if match:
            uvrange = self.get_uvrange(match.canonical_name, wavelength_cm)
            return {
                'field_name': field_name,
                'canonical_name': match.canonical_name,
                'is_calibrator': True,
                'uvrange': uvrange,
                'polcal_model': match.polcal_model,
                'is_flux_cal': match.canonical_name in CASA_FLUX_CALS,
                'is_unpolarized': match.canonical_name in self.known_unpolarized,
            }
        
        # No match - assume target
        return {
            'field_name': field_name,
            'canonical_name': field_name,
            'is_calibrator': False,
            'uvrange': None,
            'polcal_model': None,
            'is_flux_cal': False,
            'is_unpolarized': False,
        }


def build_calibration_plan(ms_info: Dict, config, logger) -> Dict[str, Any]:
    """
    Build calibration plan from MS info and config.
    
    Combines auto-detection with user overrides.
    User overrides ALWAYS win.
    
    Args:
        ms_info: Dict from get_ms_info()
        config: PipelineConfig
        logger: Logger
    
    Returns:
        cal_plan dict with flux_cal, phase_cal, targets, uvranges, etc.
    """
    
    cal_plan = {
        'flux_cal': None,
        'phase_cal': None,
        'leakage_cal': None,
        'polangle_cal': None,
        'targets': [],
        'all_calibrators': [],
        'calibrator_uvranges': {},
        'polcal_models': {},
        'field_map': {},  # field_name -> classification
    }
    
    # Get user overrides
    overrides = config.calibrator_overrides
    uvrange_overrides = config.uvrange_overrides
    
    # Initialize matcher
    matcher = CalibratorMatcher(logger=logger)
    
    wavelength_cm = ms_info.get('wavelength_cm', 20.0)
    
    # Classify all fields
    for fid, fdata in ms_info['fields'].items():
        field_name = fdata['name']
        ra_rad = fdata['ra_rad']
        dec_rad = fdata['dec_rad']
        
        classification = matcher.classify_field(field_name, ra_rad, dec_rad, wavelength_cm)
        cal_plan['field_map'][field_name] = classification
        
        if classification['is_calibrator']:
            cal_plan['all_calibrators'].append(field_name)
            
            # Store UV range if found
            if classification['uvrange']:
                cal_plan['calibrator_uvranges'][field_name] = classification['uvrange']
            
            # Store polcal model if found
            if classification['polcal_model']:
                cal_plan['polcal_models'][field_name] = classification['polcal_model']
            
            # Auto-assign flux cal (first CASA flux cal found)
            if classification['is_flux_cal'] and not cal_plan['flux_cal']:
                cal_plan['flux_cal'] = field_name
                logger.info(f"Auto-detected flux cal: {field_name}")
        else:
            cal_plan['targets'].append(field_name)
    
    # If no flux cal found, use first calibrator
    if not cal_plan['flux_cal'] and cal_plan['all_calibrators']:
        cal_plan['flux_cal'] = cal_plan['all_calibrators'][0]
        logger.warning(f"No standard flux cal found, using: {cal_plan['flux_cal']}")
    
    # Phase cal = first calibrator that's not flux cal
    for cal in cal_plan['all_calibrators']:
        if cal != cal_plan['flux_cal']:
            cal_plan['phase_cal'] = cal
            break
    
    if not cal_plan['phase_cal']:
        cal_plan['phase_cal'] = cal_plan['flux_cal']
    
    # =========================================================================
    # APPLY USER OVERRIDES (always win)
    # =========================================================================
    
    if overrides:
        # Flux/amp cal
        amp_override = overrides.get('amp')
        if amp_override:
            if isinstance(amp_override, list):
                cal_plan['flux_cal'] = amp_override[0]
            else:
                cal_plan['flux_cal'] = amp_override
            logger.info(f"User override flux cal: {cal_plan['flux_cal']}")
        
        # Phase cal
        phase_override = overrides.get('phase')
        if phase_override:
            cal_plan['phase_cal'] = phase_override
            logger.info(f"User override phase cal: {cal_plan['phase_cal']}")
        
        # Leakage cal
        leakage_override = overrides.get('leakage')
        if leakage_override:
            cal_plan['leakage_cal'] = leakage_override
            logger.info(f"User override leakage cal: {cal_plan['leakage_cal']}")
        
        # Pol angle cal
        polangle_override = overrides.get('pol_angle')
        if polangle_override:
            cal_plan['polangle_cal'] = polangle_override
            logger.info(f"User override pol angle cal: {cal_plan['polangle_cal']}")
    
    # UV range overrides (always win)
    if uvrange_overrides:
        for cal_name, uvrange in uvrange_overrides.items():
            if uvrange:  # Only if not empty
                cal_plan['calibrator_uvranges'][cal_name] = uvrange
                logger.info(f"User override uvrange for {cal_name}: {uvrange}")
    
    # Build final all_calibrators list (unique)
    all_cals = set()
    for cal in [cal_plan['flux_cal'], cal_plan['phase_cal'], 
                cal_plan['leakage_cal'], cal_plan['polangle_cal']]:
        if cal:
            all_cals.add(cal)
    cal_plan['all_calibrators'] = sorted(list(all_cals))
    
    # Report assumptions
    if config.auto_detect:
        _report_assumptions(cal_plan, logger)
    
    return cal_plan


def _report_assumptions(cal_plan: Dict, logger):
    """Report any assumptions made during calibrator detection"""
    
    # Check polcal models
    if cal_plan['polangle_cal']:
        if cal_plan['polangle_cal'] in cal_plan['polcal_models']:
            logger.info(f"Polcal model found for {cal_plan['polangle_cal']}")
        else:
            logger.warning(f"NO POLCAL MODEL for {cal_plan['polangle_cal']} - will need user model")
    
    if cal_plan['leakage_cal']:
        field_info = cal_plan['field_map'].get(cal_plan['leakage_cal'], {})
        if field_info.get('is_unpolarized'):
            logger.info(f"Leakage cal {cal_plan['leakage_cal']} is known unpolarized source")
        else:
            logger.warning(f"ASSUMING {cal_plan['leakage_cal']} is unpolarized for leakage cal")
    
    # Check UV ranges
    for cal in cal_plan['all_calibrators']:
        if cal not in cal_plan['calibrator_uvranges']:
            logger.info(f"No UV range for {cal} - using all baselines")
