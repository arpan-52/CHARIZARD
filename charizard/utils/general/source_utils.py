# charizard/utils/general/source_utils.py
"""
Source and calibrator utilities.

- CalibratorMatcher: matches fields to known calibrators by position
- build_calibration_plan: creates cal_plan dict with all calibrator info

Logic:
1. Load VLA calibrator XML database (NRAO format)
2. For each MS field:
   - If name matches known flux calibrator → flux_cal
   - If position matches calibrator database → phase_cal  
   - Otherwise → target/source
"""

import os
import numpy as np
import xml.etree.ElementTree as ET
import yaml
from typing import Dict, List, Any, Optional
from pathlib import Path

try:
    from astropy.coordinates import SkyCoord
    from astropy import units as u
    HAS_ASTROPY = True
except ImportError:
    HAS_ASTROPY = False


# Known CASA flux calibrators (setjy knows these)
AMP_CAL_STANDARD_NAMES = ["3C286", "3C48", "3C147", "3C138"]


class CalibratorMatcher:
    """
    Match MS fields to known calibrators by position.
    Uses VLA calibrator catalog (XML in NRAO format).
    """
    
    # Band wavelengths in cm
    BAND_WAVELENGTHS = {
        '4': 400.0, 'P': 90.0, 'L': 20.0, 'S': 10.0, 'C': 6.0,
        'X': 3.5, 'Ku': 2.0, 'K': 1.3, 'Ka': 0.9, 'Q': 0.7
    }
    
    def __init__(self, 
                 xml_path: Optional[str] = None,
                 user_models: Optional[Dict] = None,
                 logger=None):
        """
        Initialize matcher with calibrator database.
        
        Args:
            xml_path: Path to vla_calibrators.xml (NRAO format)
            user_models: User-provided polcal models (from --models)
            logger: Optional logger
        """
        self.logger = logger
        self.calibrators = []  # List of {name, coord, bands}
        self.user_models = user_models or {}
        
        # Find data file
        if xml_path is None:
            data_dir = Path(__file__).parent.parent.parent / "data"
            xml_path = data_dir / "vla_calibrators.xml"
        
        # Load XML catalog
        if os.path.exists(xml_path):
            self._load_xml_catalog(str(xml_path))
        else:
            if logger:
                logger.warning(f"Calibrator XML not found: {xml_path}")
    
    def _parse_ra_dec(self, ra_str: str, dec_str: str):
        """Parse RA/DEC strings to SkyCoord"""
        if not HAS_ASTROPY:
            return None
        
        try:
            coord = SkyCoord(ra_str, dec_str, unit=(u.hourangle, u.deg))
            return coord
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not parse coordinates {ra_str}, {dec_str}: {e}")
            return None
    
    def _parse_band_wavelength(self, band_str: str) -> Optional[float]:
        """Parse band string to wavelength in cm"""
        band = band_str.strip().upper()
        return self.BAND_WAVELENGTHS.get(band)
    
    def _load_xml_catalog(self, xml_path: str):
        """Load VLA calibrator catalog from XML (NRAO format)"""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            # NRAO format: <calibrator><header><j2000> with IAU_NAME, RA, DEC
            #              <bands><band> with BAND, UVMIN_KLAMBDA, UVMAX_KLAMBDA
            for cal in root.findall('calibrator'):
                j2000 = cal.find('header/j2000')
                if j2000 is not None:
                    name = j2000.findtext('IAU_NAME')
                    ra = j2000.findtext('RA')
                    dec = j2000.findtext('DEC')
                    coord = self._parse_ra_dec(ra, dec)
                    
                    if coord is not None and name:
                        # Extract band information
                        bands = []
                        bands_elem = cal.find('bands')
                        if bands_elem is not None:
                            for band in bands_elem.findall('band'):
                                band_name = band.findtext('BAND', '').strip()
                                uvmin = band.findtext('UVMIN_KLAMBDA', '').strip()
                                uvmax = band.findtext('UVMAX_KLAMBDA', '').strip()
                                
                                wavelength_cm = self._parse_band_wavelength(band_name)
                                
                                if wavelength_cm is not None:
                                    band_info = {
                                        'band': band_name,
                                        'wavelength_cm': wavelength_cm,
                                        'uvmin': uvmin if uvmin else None,
                                        'uvmax': uvmax if uvmax else None
                                    }
                                    bands.append(band_info)
                        
                        if bands:
                            self.calibrators.append({
                                "name": name,
                                "coord": coord,
                                "bands": bands
                            })
            
            if self.logger:
                self.logger.info(f"Loaded {len(self.calibrators)} calibrators from XML")
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error loading calibrator XML: {e}")
    
    def is_amp_cal(self, source_name: str) -> bool:
        """Check if source is a standard amplitude calibrator by name"""
        return any(source_name.upper() == amp.upper() for amp in AMP_CAL_STANDARD_NAMES)
    
    def match_source_to_calibrator(self, source_coord, source_name: str, 
                                    threshold_arcsec: float = 60.0):
        """
        Match source to calibrator by position.
        
        Args:
            source_coord: SkyCoord of source
            source_name: Name of source (for logging)
            threshold_arcsec: Match threshold in arcseconds
        
        Returns:
            Matched calibrator dict or None
        """
        if source_coord is None or not HAS_ASTROPY:
            return None
        
        best_match = None
        best_separation = float('inf')
        
        for cal in self.calibrators:
            sep_arcsec = source_coord.separation(cal["coord"]).arcsec
            if sep_arcsec <= threshold_arcsec and sep_arcsec < best_separation:
                best_match = cal
                best_separation = sep_arcsec
        
        if best_match and self.logger:
            self.logger.info(f"Position match: {source_name} -> {best_match['name']} ({best_separation:.1f} arcsec)")
        
        return best_match
    
    def find_closest_band(self, calibrator_bands: List[Dict], ms_wavelength_cm: float):
        """Find the band closest to MS wavelength"""
        if not calibrator_bands or ms_wavelength_cm is None:
            return None
        
        closest_band = None
        min_diff = float('inf')
        
        for band in calibrator_bands:
            diff = abs(band['wavelength_cm'] - ms_wavelength_cm)
            if diff < min_diff:
                min_diff = diff
                closest_band = band
        
        return closest_band
    
    def get_calibrator_uvrange(self, calibrator: Dict, ms_wavelength_cm: float) -> Optional[str]:
        """Get UV range for calibrator based on closest band to MS frequency"""
        if not calibrator or not calibrator.get('bands'):
            return None
        
        closest_band = self.find_closest_band(calibrator['bands'], ms_wavelength_cm)
        
        if closest_band:
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
                return f"{int(uvmin)}~{int(uvmax)}klambda"
            elif uvmax is not None:
                return f"0~{int(uvmax)}klambda"
            elif uvmin is not None:
                return f">{int(uvmin)}klambda"
        
        return None
    
    def get_user_polcal_model(self, source_name: str) -> Optional[Dict]:
        """Get polcal model from user models by name match"""
        if not self.user_models:
            return None
        
        # Check full_stokes section
        full_stokes = self.user_models.get('full_stokes', {})
        if source_name in full_stokes:
            return full_stokes[source_name]
        
        return None


def build_calibration_plan(ms_info: Dict, config, logger) -> Dict[str, Any]:
    """
    Build calibration plan from MS info and config.
    
    Logic:
    1. For each field in MS:
       - If name matches AMP_CAL_STANDARD_NAMES → amp_cal (flux cal)
       - Else if position matches calibrator DB → phase_cal
       - Else → target
    2. Apply user overrides (always win)
    
    Args:
        ms_info: Dict from get_ms_info()
        config: PipelineConfig
        logger: Logger
    
    Returns:
        cal_plan dict
    """
    if not HAS_ASTROPY:
        logger.error("astropy not available - cannot do position matching")
        return _empty_cal_plan()
    
    cal_plan = {
        'flux_cal': None,
        'phase_cal': None,
        'leakage_cal': None,
        'polangle_cal': None,
        'targets': [],
        'all_calibrators': [],
        'calibrator_uvranges': {},
        'polcal_models': {},
    }
    
    # Get user overrides
    overrides = config.calibrator_overrides
    uvrange_overrides = config.uvrange_overrides
    user_models = config.user_models
    
    # Initialize matcher
    matcher = CalibratorMatcher(user_models=user_models, logger=logger)
    
    wavelength_cm = ms_info.get('wavelength_cm', 20.0)
    
    amp_cals = []
    phase_cals = []
    targets = []
    calibrator_details = {}
    
    # Categorize each field
    for fid, fdata in ms_info['fields'].items():
        field_name = fdata['name']
        ra_rad = fdata['ra_rad']
        dec_rad = fdata['dec_rad']
        
        # Create SkyCoord from radians
        field_coord = SkyCoord(ra=ra_rad * u.radian, dec=dec_rad * u.radian)
        
        if matcher.is_amp_cal(field_name):
            # Standard amplitude calibrator by name
            amp_cals.append(field_name)
            logger.info(f"Amplitude calibrator detected: {field_name} (name match)")
            
            # Find position match for UV range (use larger threshold for name matches)
            match = matcher.match_source_to_calibrator(field_coord, field_name, threshold_arcsec=180)
            if match:
                uvrange = matcher.get_calibrator_uvrange(match, wavelength_cm)
                if uvrange:
                    calibrator_details[field_name] = {'uvrange': uvrange}
                    logger.info(f"UV range for {field_name}: {uvrange}")
        else:
            # Check position match
            match = matcher.match_source_to_calibrator(field_coord, field_name, threshold_arcsec=60)
            if match:
                phase_cals.append(field_name)
                logger.info(f"Phase calibrator detected: {field_name} (position match to {match['name']})")
                
                # Get UV range
                uvrange = matcher.get_calibrator_uvrange(match, wavelength_cm)
                if uvrange:
                    calibrator_details[field_name] = {'uvrange': uvrange}
                    logger.info(f"UV range for {field_name}: {uvrange}")
            else:
                targets.append(field_name)
                logger.info(f"Science target detected: {field_name}")
    
    # Set flux cal (first amp cal)
    if amp_cals:
        cal_plan['flux_cal'] = amp_cals[0]
    
    # Set phase cal (first phase cal, or first amp cal if no phase cals)
    if phase_cals:
        cal_plan['phase_cal'] = phase_cals[0]
    elif amp_cals:
        cal_plan['phase_cal'] = amp_cals[0]
    
    # Set targets
    cal_plan['targets'] = targets
    
    # Set all calibrators
    cal_plan['all_calibrators'] = amp_cals + phase_cals
    
    # Set UV ranges
    for name, details in calibrator_details.items():
        if 'uvrange' in details:
            cal_plan['calibrator_uvranges'][name] = details['uvrange']
    
    # Check for user polcal models
    for cal in cal_plan['all_calibrators']:
        model = matcher.get_user_polcal_model(cal)
        if model:
            cal_plan['polcal_models'][cal] = model
            logger.info(f"User polcal model found for {cal}")
    
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
    
    # Rebuild all_calibrators list
    all_cals = set()
    for cal in [cal_plan['flux_cal'], cal_plan['phase_cal'], 
                cal_plan['leakage_cal'], cal_plan['polangle_cal']]:
        if cal:
            all_cals.add(cal)
    cal_plan['all_calibrators'] = sorted(list(all_cals))
    
    return cal_plan


def _empty_cal_plan() -> Dict[str, Any]:
    """Return empty calibration plan"""
    return {
        'flux_cal': None,
        'phase_cal': None,
        'leakage_cal': None,
        'polangle_cal': None,
        'targets': [],
        'all_calibrators': [],
        'calibrator_uvranges': {},
        'polcal_models': {},
    }
