# charizard/utils/general/source_utils.py
"""
Source and calibrator utilities.
Follows original charizard auto_detect_utils.py logic exactly.
"""

import os
import re
import yaml
import xml.etree.ElementTree as ET
import numpy as np
from typing import Dict, List, Any, Optional
from pathlib import Path

from astropy.coordinates import SkyCoord
import astropy.units as u


# Known CASA flux calibrators
AMP_CAL_STANDARD_NAMES = [
    "3C147", "3C138", "3C48", "3C84", "3C295", "3C286",
    "3C196", "3C123", "J1331+3030", "J0521+1638"
]


class CalibratorMatcher:
    """
    Match MS fields to known calibrators by position.
    Uses VLA calibrator catalog (XML in NRAO format).
    """
    
    def __init__(self, 
                 xml_path: Optional[str] = None,
                 user_models: Optional[Dict] = None,
                 logger=None):
        """
        Initialize matcher with calibrator database.
        """
        self.logger = logger
        self.calibrators = []
        self.user_models = user_models or {}
        
        # Find XML file
        if xml_path is None:
            data_dir = Path(__file__).parent.parent.parent / "data"
            xml_path = data_dir / "vla_calibrators.xml"
        
        if os.path.exists(xml_path):
            self._load_xml_catalog(str(xml_path))
        else:
            if logger:
                logger.warning(f"Calibrator XML not found: {xml_path}")
    
    def parse_ra_dec(self, ra_str: str, dec_str: str):
        """Parse RA/DEC strings from various formats"""
        if not ra_str or not dec_str:
            return None
        
        try:
            # Clean RA string (handle hms format)
            ra_clean = ra_str.replace("h", " ").replace("m", " ").replace("s", "")
            # Clean DEC string (handle dms format)
            dec_clean = dec_str.replace("d", " ").replace("'", " ").replace('"', " ")
            
            coord = SkyCoord(f"{ra_clean} {dec_clean}", unit=(u.hourangle, u.deg))
            return coord
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not parse coordinates {ra_str}, {dec_str}: {e}")
            return None
    
    def parse_band_wavelength(self, band_str: str) -> Optional[float]:
        """Parse band string to get wavelength in cm"""
        if not band_str:
            return None
        
        try:
            # Extract number and unit (e.g., "21cm", "6cm", "90cm")
            match = re.match(r'([0-9.]+)\s*(cm|mm)', band_str.lower())
            if match:
                value = float(match.group(1))
                unit = match.group(2)
                
                if unit == 'mm':
                    return value / 10.0
                else:
                    return value
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not parse band wavelength: {band_str}: {e}")
        
        return None
    
    def _load_xml_catalog(self, xml_path: str):
        """Load VLA calibrator catalog from XML (NRAO format)"""
        try:
            tree = ET.parse(xml_path)
            root = tree.getroot()
            
            for cal in root.findall('calibrator'):
                j2000 = cal.find('header/j2000')
                if j2000 is not None:
                    name = j2000.findtext('IAU_NAME')
                    ra = j2000.findtext('RA')
                    dec = j2000.findtext('DEC')
                    coord = self.parse_ra_dec(ra, dec)
                    
                    if coord is not None and name:
                        # Extract band information
                        bands = []
                        bands_elem = cal.find('bands')
                        if bands_elem is not None:
                            for band in bands_elem.findall('band'):
                                band_name = band.findtext('BAND', '').strip()
                                uvmin = band.findtext('UVMIN_KLAMBDA', '').strip()
                                uvmax = band.findtext('UVMAX_KLAMBDA', '').strip()
                                
                                wavelength_cm = self.parse_band_wavelength(band_name)
                                
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
        """Check if source is a standard amplitude calibrator"""
        return any(source_name.upper() == amp.upper() for amp in AMP_CAL_STANDARD_NAMES)
    
    def match_source_to_calibrator(self, source_coord, source_name: str, 
                                    threshold_arcsec: float = 60.0):
        """Match source to calibrator by position"""
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
            uvmin_str = closest_band.get('uvmin', '').strip() if closest_band.get('uvmin') else ''
            uvmax_str = closest_band.get('uvmax', '').strip() if closest_band.get('uvmax') else ''
            
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
            
            if uvmin is not None and uvmax is not None:
                return f"{uvmin}~{uvmax}klambda"
            elif uvmin is not None and uvmax is None:
                return f">{uvmin}klambda"
            elif uvmin is None and uvmax is not None:
                return f"<{uvmax}klambda"
        
        return None
    
    def get_user_polcal_model(self, source_name: str) -> Optional[Dict]:
        """Get polcal model from user models by name match"""
        if not self.user_models:
            return None
        
        full_stokes = self.user_models.get('full_stokes', {})
        if source_name in full_stokes:
            return full_stokes[source_name]
        
        return None
    
    def categorize_sources(self, ms_sources: List[Dict], ms_wavelength_cm: float) -> Dict:
        """Categorize MS sources into amp_cal, phase_cal, and source_list"""
        results = {
            "amp_cal": [],
            "phase_cal": [],
            "source_list": [],
            "calibrator_details": {}
        }
        
        for src in ms_sources:
            name = src["name"]
            coord = src["coord"]
            
            if self.is_amp_cal(name):
                results["amp_cal"].append(name)
                if self.logger:
                    self.logger.info(f"Amplitude calibrator detected: {name} (name match)")
                
                # Find matching calibrator for UV range (larger threshold for name matches)
                match = self.match_source_to_calibrator(coord, name, threshold_arcsec=180)
                if match:
                    uvrange = self.get_calibrator_uvrange(match, ms_wavelength_cm)
                    if uvrange:
                        results["calibrator_details"][name] = {"uvrange": uvrange}
                        if self.logger:
                            self.logger.info(f"UV range for {name}: {uvrange}")
            else:
                # Check position match
                match = self.match_source_to_calibrator(coord, name)
                if match:
                    results["phase_cal"].append(name)
                    if self.logger:
                        self.logger.info(f"Phase calibrator detected: {name} (position match to {match['name']})")
                    
                    uvrange = self.get_calibrator_uvrange(match, ms_wavelength_cm)
                    if uvrange:
                        results["calibrator_details"][name] = {"uvrange": uvrange}
                        if self.logger:
                            self.logger.info(f"UV range for {name}: {uvrange}")
                else:
                    results["source_list"].append(name)
                    if self.logger:
                        self.logger.info(f"Science target detected: {name}")
        
        return results


def build_calibration_plan(ms_info: Dict, config, logger) -> Dict[str, Any]:
    """
    Build calibration plan from MS info and config.
    
    Logic:
    1. For each field: name match → amp_cal, position match → phase_cal, else → target
    2. Apply user overrides (always win)
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
    }
    
    overrides = config.calibrator_overrides
    uvrange_overrides = config.uvrange_overrides
    user_models = config.user_models
    
    # Initialize matcher
    matcher = CalibratorMatcher(user_models=user_models, logger=logger)
    
    wavelength_cm = ms_info.get('wavelength_cm', 20.0)
    
    # Build source list from MS fields
    ms_sources = []
    for fid, fdata in ms_info['fields'].items():
        field_name = fdata['name']
        ra_rad = fdata['ra_rad']
        dec_rad = fdata['dec_rad']
        
        coord = SkyCoord(ra=ra_rad * u.radian, dec=dec_rad * u.radian)
        ms_sources.append({"name": field_name, "coord": coord})
    
    # Categorize sources
    results = matcher.categorize_sources(ms_sources, wavelength_cm)
    
    # Set flux cal (first amp cal)
    if results["amp_cal"]:
        cal_plan['flux_cal'] = results["amp_cal"][0]
    
    # Set phase cal (first phase cal, or first amp cal if no phase cals)
    if results["phase_cal"]:
        cal_plan['phase_cal'] = results["phase_cal"][0]
    elif results["amp_cal"]:
        cal_plan['phase_cal'] = results["amp_cal"][0]
    
    # Set targets
    cal_plan['targets'] = results["source_list"]
    
    # Set all calibrators
    cal_plan['all_calibrators'] = results["amp_cal"] + results["phase_cal"]
    
    # Set UV ranges
    for name, details in results["calibrator_details"].items():
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
        amp_override = overrides.get('amp')
        if amp_override:
            if isinstance(amp_override, list):
                cal_plan['flux_cal'] = amp_override[0]
            else:
                cal_plan['flux_cal'] = amp_override
            logger.info(f"User override flux cal: {cal_plan['flux_cal']}")
        
        phase_override = overrides.get('phase')
        if phase_override:
            cal_plan['phase_cal'] = phase_override
            logger.info(f"User override phase cal: {cal_plan['phase_cal']}")
        
        leakage_override = overrides.get('leakage')
        if leakage_override:
            cal_plan['leakage_cal'] = leakage_override
            logger.info(f"User override leakage cal: {cal_plan['leakage_cal']}")
        
        polangle_override = overrides.get('pol_angle')
        if polangle_override:
            cal_plan['polangle_cal'] = polangle_override
            logger.info(f"User override pol angle cal: {cal_plan['polangle_cal']}")
    
    # UV range overrides
    if uvrange_overrides:
        for cal_name, uvrange in uvrange_overrides.items():
            if uvrange:
                cal_plan['calibrator_uvranges'][cal_name] = uvrange
                logger.info(f"User override uvrange for {cal_name}: {uvrange}")
    
    # Rebuild all_calibrators
    all_cals = set()
    for cal in [cal_plan['flux_cal'], cal_plan['phase_cal'], 
                cal_plan['leakage_cal'], cal_plan['polangle_cal']]:
        if cal:
            all_cals.add(cal)
    cal_plan['all_calibrators'] = sorted(list(all_cals))
    
    return cal_plan
