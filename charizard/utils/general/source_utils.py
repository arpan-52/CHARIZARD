# charizard/utils/general/source_utils.py
"""
Source and calibrator utilities.

Logic:
1. amp_cal = name match to known flux cals (3C286, 3C48, etc)
2. phase_cal = position match to VLA calibrator catalog  
3. User overrides for leakage_cal, polangle_cal
4. all_calibrators = amp + phase + leakage + polangle (all that are in MS)
5. targets = everything else
"""

import os
import re
import yaml
import xml.etree.ElementTree as ET
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
    """Match MS fields to known calibrators by position."""
    
    def __init__(self, xml_path: Optional[str] = None, user_models: Optional[Dict] = None, logger=None):
        self.logger = logger
        self.calibrators = []
        self.user_models = user_models or {}
        
        if xml_path is None:
            data_dir = Path(__file__).parent.parent.parent / "data"
            xml_path = data_dir / "vla_calibrators.xml"
        
        if os.path.exists(xml_path):
            self._load_xml_catalog(str(xml_path))
        elif logger:
            logger.warning(f"Calibrator XML not found: {xml_path}")
    
    def parse_ra_dec(self, ra_str: str, dec_str: str):
        """Parse RA/DEC strings"""
        if not ra_str or not dec_str:
            return None
        try:
            ra_clean = ra_str.replace("h", " ").replace("m", " ").replace("s", "")
            dec_clean = dec_str.replace("d", " ").replace("'", " ").replace('"', " ")
            return SkyCoord(f"{ra_clean} {dec_clean}", unit=(u.hourangle, u.deg))
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not parse coordinates {ra_str}, {dec_str}: {e}")
            return None
    
    def parse_band_wavelength(self, band_str: str) -> Optional[float]:
        """Parse band string to wavelength in cm"""
        if not band_str:
            return None
        try:
            match = re.match(r'([0-9.]+)\s*(cm|mm)', band_str.lower())
            if match:
                value = float(match.group(1))
                return value / 10.0 if match.group(2) == 'mm' else value
        except:
            pass
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
                        bands = []
                        bands_elem = cal.find('bands')
                        if bands_elem is not None:
                            for band in bands_elem.findall('band'):
                                band_name = band.findtext('BAND', '').strip()
                                uvmin = band.findtext('UVMIN_KLAMBDA', '').strip()
                                uvmax = band.findtext('UVMAX_KLAMBDA', '').strip()
                                wavelength_cm = self.parse_band_wavelength(band_name)
                                
                                if wavelength_cm is not None:
                                    bands.append({
                                        'band': band_name,
                                        'wavelength_cm': wavelength_cm,
                                        'uvmin': uvmin if uvmin else None,
                                        'uvmax': uvmax if uvmax else None
                                    })
                        
                        if bands:
                            self.calibrators.append({"name": name, "coord": coord, "bands": bands})
            
            if self.logger:
                self.logger.info(f"Loaded {len(self.calibrators)} calibrators from XML")
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error loading calibrator XML: {e}")
    
    def is_amp_cal(self, source_name: str) -> bool:
        """Check if source is a standard amplitude calibrator"""
        return any(source_name.upper() == amp.upper() for amp in AMP_CAL_STANDARD_NAMES)
    
    def match_source_to_calibrator(self, source_coord, source_name: str, threshold_arcsec: float = 60.0):
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
        """Get UV range for calibrator based on closest band"""
        if not calibrator or not calibrator.get('bands'):
            return None
        
        closest_band = self.find_closest_band(calibrator['bands'], ms_wavelength_cm)
        if closest_band:
            uvmin_str = closest_band.get('uvmin', '') or ''
            uvmax_str = closest_band.get('uvmax', '') or ''
            
            uvmin = float(uvmin_str) if uvmin_str else None
            uvmax = float(uvmax_str) if uvmax_str else None
            
            if uvmin is not None and uvmax is not None:
                return f"{uvmin}~{uvmax}klambda"
            elif uvmax is not None:
                return f"<{uvmax}klambda"
            elif uvmin is not None:
                return f">{uvmin}klambda"
        return None
    
    def get_user_polcal_model(self, source_name: str) -> Optional[Dict]:
        """Get polcal model from user models by name.
        
        Checks multiple possible locations in user models:
        - full_stokes.<source_name>
        - polcal_models.<source_name>
        - <source_name> directly
        """
        if not self.user_models:
            return None
        
        # Try full_stokes first
        full_stokes = self.user_models.get('full_stokes', {})
        if source_name in full_stokes:
            return full_stokes[source_name]
        
        # Try polcal_models
        polcal_models = self.user_models.get('polcal_models', {})
        if source_name in polcal_models:
            return polcal_models[source_name]
        
        # Try direct
        if source_name in self.user_models:
            return self.user_models[source_name]
        
        return None


def build_calibration_plan(ms_info: Dict, config, logger) -> Dict[str, Any]:
    """
    Build calibration plan from MS info and config.
    
    Returns cal_plan with:
    - flux_cal, phase_cal, leakage_cal, polangle_cal
    - all_calibrators (for cal.ms split)
    - targets (for src.ms split)
    - calibrator_uvranges
    - polcal_models
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
    
    matcher = CalibratorMatcher(user_models=user_models, logger=logger)
    wavelength_cm = ms_info.get('wavelength_cm', 20.0)
    
    # Get all field names in MS
    ms_field_names = [fdata['name'] for fdata in ms_info['fields'].values()]
    
    # Categorize sources
    amp_cals = []
    phase_cals = []
    targets = []
    calibrator_details = {}
    
    for fid, fdata in ms_info['fields'].items():
        field_name = fdata['name']
        coord = SkyCoord(ra=fdata['ra_rad'] * u.radian, dec=fdata['dec_rad'] * u.radian)
        
        if matcher.is_amp_cal(field_name):
            amp_cals.append(field_name)
            logger.info(f"Amplitude calibrator detected: {field_name} (name match)")
            
            match = matcher.match_source_to_calibrator(coord, field_name, threshold_arcsec=180)
            if match:
                uvrange = matcher.get_calibrator_uvrange(match, wavelength_cm)
                if uvrange:
                    calibrator_details[field_name] = {'uvrange': uvrange}
                    logger.info(f"UV range for {field_name}: {uvrange}")
        else:
            match = matcher.match_source_to_calibrator(coord, field_name, threshold_arcsec=60)
            if match:
                phase_cals.append(field_name)
                logger.info(f"Phase calibrator detected: {field_name} (position match to {match['name']})")
                
                uvrange = matcher.get_calibrator_uvrange(match, wavelength_cm)
                if uvrange:
                    calibrator_details[field_name] = {'uvrange': uvrange}
                    logger.info(f"UV range for {field_name}: {uvrange}")
            else:
                targets.append(field_name)
                logger.info(f"Science target detected: {field_name}")
    
    # Set detected calibrators
    cal_plan['flux_cal'] = ",".join(amp_cals) if amp_cals else None
    cal_plan['phase_cal'] = ",".join(phase_cals) if phase_cals else None
    
    # =========================================================================
    # APPLY USER OVERRIDES
    # =========================================================================
    if overrides:
        amp_override = overrides.get('amp')
        if amp_override:
            if isinstance(amp_override, list):
                cal_plan['flux_cal'] = ",".join(amp_override)
            else:
                cal_plan['flux_cal'] = amp_override
            logger.info(f"User override flux cal: {cal_plan['flux_cal']}")
        
        phase_override = overrides.get('phase')
        if phase_override:
            cal_plan['phase_cal'] = phase_override
            logger.info(f"User override phase cal: {cal_plan['phase_cal']}")
        
        leakage_override = overrides.get('leakage')
        if leakage_override:
            # Only add if it's actually in the MS
            if leakage_override in ms_field_names:
                cal_plan['leakage_cal'] = leakage_override
                logger.info(f"User override leakage cal: {cal_plan['leakage_cal']}")
            else:
                logger.warning(f"Leakage cal {leakage_override} not in MS fields!")
        
        polangle_override = overrides.get('pol_angle')
        if polangle_override:
            # Only add if it's actually in the MS
            if polangle_override in ms_field_names:
                cal_plan['polangle_cal'] = polangle_override
                logger.info(f"User override pol angle cal: {cal_plan['polangle_cal']}")
            else:
                logger.warning(f"Pol angle cal {polangle_override} not in MS fields!")
    
    # UV range overrides
    if uvrange_overrides:
        for cal_name, uvrange in uvrange_overrides.items():
            if uvrange:
                calibrator_details[cal_name] = {'uvrange': uvrange}
                logger.info(f"User override uvrange for {cal_name}: {uvrange}")
    
    cal_plan['calibrator_uvranges'] = {k: v['uvrange'] for k, v in calibrator_details.items() if 'uvrange' in v}
    
    # =========================================================================
    # BUILD all_calibrators (for cal.ms split)
    # = amp_cals + phase_cals + leakage_cal + polangle_cal
    # =========================================================================
    all_cals = set()
    
    if cal_plan['flux_cal']:
        for c in cal_plan['flux_cal'].split(','):
            all_cals.add(c.strip())
    
    if cal_plan['phase_cal']:
        for c in cal_plan['phase_cal'].split(','):
            all_cals.add(c.strip())
    
    if cal_plan['leakage_cal']:
        all_cals.add(cal_plan['leakage_cal'])
    
    if cal_plan['polangle_cal']:
        all_cals.add(cal_plan['polangle_cal'])
    
    cal_plan['all_calibrators'] = sorted(list(all_cals))
    
    # =========================================================================
    # BUILD targets (for src.ms split)
    # = everything NOT in all_calibrators
    # =========================================================================
    cal_plan['targets'] = [t for t in targets if t not in all_cals]
    
    # =========================================================================
    # POLCAL MODELS (from user models)
    # =========================================================================
    for cal in cal_plan['all_calibrators']:
        model = matcher.get_user_polcal_model(cal)
        if model:
            cal_plan['polcal_models'][cal] = model
            logger.info(f"User polcal model found for {cal}")
    
    return cal_plan
