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
        self.internal_models = {}
        self.user_models = user_models or {}
        
        # Data directory for internal files
        data_dir = Path(__file__).parent.parent.parent / "data"
        
        # Load internal models FIRST (defaults)
        internal_models_path = data_dir / "internal_models.yaml"
        if os.path.exists(internal_models_path):
            self._load_internal_models(str(internal_models_path))
        elif logger:
            logger.warning(f"Internal models not found: {internal_models_path}")
        
        # Load VLA calibrator XML catalog
        if xml_path is None:
            xml_path = data_dir / "vla_calibrators.xml"
        
        if os.path.exists(xml_path):
            self._load_xml_catalog(str(xml_path))
        elif logger:
            logger.warning(f"Calibrator XML not found: {xml_path}")
    
    def _load_internal_models(self, yaml_path: str):
        """Load internal calibrator models from YAML."""
        try:
            with open(yaml_path, 'r') as f:
                self.internal_models = yaml.safe_load(f) or {}
            if self.logger:
                polcal_count = len(self.internal_models.get('polcal_models', {}))
                unpol_count = len(self.internal_models.get('known_unpolarized', {}))
                pol_count = len(self.internal_models.get('known_polarized', {}))
                self.logger.info(f"Loaded internal models: {polcal_count} polcal, {unpol_count} unpolarized, {pol_count} polarized")
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Error loading internal models: {e}")
            self.internal_models = {}
    
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
    
    def get_polcal_model(self, source_name: str) -> Optional[Dict]:
        """Get polcal model for a source.
        
        Checks internal models first, then user models can override.
        
        Search order:
        1. User models (override): full_stokes.<name>, polcal_models.<name>, <name>
        2. Internal models: polcal_models.<name>
        """
        # Check user models first (they override internal)
        if self.user_models:
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
        
        # Fall back to internal models
        if self.internal_models:
            polcal_models = self.internal_models.get('polcal_models', {})
            if source_name in polcal_models:
                return polcal_models[source_name]
        
        return None
    
    def is_known_unpolarized(self, source_name: str) -> bool:
        """Check if source is in known_unpolarized list."""
        # Check user models first
        if self.user_models:
            known_unpol = self.user_models.get('known_unpolarized', {})
            if source_name in known_unpol:
                return True
        
        # Check internal models
        if self.internal_models:
            known_unpol = self.internal_models.get('known_unpolarized', {})
            if source_name in known_unpol:
                return True
        
        return False
    
    def is_known_polarized(self, source_name: str) -> bool:
        """Check if source is in known_polarized list."""
        # Check user models first
        if self.user_models:
            known_pol = self.user_models.get('known_polarized', {})
            if source_name in known_pol:
                return True
        
        # Check internal models
        if self.internal_models:
            known_pol = self.internal_models.get('known_polarized', {})
            if source_name in known_pol:
                return True
        
        return False
    
    def has_full_stokes_model(self, source_name: str) -> bool:
        """Check if source has a full Stokes (I, Q, U, V) model for setjy.

        A full Stokes model must have polarization_fraction and polarization_angle
        defined (for polcal sources like 3C286), OR be explicitly in full_stokes dict.

        This determines if setjy can be run with full polarization info.
        """
        # Check user models for full_stokes dict first
        if self.user_models:
            full_stokes = self.user_models.get('full_stokes', {})
            if source_name in full_stokes:
                return True

        # Check polcal_models for full Stokes info
        model = self.get_polcal_model(source_name)
        if model:
            # Model must have polarization info to be full Stokes
            has_pol_frac = 'polarization_fraction' in model or 'pol_frac' in model
            has_pol_angle = 'polarization_angle' in model or 'pol_angle' in model
            if has_pol_frac and has_pol_angle:
                return True

        return False

    def get_leakage_cal_status(self, source_name: str) -> str:
        """Get polarization status of leakage calibrator.

        Returns:
            'unpolarized' - source is in known_unpolarized
            'polarized' - source is in known_polarized
            'assumed_unpolarized' - source not in either list, assuming unpolarized
        """
        if self.is_known_unpolarized(source_name):
            return 'unpolarized'
        elif self.is_known_polarized(source_name):
            return 'polarized'
        else:
            return 'assumed_unpolarized'


def build_calibration_plan(ms_info: Dict, config, logger) -> Dict[str, Any]:
    """
    Build calibration plan from MS info and config.

    Returns cal_plan with:
    - flux_cal, phase_cal, leakage_cal, polangle_cal
    - all_calibrators (for cal.ms split)
    - targets (for src.ms split)
    - calibrator_uvranges
    - polcal_models
    - pol_basis ('circular' or 'linear')
    - gain_calibrators (for linear feeds: excludes polarized cals without models)
    - polangle_has_full_stokes_model (bool)
    """

    cal_plan = {
        'flux_cal': None,
        'phase_cal': None,
        'leakage_cal': None,
        'polangle_cal': None,
        'targets': [],
        'all_calibrators': [],
        'gain_calibrators': [],  # For linear feeds: cals to use in gain cal
        'calibrator_uvranges': {},
        'polcal_models': {},
        'pol_basis': ms_info.get('pol_basis', 'circular'),
        'polangle_has_full_stokes_model': False,
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
            if isinstance(phase_override, list):
                cal_plan['phase_cal'] = ",".join(phase_override)
            else:
                cal_plan['phase_cal'] = phase_override
            logger.info(f"User override phase cal: {cal_plan['phase_cal']}")
        
        leakage_override = overrides.get('leakage')
        if leakage_override:
            if isinstance(leakage_override, list):
                leakage_override = ",".join(leakage_override)
            # Only add if it's actually in the MS
            if leakage_override in ms_field_names:
                cal_plan['leakage_cal'] = leakage_override
                logger.info(f"User override leakage cal: {cal_plan['leakage_cal']}")
            else:
                logger.warning(f"Leakage cal {leakage_override} not in MS fields!")
        
        polangle_override = overrides.get('pol_angle')
        if polangle_override:
            if isinstance(polangle_override, list):
                polangle_override = ",".join(polangle_override)
            # Only add if it's actually in the MS
            if polangle_override in ms_field_names:
                cal_plan['polangle_cal'] = polangle_override
                logger.info(f"User override pol angle cal: {cal_plan['polangle_cal']}")
            else:
                logger.warning(f"Pol angle cal {polangle_override} not in MS fields!")
        
        targets_override = overrides.get('targets')
        if targets_override:
            if isinstance(targets_override, list):
                targets = targets_override
            else:
                targets = [t.strip() for t in targets_override.split(',')]
            logger.info(f"User override targets: {targets}")
    
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
    # POLCAL MODELS (from internal + user models)
    # =========================================================================
    for cal in cal_plan['all_calibrators']:
        model = matcher.get_polcal_model(cal)
        if model:
            cal_plan['polcal_models'][cal] = model
            logger.info(f"Polcal model found for {cal}")

    # =========================================================================
    # CHECK IF POLANGLE CAL HAS FULL STOKES MODEL
    # =========================================================================
    if cal_plan['polangle_cal']:
        has_full_stokes = matcher.has_full_stokes_model(cal_plan['polangle_cal'])
        cal_plan['polangle_has_full_stokes_model'] = has_full_stokes
        if has_full_stokes:
            logger.info(f"Pol angle cal {cal_plan['polangle_cal']}: has full Stokes model (can use for gain)")
        else:
            logger.info(f"Pol angle cal {cal_plan['polangle_cal']}: no full Stokes model")

    # =========================================================================
    # BUILD GAIN CALIBRATORS LIST
    # For linear feeds: only include polarized sources if they have full Stokes model
    # For circular feeds: use all calibrators
    # =========================================================================
    pol_basis = cal_plan['pol_basis']
    gain_cals = set()

    # Always include flux cal and phase cal (typically unpolarized or have models)
    if cal_plan['flux_cal']:
        for c in cal_plan['flux_cal'].split(','):
            cal_name = c.strip()
            if pol_basis == 'linear':
                # For linear: only include if unpolarized OR has full Stokes model
                if matcher.is_known_unpolarized(cal_name) or matcher.has_full_stokes_model(cal_name):
                    gain_cals.add(cal_name)
                elif not matcher.is_known_polarized(cal_name):
                    # Unknown polarization status - assume ok for gain
                    gain_cals.add(cal_name)
                else:
                    logger.warning(f"Linear feeds: excluding {cal_name} from gain cal (polarized, no model)")
            else:
                gain_cals.add(cal_name)

    if cal_plan['phase_cal']:
        for c in cal_plan['phase_cal'].split(','):
            cal_name = c.strip()
            if pol_basis == 'linear':
                if matcher.is_known_unpolarized(cal_name) or matcher.has_full_stokes_model(cal_name):
                    gain_cals.add(cal_name)
                elif not matcher.is_known_polarized(cal_name):
                    gain_cals.add(cal_name)
                else:
                    logger.warning(f"Linear feeds: excluding {cal_name} from gain cal (polarized, no model)")
            else:
                gain_cals.add(cal_name)

    # For polangle_cal: include in gain only if has full Stokes model (both feed types)
    if cal_plan['polangle_cal'] and cal_plan['polangle_has_full_stokes_model']:
        gain_cals.add(cal_plan['polangle_cal'])
        logger.info(f"Including {cal_plan['polangle_cal']} in gain cal (has full Stokes model)")

    cal_plan['gain_calibrators'] = sorted(list(gain_cals))
    logger.info(f"Gain calibrators: {cal_plan['gain_calibrators']}")

    # =========================================================================
    # LEAKAGE CALIBRATOR STATUS
    # =========================================================================
    if cal_plan['leakage_cal']:
        leakage_status = matcher.get_leakage_cal_status(cal_plan['leakage_cal'])
        cal_plan['leakage_cal_status'] = leakage_status
        if leakage_status == 'unpolarized':
            logger.info(f"Leakage cal {cal_plan['leakage_cal']}: known unpolarized source")
        elif leakage_status == 'polarized':
            logger.info(f"Leakage cal {cal_plan['leakage_cal']}: known polarized source")
        else:
            logger.info(f"Leakage cal {cal_plan['leakage_cal']}: assuming unpolarized (not in catalog)")

    return cal_plan
