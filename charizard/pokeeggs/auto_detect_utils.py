import os
import yaml
import xml.etree.ElementTree as ET
from astropy.coordinates import SkyCoord
import astropy.units as u
import numpy as np
from casacore import tables
import logging
import re

class CalibratorAutoDetector:
    """
    Automated calibrator identification system for VLA observations
    """
    
    def __init__(self, xml_path=None, logger=None):
        if xml_path is None:
            # Default: look for XML file in the same directory as this module
            current_dir = os.path.dirname(os.path.abspath(__file__))
            xml_path = os.path.join(current_dir, "vla_calibrators_from_web.xml")
        
        self.xml_path = xml_path
        self.logger = logger
        self.calibrators = []
        self.amp_cal_standard_names = [
            "3C147", "3C138", "3C48", "3C84", "3C295", "3C286", 
            "3C196", "3C123", "J1331+3030", "J0521+1638"
        ]
        self.load_calibrator_xml()

    def parse_ra_dec(self, ra_str, dec_str):
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

    def parse_band_wavelength(self, band_str):
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
                    return value / 10.0  # convert mm to cm
                else:  # cm
                    return value
        except Exception as e:
            if self.logger:
                self.logger.warning(f"Could not parse band wavelength: {band_str}: {e}")
            
        return None

    def load_calibrator_xml(self):
        """Load calibrator data from XML file"""
        if not os.path.exists(self.xml_path):
            if self.logger:
                self.logger.warning(f"Calibrator XML file not found: {self.xml_path}")
            return
            
        try:
            tree = ET.parse(self.xml_path)
            root = tree.getroot()
            
            for cal in root.findall('calibrator'):
                j2000 = cal.find('header/j2000')
                if j2000 is not None:
                    name = j2000.findtext('IAU_NAME')
                    ra = j2000.findtext('RA')
                    dec = j2000.findtext('DEC')
                    coord = self.parse_ra_dec(ra, dec)
                    
                    if coord is not None and name:
                        # Extract band information with wavelengths and UV ranges
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
                        
                        if bands:  # Only add calibrators with valid band information
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

    def read_ms_sources(self, ms_path):
        """Read source list and coordinates from MS"""
        sources = []
        
        try:
            # Read source table
            with tables.table(f"{ms_path}/SOURCE") as tb:
                names = tb.getcol('NAME')
                ras = tb.getcol('DIRECTION')[:, 0]  # RA in radians
                decs = tb.getcol('DIRECTION')[:, 1]  # DEC in radians
                
            # Convert to unique sources with coordinates
            unique_sources = {}
            for name, ra, dec in zip(names, ras, decs):
                if name not in unique_sources:
                    coord = SkyCoord(ra=ra*u.radian, dec=dec*u.radian)
                    unique_sources[name] = coord
                    
            sources = [{"name": name, "coord": coord} for name, coord in unique_sources.items()]
            if self.logger:
                self.logger.info(f"Found {len(sources)} unique sources in MS: {[s['name'] for s in sources]}")
            
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error reading MS sources: {e}")
            
        return sources

    def get_ms_central_frequency(self, ms_path):
        """Get central frequency from MS spectral window table"""
        try:
            with tables.table(f"{ms_path}/SPECTRAL_WINDOW") as tb:
                frequencies = tb.getcol('CHAN_FREQ')[0]  # Get first SPW frequencies in Hz
                central_freq = np.mean(frequencies)  # Central frequency in Hz
                
                # Convert to wavelength in cm
                c = 2.998e8  # speed of light in m/s
                wavelength_m = c / central_freq
                wavelength_cm = wavelength_m * 100
                
                # Convert numpy types to Python native types for YAML serialization
                central_freq = float(central_freq)
                wavelength_cm = float(wavelength_cm)
                
                if self.logger:
                    self.logger.info(f"MS central frequency: {central_freq/1e6:.1f} MHz ({wavelength_cm:.1f} cm)")
                return central_freq, wavelength_cm
                
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error reading MS frequency: {e}")
            return None, None

    def find_closest_band(self, calibrator_bands, ms_wavelength_cm):
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
                
        if closest_band and self.logger:
            self.logger.info(f"Closest band: {closest_band['band']} ({closest_band['wavelength_cm']}cm) "
                           f"for MS wavelength {ms_wavelength_cm:.1f}cm (diff: {min_diff:.1f}cm)")
            
        return closest_band

    def get_calibrator_uvrange(self, calibrator, ms_wavelength_cm):
        """Get UV range for calibrator based on closest band to MS frequency"""
        if not calibrator or not calibrator.get('bands'):
            return None
            
        closest_band = self.find_closest_band(calibrator['bands'], ms_wavelength_cm)
        
        if closest_band:
            uvmin_str = closest_band.get('uvmin', '').strip() if closest_band.get('uvmin') else ''
            uvmax_str = closest_band.get('uvmax', '').strip() if closest_band.get('uvmax') else ''
            
            uvmin = None
            uvmax = None
            
            # Try to parse uvmin
            if uvmin_str:
                try:
                    uvmin = float(uvmin_str)
                except ValueError:
                    pass
            
            # Try to parse uvmax  
            if uvmax_str:
                try:
                    uvmax = float(uvmax_str)
                except ValueError:
                    pass
            
            # Generate appropriate UV range constraint
            if uvmin is not None and uvmax is not None:
                # Both min and max exist: range constraint
                return f"{uvmin}~{uvmax}klambda"
            elif uvmin is not None and uvmax is None:
                # Only minimum exists: greater than constraint
                return f">{uvmin}klambda"
            elif uvmin is None and uvmax is not None:
                # Only maximum exists: less than constraint  
                return f"<{uvmax}klambda"
            # If neither exists, fall through to return None
                
        return None

    def is_amp_cal(self, source_name):
        """Check if source is a standard amplitude calibrator"""
        return any(source_name.upper() == amp.upper() for amp in self.amp_cal_standard_names)

    def match_source_to_calibrator(self, source_coord, source_name, threshold_arcsec=60):
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

    def categorize_sources(self, ms_sources, ms_wavelength_cm):
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
                # Standard amplitude calibrator
                results["amp_cal"].append(name)
                if self.logger:
                    self.logger.info(f"Amplitude calibrator detected: {name} (name match)")
                
                # Find matching calibrator in database for UV range
                match = self.match_source_to_calibrator(coord, name, threshold_arcsec=180)  # Larger threshold for name matches
                if match:
                    uvrange = self.get_calibrator_uvrange(match, ms_wavelength_cm)
                    if uvrange:
                        results["calibrator_details"][name] = {"uvrange": uvrange}
                        if self.logger:
                            self.logger.info(f"UV range for {name}: {uvrange}")
                        
            else:
                # Check position match against calibrator database
                match = self.match_source_to_calibrator(coord, name)
                if match:
                    results["phase_cal"].append(name)
                    if self.logger:
                        self.logger.info(f"Phase calibrator detected: {name} (position match to {match['name']})")
                    
                    # Get UV range
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

    def generate_calibrator_file(self, ms_path, output_file=None):
        """Generate calibrator file for given MS"""
        if output_file is None:
            ms_basename = os.path.basename(ms_path).replace('.ms', '')
            output_file = f"{ms_basename}_calibrators.yaml"
            
        # Read MS sources and get central frequency
        ms_sources = self.read_ms_sources(ms_path)
        if not ms_sources:
            if self.logger:
                self.logger.error("No sources found in MS")
            raise ValueError("No sources found in MS")
            
        central_freq, ms_wavelength_cm = self.get_ms_central_frequency(ms_path)
        if central_freq is None:
            if self.logger:
                self.logger.error("Could not determine MS frequency")
            raise ValueError("Could not determine MS frequency")
        
        # Categorize sources
        results = self.categorize_sources(ms_sources, ms_wavelength_cm)
        
        # Check if we found any calibrators
        if not results["amp_cal"] and not results["phase_cal"]:
            if self.logger:
                self.logger.error("No calibrators detected in MS")
            raise ValueError("No calibrators detected in MS")
        
        # Create output structure
        calibrator_data = {
            "amp_cal": ",".join(results["amp_cal"]) if results["amp_cal"] else "",
            "phase_cal": ",".join(results["phase_cal"]) if results["phase_cal"] else "",
            "source_list": ",".join(results["source_list"]) if results["source_list"] else "",
            "ms_frequency_mhz": float(central_freq / 1e6),  # Ensure it's a Python float
            "ms_wavelength_cm": float(ms_wavelength_cm),    # Ensure it's a Python float
            "calibrator_details": results["calibrator_details"]
        }
        
        # Write to file
        try:
            with open(output_file, 'w') as f:
                yaml.dump(calibrator_data, f, default_flow_style=False)
            if self.logger:
                self.logger.info(f"Calibrator file written: {output_file}")
            return output_file
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error writing calibrator file: {e}")
            raise

def auto_detect_calibrators(config, logger):
    """
    Main function to auto-detect calibrators
    Called from setup_battle() when calibrator_auto_detect=True
    """
    if not config['msinfo'].get('calibrator_auto_detect', False):
        logger.info("Calibrator auto-detection disabled")
        return None
        
    logger.info("Starting calibrator auto-detection...")
    
    ms_path = config['msinfo']['parent_ms']
    if not os.path.exists(ms_path):
        logger.error(f"Parent MS not found: {ms_path}")
        raise FileNotFoundError(f"Parent MS not found: {ms_path}")
        
    # Initialize detector
    detector = CalibratorAutoDetector(logger=logger)
    
    # Generate calibrator file
    calibrator_file = detector.generate_calibrator_file(ms_path)
    
    logger.info("Calibrator auto-detection completed successfully")
    return calibrator_file

def get_calibrator_info(config, logger):
    """
    Get calibrator information for use in calibration functions
    Returns the auto-detected values if available, otherwise config values
    """
    if not config['msinfo'].get('calibrator_auto_detect', False):
        # Use config values
        return {
            'amp_cal': config['msinfo'].get('amp_cal', ''),
            'phase_cal': config['msinfo'].get('phase_cal', ''),
            'source_list': config['msinfo'].get('source_list', ''),
            'leakage_cal': config['msinfo'].get('leakage_cal', ''),
            'polang_cal': config['msinfo'].get('polang_cal', ''),
            'calibrator_details': {}
        }
    
    # Auto-detect mode - read from calibrator file
    ms_basename = os.path.basename(config['msinfo']['parent_ms']).replace('.ms', '')
    calibrator_file = f"{ms_basename}_calibrators.yaml"
    
    if not os.path.exists(calibrator_file):
        logger.error(f"Calibrator file not found: {calibrator_file}")
        logger.error("Auto-detection enabled but calibrator file missing. Run auto-detection first.")
        raise FileNotFoundError(f"Calibrator file not found: {calibrator_file}")
        
    try:
        with open(calibrator_file, 'r') as f:
            cal_data = yaml.safe_load(f)
            
        # Always use config values for leakage_cal and polang_cal
        return {
            'amp_cal': cal_data.get('amp_cal', ''),
            'phase_cal': cal_data.get('phase_cal', ''), 
            'source_list': cal_data.get('source_list', ''),
            'leakage_cal': config['msinfo'].get('leakage_cal', ''),
            'polang_cal': config['msinfo'].get('polang_cal', ''),
            'calibrator_details': cal_data.get('calibrator_details', {})
        }
    except Exception as e:
        logger.error(f"Error reading calibrator file: {e}")
        raise