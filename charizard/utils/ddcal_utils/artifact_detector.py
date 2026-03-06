# charizard/utils/ddcal_utils/artifact_detector.py
"""
Enhanced Bright Source Artifact Detection System
Handles multi-component sources properly using PyBDSF classifications
Creates appropriate DS9 regions based on source morphology

Based on Arpan's EnhancedBrightSourceArtifactDetector.
"""

import os
import numpy as np
import pandas as pd
from astropy.io import fits
from astropy.table import Table
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import cdist
from scipy.stats import circstd
import warnings
warnings.filterwarnings('ignore')


class EnhancedBrightSourceArtifactDetector:
    """Detect bright sources with artifact patterns and create morphology-aware regions"""
    
    def __init__(self, catalog_file, mfs_image, mask_file=None, flux_threshold_mJy=50, 
                 eps_factor=5, min_samples=3, mask_radius_factor=2.5, logger=None):
        """
        Initialize enhanced detector
        
        Args:
            catalog_file: PyBDSF catalog FITS file
            mfs_image: FITS image for beam info
            mask_file: PyBDSF island mask FITS file (optional)
            flux_threshold_mJy: Only process clusters with brightest source > this limit
            eps_factor: Clustering radius multiplier (beam_size * eps_factor)
            min_samples: Minimum sources per cluster for DBSCAN
            mask_radius_factor: Radius factor for mask cleaning (default 2.5)
            logger: Optional logger
        """
        self.logger = logger
        self.catalog_df = self.load_catalog(catalog_file)
        self.beam_size = self.get_beam_size(mfs_image)
        self.mask_file = mask_file
        self.flux_threshold = flux_threshold_mJy / 1000.0  # Convert mJy to Jy
        self.eps_factor = eps_factor
        self.min_samples = min_samples
        self.mask_radius_factor = mask_radius_factor
        self.problematic_sources = []
        
        self._log(f"Enhanced Detector Parameters:")
        self._log(f"  Flux threshold: {flux_threshold_mJy} mJy ({self.flux_threshold:.3f} Jy)")
        self._log(f"  Clustering radius: {eps_factor} × beam size")
        self._log(f"  Min cluster size: {min_samples} sources")
        self._log(f"  Mask radius factor: {mask_radius_factor}")
        if mask_file:
            self._log(f"  Mask file: {mask_file}")
    
    def _log(self, msg):
        """Log message using logger or print"""
        if self.logger:
            self.logger.info(msg)
        else:
            print(msg)
    
    def load_catalog(self, catalog_file):
        """Load PyBDSF FITS catalog"""
        self._log(f"Loading catalog: {catalog_file}")
        with fits.open(catalog_file) as hdul:
            for hdu in hdul:
                if hasattr(hdu, 'data') and hdu.data is not None and hasattr(hdu.data, 'dtype'):
                    df = Table(hdu.data).to_pandas()
                    self._log(f"Loaded {len(df)} sources")
                    
                    # Ensure we have flux columns
                    flux_cols = ['Peak_flux', 'Total_flux', 'peak_flux', 'total_flux']
                    flux_col = None
                    for col in flux_cols:
                        if col in df.columns:
                            flux_col = col
                            break
                    
                    if flux_col is None:
                        raise ValueError(f"No flux column found. Available: {list(df.columns)}")
                    
                    df['flux'] = df[flux_col]
                    self._log(f"Using flux column: {flux_col}")
                    self._log(f"Flux range: {df['flux'].min():.6f} to {df['flux'].max():.3f} Jy")
                    
                    # Check for PyBDSF classification columns
                    if 'S_Code' in df.columns:
                        s_codes = df['S_Code'].value_counts()
                        self._log(f"Source types: {dict(s_codes)}")
                    
                    # Check for island/source grouping columns
                    grouping_cols = ['Isl_id', 'Source_id', 'Island_id', 'Src_id']
                    self.grouping_col = None
                    for col in grouping_cols:
                        if col in df.columns:
                            self.grouping_col = col
                            self._log(f"Using grouping column: {col}")
                            break
                    
                    if self.grouping_col is None:
                        self._log("Warning: No island/source grouping column found")
                    
                    return df
        raise ValueError("No table found in catalog")
    
    def get_beam_size(self, mfs_image):
        """Extract beam parameters from MFS FITS image"""
        self._log(f"Reading beam from: {mfs_image}")
        with fits.open(mfs_image) as hdul:
            header = hdul[0].header
            
            bmaj = header.get('BMAJ', 0)
            bmin = header.get('BMIN', 0)
            bpa = header.get('BPA', 0)
            
            if bmaj > 0:
                beam_maj_arcsec = bmaj * 3600
                beam_min_arcsec = bmin * 3600 if bmin > 0 else beam_maj_arcsec
                
                self.beam_maj = beam_maj_arcsec
                self.beam_min = beam_min_arcsec
                self.beam_pa = bpa
                
                self._log(f"Beam: {beam_maj_arcsec:.1f}\" x {beam_min_arcsec:.1f}\" @ {bpa:.1f}°")
                return beam_maj_arcsec
            else:
                self._log("No beam found, using defaults")
                self.beam_maj = 6.0
                self.beam_min = 6.0
                self.beam_pa = 0.0
                return 6.0
    
    def spatial_clustering(self):
        """Perform DBSCAN spatial clustering"""
        self._log("Performing spatial clustering...")
        
        coords = self.catalog_df[['RA', 'DEC']].values
        eps = (self.beam_size / 3600.0) * self.eps_factor
        
        dbscan = DBSCAN(eps=eps, min_samples=self.min_samples)
        cluster_labels = dbscan.fit_predict(coords)
        self.catalog_df['cluster_id'] = cluster_labels
        
        n_clusters = len(set(cluster_labels)) - (1 if -1 in cluster_labels else 0)
        n_isolated = sum(cluster_labels == -1)
        
        self._log(f"Found {n_clusters} clusters, {n_isolated} isolated sources")
        return cluster_labels
    
    def analyze_clusters(self):
        """Analyze each cluster for problematic patterns"""
        problematic_clusters = []
        
        for cluster_id in sorted(self.catalog_df['cluster_id'].unique()):
            if cluster_id == -1:
                continue
                
            cluster_sources = self.catalog_df[self.catalog_df['cluster_id'] == cluster_id]
            if len(cluster_sources) < 3:
                continue
            
            brightest_idx = cluster_sources['flux'].idxmax()
            brightest_source = cluster_sources.loc[brightest_idx]
            brightest_flux_mJy = brightest_source['flux'] * 1000
            
            if brightest_source['flux'] < self.flux_threshold:
                continue
            
            self._log(f"Analyzing cluster {cluster_id}: {len(cluster_sources)} sources, "
                      f"brightest = {brightest_flux_mJy:.1f} mJy")
            
            pattern_analysis = self.classify_cluster_pattern(cluster_sources, brightest_source)
            
            if pattern_analysis['is_problematic']:
                cluster_info = {
                    'cluster_id': cluster_id,
                    'brightest_source_idx': brightest_idx,
                    'brightest_ra': brightest_source['RA'],
                    'brightest_dec': brightest_source['DEC'],
                    'brightest_flux_mJy': brightest_flux_mJy,
                    'pattern_type': pattern_analysis['pattern_type'],
                    'confidence': pattern_analysis['confidence'],
                    'n_sources': len(cluster_sources),
                    'artifact_indices': [idx for idx in cluster_sources.index if idx != brightest_idx]
                }
                problematic_clusters.append(cluster_info)
                
                self._log(f"  → PROBLEMATIC: {pattern_analysis['pattern_type']} "
                          f"(confidence: {pattern_analysis['confidence']:.2f})")
        
        self.problematic_sources = problematic_clusters
        self._log(f"Found {len(problematic_clusters)} problematic bright sources")
        return problematic_clusters
    
    def classify_cluster_pattern(self, cluster_sources, brightest_source):
        """Classify if cluster shows artifact patterns around bright source"""
        
        coords = cluster_sources[['RA', 'DEC']].values
        fluxes = cluster_sources['flux'].values
        
        bright_coord = np.array([brightest_source['RA'], brightest_source['DEC']])
        distances = cdist(coords, [bright_coord]).flatten()
        angles = np.arctan2(coords[:, 1] - bright_coord[1], coords[:, 0] - bright_coord[0])
        
        mask = distances > 0
        if np.sum(mask) < 2:
            return {'is_problematic': False, 'pattern_type': 'insufficient_sources', 'confidence': 0.0}
        
        satellite_distances = distances[mask]
        satellite_fluxes = fluxes[mask]
        satellite_angles = angles[mask]
        
        patterns = []
        
        # 1. Sidelobe ring detection
        sidelobe_confidence = self.detect_sidelobe_pattern(
            satellite_distances, satellite_fluxes, satellite_angles, brightest_source['flux']
        )
        if sidelobe_confidence > 0.5:
            patterns.append(('sidelobe_ring', sidelobe_confidence))
        
        # 2. Calibration spike detection
        spike_confidence = self.detect_calibration_spikes(satellite_angles, satellite_distances)
        if spike_confidence > 0.5:
            patterns.append(('calibration_spikes', spike_confidence))
        
        # 3. RFI streak detection
        rfi_confidence = self.detect_rfi_pattern(coords[mask], satellite_fluxes)
        if rfi_confidence > 0.5:
            patterns.append(('rfi_streak', rfi_confidence))
        
        # 4. Dynamic range artifacts
        dynamic_confidence = self.detect_dynamic_range_artifacts(
            satellite_distances, satellite_fluxes, brightest_source['flux']
        )
        if dynamic_confidence > 0.5:
            patterns.append(('dynamic_range', dynamic_confidence))
        
        if patterns:
            best_pattern = max(patterns, key=lambda x: x[1])
            return {
                'is_problematic': True,
                'pattern_type': best_pattern[0],
                'confidence': best_pattern[1]
            }
        else:
            return {
                'is_problematic': False,
                'pattern_type': 'no_pattern',
                'confidence': 0.0
            }
    
    def detect_sidelobe_pattern(self, distances, fluxes, angles, central_flux):
        """Detect sidelobe ring pattern"""
        if len(distances) < 3:
            return 0.0
        
        confidence = 0.0
        
        # Check 1: Flux decreases with distance
        if len(distances) > 2:
            flux_distance_corr = np.corrcoef(distances, fluxes)[0, 1]
            if flux_distance_corr < -0.2:
                confidence += 0.3
        
        # Check 2: Sources distributed around center
        angle_std = circstd(angles)
        if angle_std > 1.0:
            confidence += 0.3
        
        # Check 3: Central source much brighter
        flux_ratio = central_flux / fluxes.max()
        if flux_ratio > 3:
            confidence += 0.2
        
        # Check 4: Multiple rings/distances
        if len(np.unique(np.round(distances, 3))) > 1:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def detect_calibration_spikes(self, angles, distances):
        """Detect symmetric calibration spike pattern"""
        if len(angles) < 4:
            return 0.0
        
        confidence = 0.0
        
        # 4-fold symmetry
        angles_normalized = (angles % (np.pi/2)) * 2/np.pi
        angle_bins = np.histogram(angles_normalized, bins=4)[0]
        if angle_bins.std() / (angle_bins.mean() + 1e-6) < 0.8:
            confidence += 0.4
        
        # 8-fold symmetry
        angles_8fold = (angles % (np.pi/4)) * 4/np.pi
        angle_bins_8 = np.histogram(angles_8fold, bins=4)[0]
        if angle_bins_8.std() / (angle_bins_8.mean() + 1e-6) < 0.8:
            confidence += 0.4
        
        # Similar distances
        distance_cv = distances.std() / distances.mean() if distances.mean() > 0 else 1
        if distance_cv < 0.5:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def detect_rfi_pattern(self, coords, fluxes):
        """Detect RFI linear streak pattern"""
        if len(coords) < 3:
            return 0.0
        
        confidence = 0.0
        
        x, y = coords[:, 0], coords[:, 1]
        try:
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            r_squared = 1 - (np.sum((y - p(x))**2) / np.sum((y - y.mean())**2))
            if r_squared > 0.8:
                confidence += 0.5
        except:
            pass
        
        flux_cv = fluxes.std() / fluxes.mean() if fluxes.mean() > 0 else 1
        if flux_cv < 0.3:
            confidence += 0.3
        
        coord_range_x = coords[:, 0].max() - coords[:, 0].min()
        coord_range_y = coords[:, 1].max() - coords[:, 1].min()
        aspect_ratio = max(coord_range_x, coord_range_y) / (min(coord_range_x, coord_range_y) + 1e-6)
        if aspect_ratio > 3:
            confidence += 0.2
        
        return min(confidence, 1.0)
    
    def detect_dynamic_range_artifacts(self, distances, fluxes, central_flux):
        """Detect dynamic range limitation artifacts"""
        if len(distances) < 3:
            return 0.0
        
        confidence = 0.0
        
        try:
            log_dist = np.log(distances + 1e-6)
            log_flux = np.log(fluxes + 1e-6)
            slope = np.polyfit(log_dist, log_flux, 1)[0]
            if slope < -0.5:
                confidence += 0.4
        except:
            pass
        
        max_satellite_flux = fluxes.max()
        flux_ratio = max_satellite_flux / central_flux
        if 0.01 < flux_ratio < 0.1:
            confidence += 0.3
        
        if len(distances) > 3:
            confidence += 0.3
        
        return min(confidence, 1.0)
    
    def create_ds9_regions(self, output_file="problematic_bright_sources.reg"):
        """Create DS9 region file with circular regions"""
        
        if not self.problematic_sources:
            self._log("No problematic sources found - no region file created")
            return None
        
        with open(output_file, 'w') as f:
            f.write("# Region file format: DS9 version 4.1\n")
            f.write("global color=red dashlist=8 3 width=3 font=\"helvetica 12 bold roman\" ")
            f.write("select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n")
            f.write("fk5\n")
            
            for source in self.problematic_sources:
                ra = source['brightest_ra']
                dec = source['brightest_dec']
                
                # Fix RA coordinate: add 360 degrees
                corrected_ra = 24 * 15 + ra  # 360 + ra
                
                circle_radius = self.mask_radius_factor * self.beam_maj
                f.write(f'circle({corrected_ra:.7f},{dec:.7f},{circle_radius:.1f}")\n')
        
        self._log(f"Created DS9 region file: {output_file}")
        self._log(f"Contains {len(self.problematic_sources)} circular regions")
        
        return output_file
    
    def create_cleaned_mask(self, output_file=None):
        """Create cleaned mask FITS file with artifacts removed"""
        
        if not self.mask_file:
            self._log("No mask file provided - skipping mask cleaning")
            return None
        
        if not self.problematic_sources:
            self._log("No problematic sources found - no mask cleaning needed")
            return None
        
        if output_file is None:
            base = os.path.splitext(self.mask_file)[0]
            output_file = f"{base}_cleaned.fits"
        
        self._log(f"Creating cleaned mask from: {self.mask_file}")
        
        with fits.open(self.mask_file) as hdul:
            mask_data = hdul[0].data.copy()
            header = hdul[0].header.copy()
        
        if mask_data.ndim == 4:
            mask_2d = mask_data[0, 0, :, :].copy()
        else:
            mask_2d = mask_data.copy()
        
        ny, nx = mask_2d.shape
        
        crval1 = header.get('CRVAL1', 0)
        crval2 = header.get('CRVAL2', 0)
        crpix1 = header.get('CRPIX1', 1)
        crpix2 = header.get('CRPIX2', 1)
        cdelt1 = header.get('CDELT1', 1)
        cdelt2 = header.get('CDELT2', 1)
        
        self._log(f"Processing {len(self.problematic_sources)} problematic clusters...")
        
        for source in self.problematic_sources:
            ra_brightest = source['brightest_ra']
            dec_brightest = source['brightest_dec']
            cluster_id = source['cluster_id']
            
            cluster_sources = self.catalog_df[self.catalog_df['cluster_id'] == cluster_id]
            
            x_brightest = (ra_brightest - crval1) / cdelt1 + crpix1 - 1
            y_brightest = (dec_brightest - crval2) / cdelt2 + crpix2 - 1
            
            # Remove ALL sources in this cluster from mask
            for _, cluster_source in cluster_sources.iterrows():
                source_ra = cluster_source['RA']
                source_dec = cluster_source['DEC']
                
                x_source = (source_ra - crval1) / cdelt1 + crpix1 - 1
                y_source = (source_dec - crval2) / cdelt2 + crpix2 - 1
                
                maj_deg = cluster_source.get('Maj', self.beam_maj / 3600.0)
                min_deg = cluster_source.get('Min', self.beam_min / 3600.0)
                pa_deg = cluster_source.get('PA', 0.0)
                
                maj_pix = maj_deg * 3600.0 / abs(cdelt1 * 3600)
                min_pix = min_deg * 3600.0 / abs(cdelt1 * 3600)
                pa_rad = np.radians(pa_deg)
                
                if 0 <= x_source < nx and 0 <= y_source < ny:
                    yy, xx = np.ogrid[:ny, :nx]
                    
                    cos_pa = np.cos(pa_rad)
                    sin_pa = np.sin(pa_rad)
                    
                    dx = xx - x_source
                    dy = yy - y_source
                    
                    x_rot = cos_pa * dx + sin_pa * dy
                    y_rot = -sin_pa * dx + cos_pa * dy
                    
                    ellipse_mask = (x_rot / maj_pix)**2 + (y_rot / min_pix)**2 <= 1.0
                    mask_2d[ellipse_mask] = 0
            
            # Add back circular region around ONLY the brightest source
            circle_radius_pix = self.mask_radius_factor * self.beam_maj / abs(cdelt1 * 3600)
            
            yy, xx = np.ogrid[:ny, :nx]
            circle_mask = (xx - x_brightest)**2 + (yy - y_brightest)**2 <= circle_radius_pix**2
            mask_2d[circle_mask] = 1
        
        if mask_data.ndim == 4:
            mask_data[0, 0, :, :] = mask_2d
        else:
            mask_data[:, :] = mask_2d
        
        header['HISTORY'] = f'Cleaned by EnhancedBrightSourceArtifactDetector'
        header['HISTORY'] = f'Removed artifacts from {len(self.problematic_sources)} bright sources'
        
        fits.writeto(output_file, mask_data, header, overwrite=True)
        self._log(f"Cleaned mask saved: {output_file}")
        
        return output_file
    
    def save_results(self, output_dir="."):
        """Save analysis results"""
        
        if self.problematic_sources:
            results_data = []
            for source in self.problematic_sources:
                base_info = source.copy()
                base_info['region_radius_arcsec'] = self.mask_radius_factor * self.beam_maj
                base_info['region_shape'] = 'circle'
                results_data.append(base_info)
            
            prob_df = pd.DataFrame(results_data)
            prob_file = os.path.join(output_dir, 'problematic_sources.csv')
            prob_df.to_csv(prob_file, index=False)
            self._log(f"Saved {prob_file}")
        
        catalog_file = os.path.join(output_dir, 'catalog_with_clusters.csv')
        self.catalog_df.to_csv(catalog_file, index=False)
        self._log(f"Saved {catalog_file}")
    
    def run_analysis(self):
        """Complete enhanced analysis pipeline"""
        self._log("Starting enhanced bright source artifact detection...")
        
        self.spatial_clustering()
        self.analyze_clusters()
        self.create_ds9_regions()
        self.create_cleaned_mask()
        self.save_results()
        
        self._log("Enhanced analysis complete!")
        return self.problematic_sources


# Wrapper functions for charizard integration

def detect_artifacts(catalog_file: str, mfs_image: str,
                     flux_threshold_mJy: float = 50,
                     eps_factor: float = 5,
                     min_samples: int = 3,
                     mask_radius_factor: float = 2.5,
                     mask_file: str = None,
                     logger=None):
    """
    Detect bright sources with artifacts.
    
    Returns:
        List of problematic source dicts
    """
    detector = EnhancedBrightSourceArtifactDetector(
        catalog_file=catalog_file,
        mfs_image=mfs_image,
        mask_file=mask_file,
        flux_threshold_mJy=flux_threshold_mJy,
        eps_factor=eps_factor,
        min_samples=min_samples,
        mask_radius_factor=mask_radius_factor,
        logger=logger
    )
    
    detector.spatial_clustering()
    return detector.analyze_clusters()


def create_ds9_regions(sources, output_file, beam_maj=6.0, mask_radius_factor=2.5):
    """Create DS9 region file from detected sources."""
    
    if not sources:
        return None
    
    with open(output_file, 'w') as f:
        f.write("# Region file format: DS9 version 4.1\n")
        f.write("global color=red dashlist=8 3 width=3 font=\"helvetica 12 bold roman\" ")
        f.write("select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n")
        f.write("fk5\n")
        
        for source in sources:
            ra = source['brightest_ra']
            dec = source['brightest_dec']
            corrected_ra = 360 + ra
            circle_radius = mask_radius_factor * beam_maj
            f.write(f'circle({corrected_ra:.7f},{dec:.7f},{circle_radius:.1f}")\n')
    
    return output_file


def get_sources_for_peeling(sources):
    """Get sources sorted by flux for sequential peeling (brightest first)."""
    return sorted(sources, key=lambda x: x['brightest_flux_mJy'], reverse=True)
