# charizard/utils/ddcal_utils/artifact_detector.py
"""
Enhanced Bright Source Artifact Detection.
Finds bright sources with sidelobe/calibration artifacts that need peeling.
Creates DS9 region files for crystalball.

Based on Arpan's EnhancedBrightSourceArtifactDetector.
"""

import os
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from astropy.io import fits
from astropy.table import Table
from sklearn.cluster import DBSCAN
from scipy.spatial.distance import cdist
from scipy.stats import circstd
import warnings
warnings.filterwarnings('ignore')


class BrightSourceArtifactDetector:
    """Detect bright sources with artifact patterns and create DS9 regions."""
    
    def __init__(self, catalog_file: str, mfs_image: str,
                 flux_threshold_mJy: float = 50,
                 eps_factor: float = 5,
                 min_samples: int = 3,
                 mask_radius_factor: float = 2.5):
        """
        Initialize detector.
        
        Args:
            catalog_file: PyBDSF catalog FITS file
            mfs_image: FITS image for beam info
            flux_threshold_mJy: Only process sources brighter than this
            eps_factor: Clustering radius = eps_factor × beam_size
            min_samples: Minimum sources per cluster for DBSCAN
            mask_radius_factor: Region radius = factor × beam major axis
        """
        self.catalog_df = self._load_catalog(catalog_file)
        self.beam_maj, self.beam_min, self.beam_pa = self._get_beam(mfs_image)
        self.flux_threshold = flux_threshold_mJy / 1000.0  # Convert to Jy
        self.eps_factor = eps_factor
        self.min_samples = min_samples
        self.mask_radius_factor = mask_radius_factor
        self.problematic_sources = []
    
    def _load_catalog(self, catalog_file: str) -> pd.DataFrame:
        """Load PyBDSF FITS catalog."""
        with fits.open(catalog_file) as hdul:
            for hdu in hdul:
                if hasattr(hdu, 'data') and hdu.data is not None:
                    try:
                        df = Table(hdu.data).to_pandas()
                        
                        # Find flux column
                        flux_cols = ['Peak_flux', 'Total_flux', 'peak_flux', 'total_flux']
                        for col in flux_cols:
                            if col in df.columns:
                                df['flux'] = df[col]
                                break
                        
                        return df
                    except:
                        continue
        
        raise ValueError(f"Could not load catalog: {catalog_file}")
    
    def _get_beam(self, mfs_image: str) -> Tuple[float, float, float]:
        """Get beam parameters from FITS image."""
        with fits.open(mfs_image) as hdul:
            header = hdul[0].header
            
            bmaj = header.get('BMAJ', 0) * 3600  # degrees to arcsec
            bmin = header.get('BMIN', 0) * 3600
            bpa = header.get('BPA', 0)
            
            if bmaj == 0:
                bmaj = 6.0
                bmin = 6.0
            
            return bmaj, bmin, bpa
    
    def spatial_clustering(self) -> np.ndarray:
        """Perform DBSCAN spatial clustering."""
        coords = self.catalog_df[['RA', 'DEC']].values
        eps = (self.beam_maj / 3600.0) * self.eps_factor  # degrees
        
        dbscan = DBSCAN(eps=eps, min_samples=self.min_samples)
        cluster_labels = dbscan.fit_predict(coords)
        self.catalog_df['cluster_id'] = cluster_labels
        
        return cluster_labels
    
    def analyze_clusters(self) -> List[Dict]:
        """Analyze each cluster for problematic patterns."""
        problematic_clusters = []
        
        for cluster_id in sorted(self.catalog_df['cluster_id'].unique()):
            if cluster_id == -1:  # Skip isolated sources
                continue
            
            cluster_sources = self.catalog_df[self.catalog_df['cluster_id'] == cluster_id]
            if len(cluster_sources) < 3:
                continue
            
            # Find brightest source
            brightest_idx = cluster_sources['flux'].idxmax()
            brightest_source = cluster_sources.loc[brightest_idx]
            brightest_flux_mJy = brightest_source['flux'] * 1000
            
            # Apply flux threshold
            if brightest_source['flux'] < self.flux_threshold:
                continue
            
            # Analyze pattern
            pattern = self._classify_pattern(cluster_sources, brightest_source)
            
            if pattern['is_problematic']:
                # Calculate region radius based on cluster extent
                coords = cluster_sources[['RA', 'DEC']].values
                bright_coord = np.array([brightest_source['RA'], brightest_source['DEC']])
                distances = cdist(coords, [bright_coord]).flatten() * 3600  # to arcsec
                max_distance = distances.max()
                
                # Region radius: max of (mask_radius_factor × beam) or (cluster extent)
                region_radius = max(
                    self.mask_radius_factor * self.beam_maj,
                    max_distance * 1.2
                )
                
                cluster_info = {
                    'cluster_id': cluster_id,
                    'ra': brightest_source['RA'],
                    'dec': brightest_source['DEC'],
                    'flux_mJy': brightest_flux_mJy,
                    'pattern_type': pattern['pattern_type'],
                    'confidence': pattern['confidence'],
                    'n_sources': len(cluster_sources),
                    'region_radius': region_radius,
                }
                problematic_clusters.append(cluster_info)
        
        self.problematic_sources = problematic_clusters
        return problematic_clusters
    
    def _classify_pattern(self, cluster_sources: pd.DataFrame, 
                          brightest_source: pd.Series) -> Dict:
        """Classify if cluster shows artifact patterns."""
        coords = cluster_sources[['RA', 'DEC']].values
        fluxes = cluster_sources['flux'].values
        
        bright_coord = np.array([brightest_source['RA'], brightest_source['DEC']])
        distances = cdist(coords, [bright_coord]).flatten()
        angles = np.arctan2(coords[:, 1] - bright_coord[1], 
                           coords[:, 0] - bright_coord[0])
        
        # Remove brightest source
        mask = distances > 0
        if np.sum(mask) < 2:
            return {'is_problematic': False, 'pattern_type': 'insufficient', 'confidence': 0.0}
        
        sat_distances = distances[mask]
        sat_fluxes = fluxes[mask]
        sat_angles = angles[mask]
        
        confidence = 0.0
        pattern_type = 'none'
        
        # Check 1: Flux decreases with distance (sidelobe pattern)
        if len(sat_distances) > 2:
            corr = np.corrcoef(sat_distances, sat_fluxes)[0, 1]
            if corr < -0.2:
                confidence += 0.3
                pattern_type = 'sidelobe'
        
        # Check 2: Sources distributed around center
        angle_std = circstd(sat_angles)
        if angle_std > 1.0:
            confidence += 0.3
        
        # Check 3: Central source much brighter
        flux_ratio = brightest_source['flux'] / sat_fluxes.max()
        if flux_ratio > 3:
            confidence += 0.2
        
        # Check 4: Dynamic range artifacts (flux ~ 1-10% of central)
        max_sat_flux = sat_fluxes.max()
        if 0.01 < max_sat_flux / brightest_source['flux'] < 0.1:
            confidence += 0.2
            if pattern_type == 'none':
                pattern_type = 'dynamic_range'
        
        return {
            'is_problematic': confidence > 0.5,
            'pattern_type': pattern_type,
            'confidence': min(confidence, 1.0)
        }
    
    def run_detection(self) -> List[Dict]:
        """Run full detection pipeline."""
        self.spatial_clustering()
        return self.analyze_clusters()


def detect_artifacts(catalog_file: str, mfs_image: str,
                     flux_threshold_mJy: float = 50,
                     eps_factor: float = 5,
                     min_samples: int = 3,
                     mask_radius_factor: float = 2.5) -> List[Dict]:
    """
    Detect bright sources with artifacts.
    
    Args:
        catalog_file: PyBDSF catalog FITS file
        mfs_image: MFS FITS image
        flux_threshold_mJy: Minimum flux for peeling
        eps_factor: Clustering radius factor
        min_samples: Minimum cluster size
        mask_radius_factor: Region radius factor
    
    Returns:
        List of problematic source dicts
    """
    detector = BrightSourceArtifactDetector(
        catalog_file=catalog_file,
        mfs_image=mfs_image,
        flux_threshold_mJy=flux_threshold_mJy,
        eps_factor=eps_factor,
        min_samples=min_samples,
        mask_radius_factor=mask_radius_factor
    )
    
    return detector.run_detection()


def create_ds9_regions(sources: List[Dict], output_file: str) -> str:
    """
    Create DS9 region file from detected sources.
    
    Region format:
    # Region file format: DS9 version 4.1
    global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" ...
    fk5
    circle(RA,DEC,radius")
    
    Args:
        sources: List of source dicts with ra, dec, region_radius
        output_file: Output region file path
    
    Returns:
        Output file path
    """
    with open(output_file, 'w') as f:
        f.write("# Region file format: DS9 version 4.1\n")
        f.write('global color=green dashlist=8 3 width=1 font="helvetica 10 normal roman" ')
        f.write('select=1 highlite=1 dash=0 fixed=0 edit=1 move=1 delete=1 include=1 source=1\n')
        f.write("fk5\n")
        
        for source in sources:
            ra = source['ra']
            dec = source['dec']
            radius = source['region_radius']
            
            f.write(f'circle({ra:.7f},{dec:.7f},{radius:.3f}")\n')
    
    return output_file


def get_sources_for_peeling(sources: List[Dict]) -> List[Dict]:
    """
    Get sources sorted by flux for sequential peeling.
    Brightest first.
    
    Args:
        sources: List of detected sources
    
    Returns:
        Sorted list (brightest first)
    """
    return sorted(sources, key=lambda x: x['flux_mJy'], reverse=True)
