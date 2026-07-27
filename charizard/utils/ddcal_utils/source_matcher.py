# charizard/utils/ddcal_utils/source_matcher.py
"""
Find bright sources from PyBDSF catalog and write region files for peeling.
"""

import os
import pandas as pd
from astropy.io import fits
from astropy.table import Table
from astropy.coordinates import SkyCoord
import astropy.units as u
from typing import List, Optional


def load_pybdsf_catalog(catalog_file: str, logger=None) -> pd.DataFrame:
    """Load PyBDSF FITS catalog."""
    def _log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)
    
    with fits.open(catalog_file) as hdul:
        for hdu in hdul:
            if hasattr(hdu, 'data') and hdu.data is not None:
                try:
                    df = Table(hdu.data).to_pandas()
                    
                    flux_cols = ['Peak_flux', 'Total_flux', 'peak_flux', 'total_flux']
                    for col in flux_cols:
                        if col in df.columns:
                            df['flux_Jy'] = df[col]
                            df['flux_mJy'] = df[col] * 1000
                            break
                    
                    _log(f"Loaded {len(df)} sources from PyBDSF")
                    if len(df) > 0:
                        _log(f"Flux range: {df['flux_mJy'].min():.1f} - {df['flux_mJy'].max():.1f} mJy")
                    
                    return df
                except:
                    continue
    
    raise ValueError(f"Could not load PyBDSF catalog: {catalog_file}")


def get_image_center_and_halfwidth(image_file: str):
    """
    Read image center (RA, DEC in deg) and half-width (deg) from a FITS header.

    Returns (center_ra, center_dec, half_width_deg) or None if the header
    is missing the needed keywords.
    """
    with fits.open(image_file) as hdul:
        header = hdul[0].header

    ra = header.get('CRVAL1')
    dec = header.get('CRVAL2')
    naxis = header.get('NAXIS1')
    cdelt = header.get('CDELT1', header.get('CD1_1'))

    if ra is None or dec is None or naxis is None or cdelt is None:
        return None

    half_width_deg = abs(cdelt) * naxis / 2.0
    return float(ra), float(dec), half_width_deg


def find_bright_sources_and_write_regions(pybdsf_df: pd.DataFrame,
                                           output_dir: str,
                                           flux_threshold_mJy: float = 50.0,
                                           region_radius_arcsec: float = 15.0,
                                           image_file: Optional[str] = None,
                                           min_distance_fraction: float = 0.1,
                                           logger=None) -> List[str]:
    """
    Find bright sources from PyBDSF catalog and write individual region files.

    Sources closer to the image center than min_distance_fraction of the
    image half-width are NOT peeled: direction-independent selfcal already
    corrects the field center, and peeling there can absorb target flux.

    Args:
        pybdsf_df: PyBDSF catalog DataFrame
        output_dir: Directory to write region files
        flux_threshold_mJy: Minimum flux for peeling
        region_radius_arcsec: Radius for region circles
        image_file: Image the catalog came from (for the distance cut);
            None disables the cut
        min_distance_fraction: Inner exclusion radius as a fraction of the
            image half-width (0 disables the cut)
        logger: Optional logger

    Returns:
        List of region file paths (ordered brightest first)
    """
    def _log(msg):
        if logger:
            logger.info(msg)
        else:
            print(msg)

    os.makedirs(output_dir, exist_ok=True)

    # Filter by flux threshold
    bright_sources = pybdsf_df[pybdsf_df['flux_mJy'] >= flux_threshold_mJy].copy()
    _log(f"Sources above {flux_threshold_mJy} mJy: {len(bright_sources)}")

    # Filter by distance from image center
    if len(bright_sources) > 0 and image_file and min_distance_fraction > 0:
        geometry = None
        if os.path.exists(image_file):
            geometry = get_image_center_and_halfwidth(image_file)

        if geometry is None:
            _log(f"WARNING: could not read center/scale from {image_file} - "
                 f"distance cut skipped, peeling ALL sources above threshold")
        else:
            center_ra, center_dec, half_width_deg = geometry
            min_sep_deg = min_distance_fraction * half_width_deg

            center = SkyCoord(center_ra * u.deg, center_dec * u.deg)
            coords = SkyCoord(bright_sources['RA'].values * u.deg,
                              bright_sources['DEC'].values * u.deg)
            sep_deg = center.separation(coords).deg
            bright_sources['sep_deg'] = sep_deg

            too_close = bright_sources[bright_sources['sep_deg'] < min_sep_deg]
            for _, src in too_close.iterrows():
                _log(f"  Skipping {src['flux_mJy']:.1f} mJy source at "
                     f"{src['sep_deg'] * 60:.1f} arcmin from center "
                     f"(inside {min_sep_deg * 60:.1f} arcmin exclusion - "
                     f"selfcal handles the field center)")

            bright_sources = bright_sources[bright_sources['sep_deg'] >= min_sep_deg]
            _log(f"Sources outside inner {min_distance_fraction:.0%} of image: "
                 f"{len(bright_sources)}")

    if len(bright_sources) == 0:
        _log("No bright sources to peel")
        return []
    
    # Sort by flux (brightest first)
    bright_sources = bright_sources.sort_values('flux_mJy', ascending=False)
    
    region_files = []
    
    for i, (idx, source) in enumerate(bright_sources.iterrows()):
        source_num = i + 1
        ra = source['RA']
        dec = source['DEC']
        flux_mJy = source['flux_mJy']
        
        # Write region file for this source
        region_file = f"{output_dir}/peel_source_{source_num}.reg"
        
        with open(region_file, 'w') as f:
            f.write("# Region file format: DS9 version 4.1\n")
            f.write(f"# Source {source_num}\n")
            f.write(f"# Flux: {flux_mJy:.1f} mJy\n")
            f.write("global color=red dashlist=8 3 width=3\n")
            f.write("fk5\n")
            f.write(f'circle({ra:.7f},{dec:.7f},{region_radius_arcsec:.1f}")\n')
        
        region_files.append(region_file)
        
        _log(f"  Source {source_num}: {flux_mJy:.1f} mJy at RA={ra:.5f}, DEC={dec:.5f} -> {region_file}")
    
    _log(f"Created {len(region_files)} region files for peeling")
    
    return region_files