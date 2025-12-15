#!/bin/bash

################################################################################
# Radio Interferometry Peeling Pipeline
# 
# This script performs peeling of bright sources from radio interferometry data
# through imaging, calibration, and subtraction steps.
#
# Author: Arpan Pal
# Date: 21st January 2025
################################################################################

# Set your measurement set name
MS="combined_spw3.ms"

################################################################################
# STEP 1: Initial Imaging with WSClean
################################################################################
# Purpose: Create an initial deep image to identify bright sources
# This uses the original DATA column

wsclean \
  -name test5 \                           # Output image prefix
  -j 20 \                                 # Number of parallel threads
  -mem 100 \                              # Memory limit in GB
  -weight briggs 0.0 \                    # Robust weighting (0 = natural weighting)
  -super-weight 1.0 \                     # Super-weighting factor
  -weighting-rank-filter-size 16 \        # Rank filter size for weighting
  -taper-gaussian 0 \                     # No Gaussian taper
  -size 9600 9600 \                       # Image size in pixels
  -scale 1asec \                          # Pixel scale
  -channels-out 10 \                      # Number of output frequency channels
  -grid-mode kb \                         # Kaiser-Bessel gridding
  -kernel-size 7 \                        # Gridding kernel size
  -oversampling 63 \                      # Gridding oversampling factor
  -pol I \                                # Stokes I only
  -intervals-out 1 \                      # Single time interval
  -data-column DATA \                     # Input data column
  -niter 50000 \                          # Maximum clean iterations
  -auto-threshold 0.05 \                  # Auto-masking threshold (5% of peak)
  -local-rms-window 31 \                  # Local RMS window size
  -gain 0.1 \                             # Clean gain per iteration
  -mgain 0.9 \                            # Major iteration gain
  -join-channels \                        # Joint deconvolution across channels
  -multiscale-scale-bias 0.6 \            # Bias toward smaller scales
  -fit-spectral-pol 3 \                   # Fit 3rd order spectral polynomial
  -fit-beam \                             # Fit clean beam
  -elliptical-beam \                      # Use elliptical beam
  -padding 1.3 \                          # Image padding factor
  -parallel-deconvolution 8192 \          # Parallel deconvolution subimage size
  -field 0 \                              # Field ID to image
  -save-source-list \                     # Save component list
  $MS

################################################################################
# STEP 2: Source Finding with Crystalball
################################################################################
# Purpose: Identify bright sources for peeling and create sky model
# This creates a model of bright sources that will be peeled out

crystalball \
  $MS \                                   # Input measurement set
  -sm test5-sources.txt \                 # Output source model
  -w ds9.reg \                            # DS9 region file input, so it will only find bad sources inside those regions.
  -o bright_ext_source_column \           # Output column name for model
  -f 2                                    # Frequency channel selection

################################################################################
# STEP 3: Direction-Dependent Calibration with QuartiCal
################################################################################
# Purpose: Solve for direction-dependent gains and subtract bright sources
# This solves for both direction-independent (G) and direction-dependent (dE) 
# gains, then subtracts the bright sources from the data

goquartical \
  input_ms.path=$MS \
  input_ms.data_column=DATA \             # Input data column
  input_ms.time_chunk='300s' \            # Time chunking (5 minutes)
  input_ms.freq_chunk='0' \               # Frequency chunking (0=all channels)
  input_model.recipe=MODEL_DATA~bright_ext_source_column:bright_ext_source_column \
  solver.terms='[G,dE]' \                 # Solve for G and dE Jones terms
  solver.iter_recipe='[25,25,10,10]' \    # Iterations: 25 for G, 25 for dE, then 10,10
  output.log_directory=outputs.qc \       # Log output directory
  output.overwrite=1 \                    # Overwrite existing output
  output.products=[corrected_data,corrected_residual,corrected_weight] \
  output.columns=[CORRECTED_DATA,SUBDD_DATA_bright_ext_q,WEIGHT_SPECTRUM] \
  output.subtract_directions=[1] \        # Subtract the second direction (bright sources)
  dask.threads=6 \                        # Dask parallel threads
  G.type=diag_complex \                   # G-Jones: diagonal complex (per-antenna)
  G.time_interval='10s' \                 # G solution interval: 10 seconds
  G.freq_interval='10' \                  # G frequency interval: 10 channels
  dE.time_interval='100' \                # dE solution interval: 100 seconds
  dE.freq_interval='100' \                # dE frequency interval: 100 channels
  dE.type=complex \                       # dE Jones type: full complex
  dE.direction_dependent=true             # dE is direction-dependent (for peeling)
  

################################################################################
# STEP 4: Final Imaging After Peeling
################################################################################
# Purpose: Image the peeled data to verify source removal and image quality
# This uses the corrected data column from QuartiCal where bright sources
# have been subtracted

wsclean \
  -name test6_q \                         # Output image prefix
  -j 20 \                                 # Number of parallel threads
  -mem 100 \                              # Memory limit in GB
  -weight briggs 0.0 \                    # Robust weighting
  -super-weight 1.0 \                     # Super-weighting factor
  -weighting-rank-filter-size 16 \        # Rank filter size
  -taper-gaussian 0 \                     # No taper
  -size 9600 9600 \                     # Image size
  -scale 1asec \                      # Pixel scale
  -channels-out 10 \                      # Output channels
  -grid-mode kb \                         # Kaiser-Bessel gridding
  -kernel-size 7 \                        # Kernel size
  -oversampling 63 \                      # Oversampling factor
  -pol I \                                # Stokes I
  -intervals-out 1 \                      # Single time interval
  -data-column SUBDD_DATA_bright_ext_q \  # Use peeled data column
  -niter 50000 \                          # Clean iterations
  -auto-threshold 0.05 \                  # Auto-threshold (5% of peak)
  -local-rms-window 31 \                  # Local RMS window
  -gain 0.1 \                             # Clean gain
  -mgain 0.9 \                            # Major cycle gain
  -join-channels \                        # Join channels
  -multiscale-scale-bias 0.6 \            # Multiscale bias
  -fit-spectral-pol 3 \                   # Spectral polynomial order
  -fit-beam \                             # Fit beam
  -elliptical-beam \                      # Elliptical beam
  -padding 1.3 \                          # Padding
  -parallel-deconvolution 8192 \          # Parallel deconvolution
  -save-source-list \                     # Save source list
  $MS

################################################################################
# END OF PIPELINE
################################################################################

echo "Peeling pipeline completed successfully!"