# RADIO ASTRONOMY PROTOCOL: PHYSICS & DATA INTEGRITY

## 1. THE LAWS OF PHYSICS (MANDATORY)
- **UNIT CONSERVATION**: Never perform arithmetic on variables without verifying units (e.g., Jy vs K, Hz vs MHz). Always use `astropy.units` for conversions.
- **PHYSICAL CONSTANTS**: Never hard-code $c$, $k_B$, or $h$. Always import from `astropy.constants` or `scipy.constants`.
- **COORDINATE RIGOR**: Strictly distinguish between J2000, ICRS, and Galactic frames. Any coordinate transformation must be verified using `astropy.coordinates`.
- **NYQUIST-SHANNON**: For any sampling or FFT logic, verify that the sampling rate meets the Nyquist criterion for the given bandwidth.

## 2. DATA STRUCTURES & TOOLS (CASACORE/CASA)
- **CASACORE TABLES**: When accessing MeasurementSets (MS) or Tables, use `python-casacore` (`casacore.tables`). Always wrap table access in `try...finally` to ensure `tb.close()` is called, preventing table locks.
- **VISIBILITIES**: For interferometry data, respect the standard [Time, Baseline, Channel, Polarization] axis ordering. Verify the `DATA`, `CORRECTED_DATA`, and `MODEL_DATA` columns before processing.
- **WCS RIGOR**: All image-plane operations must respect the World Coordinate System (WCS) headers. Use `astropy.wcs` or `casacore.images`.

## 3. NON-HALLUCINOGENIC GUARDRAILS
- **GIVE UP EARLY**: If a specific FITS keyword or MeasurementSet column is not visible in the current file scan, state "I do not have the metadata" instead of guessing the key name.
- **VECTORIZATION**: Favor `numpy` and `casacore` vectorized operations over Python loops. If you see a `for` loop over visibilities, rewrite it as a slice or ufunc.
- **ALGORITHM TRACE**: For calibration (Gain, Bandpass, Phase) or imaging (CLEAN, Deconvolution), you must state the mathematical step (e.g., "Applying Jones Matrix") before writing the code.

## 4. VERIFICATION WORKFLOW
1. **INSPECT**: Use `tb.getcol()` or `fits.getheader()` to verify the real data shape.
2. **PLAN**: Outline the math and the specific `casacore` or `astropy` sub-modules.
3. **CODE**: Write surgical, well-commented code that includes unit checks.
4. **SANITY CHECK**: Automatically suggest a "sanity script" (e.g., checking for negative flux or non-physical temperatures).


