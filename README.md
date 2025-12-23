# Charizard v2.0

**Radio Interferometry Calibration Pipeline**

A modular, HPC-ready pipeline for VLA/GMRT/MeerKAT data processing.

## Installation

```bash
# Install housekeeper first
pip install -e /path/to/housekeeper

# Install charizard
pip install -e .
```

## Usage

```bash
charizard --config pokedex.yaml --scheduler_config scheduler_config.yaml
```

### Command Line Options

```
--config, -c         Pipeline config (pokedex.yaml)
--scheduler_config, -s  Scheduler config for housekeeper
--models, -m         User models.yaml (optional)
--start-from         Start from: stager, flagging, calibration, selfcal
--stop-after         Stop after: stager, flagging, calibration, selfcal
--jobs-dir           Jobs directory (default: <ms_name>_jobs)
```

## Configuration

### pokedex.yaml

```yaml
data:
  ms: /path/to/observation.ms
  processing_spw: 4

environment:
  casa_path: /opt/casa/casa-6.5
  preamble: |
    module load python/3.10

sources:
  auto_detect: true
  overrides:
    calibrators:
      flux: 3C286
      phase: J1407+2827

flow:
  initial_calibration_flagging:
    setup: {initialize: true, make_structure: true, brotherhood: true}
    flagging:
      bad_antennas: {auto: true, list: []}
      rfi: true
    calibration:
      refant: null  # Auto-detect best antenna
      pol: {leakage: {enabled: true, mode: Df}, angle: {enabled: true}}
      control: {rounds: 2, flag_before: true, flag_after: true}
      apply: {calibrators: true, targets: true}
      show_me_plots: true
  
  imaging_selfcal:
    selfcal:
      imaging: {imsize: 4096, cellsize: 1asec}
      loops: {phase: 4, amp_phase: 2, solint: 4min, refant: C00}
      clean: {start_iters: 1000}
```

### scheduler_config.yaml

```yaml
scheduler: pbs

pbs:
  resource_style: select  # OpenPBS
  queues: {default: workq, gpu: gpu}
  gpu: {enabled: true, host: bhima04, ngpus: 1, modules: [cuda/12.3]}
  directives: ["-V", "-j oe"]
  modules: [python/3.10, casa/6.5]
```

## Features

### Automatic Antenna Analysis

When `flagging.bad_antennas.auto: true`:

1. **Bad Antenna Detection**: Flags antennas with:
   - Low amplitude (< 10% of reference)
   - High amplitude (> 10× reference)
   - High flag fraction (> 80%)

2. **Reference Antenna Selection**: Finds best refant by:
   - **SNR metric**: mean / spread (higher is better)
   - **Flag fraction**: lower is better
   - **Combined metric**: SNR × (1 - flag_fraction)

### Diagnostic Plots

When `calibration.show_me_plots: true`, generates shadems plots:

**Per calibrator:**
- UV vs Amp/Phase (colour by antenna)
- Freq vs Amp/Phase (colour by scan)
- Time vs Amp/Phase (colour by antenna)

**For polcal sources:**
- Freq vs Cross-hand Amp/Phase (RL, LR)
- Time vs Cross-hand Amp/Phase

**Per target:**
- UV vs Amp
- Freq vs Amp
- Time vs Amp

All plots saved to `spw{N}/plots/`

## Pipeline Stages

1. **Stager**: MS analysis, field classification, splitting
2. **Flagging**: Bad antenna detection, initial flags, TFCrop
3. **Calibration**: Gains, polcal, applycal, NAMI flagging
4. **Selfcal**: Phase loops, amp+phase loops, final imaging

## Directory Structure

After running:
```
observation_jobs/
├── housekeeper.db      # Job tracking database
├── charizard.log       # Pipeline log
├── charizard.html      # HTML log
├── spw0/
│   ├── cal.ms          # Calibrators
│   ├── src.ms          # Targets
│   ├── caltables/      # Calibration tables
│   ├── plots/          # Diagnostic plots
│   └── field_name/     # Selfcal per field
│       ├── pcal1.ms, pcal2.ms, ...
│       ├── selfcal-tables/
│       └── final_field-MFS-image.fits
└── spw1/
    └── ...
```

## Requirements

- Python >= 3.8
- CASA >= 6.5
- WSClean >= 3.0
- casacore
- astropy
- rich
- housekeeper (included)

Optional:
- NAMI (for post-calibration flagging)
- shadems (for diagnostic plots)
- foresight, crystalball, quartical (for catalog selfcal)
