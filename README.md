# CHARIZARD

Radio Interferometry Calibration Pipeline

## Installation

```bash
pip install -e .
```

Requires `housekeeper` for job submission:
```bash
pip install /path/to/housekeeper
```

## Usage

```bash
charizard pokedex.yaml
```

With scheduler config:
```bash
charizard pokedex.yaml -s scheduler.yaml
```

## Configuration (pokedex.yaml)

```yaml
environment:
  working_dir: ./
  scheduler: pbs
  casa_path: /path/to/casa
  shell_preamble: |
    source ~/.bashrc
    mamba activate myenv
  resources:
    default: {nodes: 1, ppn: 4, walltime: 04:00:00, mem_gb: 128}
    flagging: {nodes: 2, ppn: 8, walltime: 12:00:00, mem_gb: 512}
    crosscal: {nodes: 2, ppn: 8, walltime: 12:00:00, mem_gb: 128}

data:
  ms: mydata.ms
  processing_spw: 4

sources:
  auto_detect: true
  overrides:
    calibrators: 
      amp: [3C286]
      phase: J1234+5678
      leakage: J1234+5678
      pol_angle: 3C286
    uvranges: 
      3C286: "0~56klambda"

flow:
  initial_calibration_flagging:
    setup: 
      initialize: true
      make_structure: true
      brotherhood: true
    flagging: 
      bad_antennas: {auto: true, list: []}
      rfi: true
    calibration:
      refant: C00
      pol: {leakage: {mode: Df}, angle: true}
      control: {check_solutions: true, flag_before: true, flag_after: true}
      apply: {targets: true, calibrators: true}
```

## Structure

- `main.py` - Entry point
- `charizard.py` - Orchestrator
- `utils/general/` - General utilities (config, ms, logging, source matching)
- `utils/splitting_utils/` - MS splitting
- `utils/flagging_utils/` - Flagging (to be implemented)
- `utils/calibration_utils/` - Calibration (to be implemented)
- `utils/selfcal_utils/` - Self-calibration (to be implemented)
- `utils/imaging_utils/` - Imaging (to be implemented)
