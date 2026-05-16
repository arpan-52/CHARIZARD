# CHARIZARD

Radio interferometry calibration pipeline for VLA/WSRT/MeerKAT data. Handles
cross-calibration, RFI flagging, self-calibration, and direction-dependent
calibration in a single run. All heavy computation runs inside a container —
your HPC node only needs Python and udocker.

## How it works

CHARIZARD runs on the login node and submits jobs to your PBS/SLURM cluster
via [housekeeper](https://github.com/arpan-52/housekeeper). Every job runs
inside the compute container (CASA, WSClean, catboss, PyBDSF, foresight) via
udocker — no root access needed, no module conflicts.

```
You → charizard run pokedex.yaml
         │
         └── housekeeper (PBS/SLURM)
                  │
                  └── udocker run charizard <job>
                           │
                           ├── CASA 6.7.3
                           ├── WSClean 3.6 (IDG + EveryBeam)
                           ├── catboss (GPU flagging — pooh + nimki)
                           ├── PyBDSF
                           └── foresight
```

## Requirements

- Python 3.8+
- [micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) or conda
- [udocker](https://github.com/indigo-dc/udocker) (installed automatically)
- PBS or SLURM scheduler on your cluster
- NVIDIA GPU on compute nodes (for catboss flagging)

## Installation

Clone and install into your environment:

```bash
git clone https://github.com/arpan-52/charizard
cd charizard
pip install -e .
```

Install dependencies:

```bash
pip install git+https://github.com/arpan-52/housekeeper
pip install git+https://github.com/arpan-52/catboss
pip install /path/to/foresight
```

## Container setup

Do this once per machine/cluster. It pulls the compute image, sets up the
container, configures GPU access, and tells you exactly what to add to your
config:

```bash
charizard setup env
```

If your CUDA libraries are at a non-standard path (common on HPC clusters):

```bash
charizard setup env --cuda-lib-path /usr/local/cuda/lib64
```

To use a different container image:

```bash
charizard setup env --image yourrepo/yourimage:latest --name mycontainer
```

At the end of setup, CHARIZARD prints the `container:` block to add to your
`pokedex.yaml`. Copy-paste it in.

## Running the pipeline

```bash
charizard run pokedex.yaml
```

With a custom scheduler config:

```bash
charizard run pokedex.yaml -s scheduler.yaml
```

With custom polarization calibrator models:

```bash
charizard run pokedex.yaml -m my_models.yaml
```

## Configuration

Everything lives in `pokedex.yaml`. A full example:

```yaml
environment:
  working_dir: /scratch/myproject
  scheduler: pbs
  shell_preamble: |
    source ~/.bashrc
    micromamba activate 312data

  # Added by `charizard setup env` — copy from setup output
  container:
    image: apal52/charizard-pipeline:latest
    name: charizard
    cuda_lib_path: /usr/lib          # optional, only if GPU auto-detect failed
    volumes: []                      # extra mounts, e.g. [/scratch:/scratch]

  resources:
    default:  {nodes: 1, ppn: 4,  walltime: "04:00:00", mem_gb: 128}
    flagging: {nodes: 1, ppn: 8,  walltime: "08:00:00", mem_gb: 256}
    crosscal: {nodes: 1, ppn: 8,  walltime: "12:00:00", mem_gb: 128}
    selfcal:  {nodes: 1, ppn: 4,  walltime: "12:00:00", mem_gb: 128}
    imaging:  {nodes: 1, ppn: 8,  walltime: "24:00:00", mem_gb: 512}

data:
  ms: /data/G71.ms
  processing_spw: 4

sources:
  auto_detect: true       # detect calibrators from MS field names
  overrides:
    calibrators:
      amp:       [3C286, 3C48]
      phase:     1845+401
      leakage:   1845+401
      pol_angle: 3C286
    uvranges:
      3C286: "0~56klambda"
      3C48:  ""

flow:
  initial_calibration_flagging:
    setup:
      initialize:     true
      make_structure: true
      brotherhood:    true    # all SPWs succeed or all fail together

    flagging:
      bad_antennas: {auto: true, list: []}
      rfi:          true
      use_gpu:      true      # catboss runs in GPU mode (requires GPU nodes)

    calibration:
      refant: C00
      pol:
        leakage: {mode: Df}
        angle:   true
      control:
        check_solutions: true
        plot:            true
      apply:
        targets:     true
        calibrators: true

  imaging_selfcal:
    dirty_image: true
    selfcal:
      avg_flag:        true
      freqbin:         10
      flag_residuals:  true
      imaging:
        imsize:        7200
        cellsize:      1asec
        use_catalogs:  true
      loops:
        phase:     4
        amp_phase: 2
        solint:    4min      # starting solint
        min_solint: 8s       # solint floor
        factor:    2         # per-round progression factor (see below)
        refant:    C00
      clean:
        threshold:     0.001 # starting clean threshold
        min_threshold: 0.0   # threshold floor (0 = no floor)
        start_iters:   1000  # starting wsclean niter

  dd_cal:
    source_finding:
      flux_threshold_mJy: 50
      eps_factor:         5
      min_samples:        3
      mask_radius_factor: 2.5
    peeling:
      g_time_interval:  120s
      g_freq_interval:  0
      de_time_interval: 120s
      de_freq_interval: 0
```

### Key config options

**`brotherhood`** — when true, all SPWs are treated as a unit. If any SPW fails
a step, the whole pipeline stops. Set to false if you want surviving SPWs to
continue.

**`use_gpu`** — controls whether catboss runs in GPU mode. Requires GPU nodes
in your PBS/SLURM setup. WSClean and CASA get GPU resources automatically when
available.

**`auto_detect`** — CHARIZARD reads your MS field names and matches them to
known calibrators. Set to false and use `overrides` if auto-detection gets it
wrong.

**`shell_preamble`** — runs on the compute node before each job. Must activate
the environment that has udocker. Does not run inside the container.

**`factor`** (selfcal) — a single progression factor applied every self-cal
round: `niter` is multiplied by it, while `clean.threshold` and `solint` are
divided by it. So with `factor: 2`, `start_iters: 1000`, `solint: 4min`,
`threshold: 0.001` the rounds run niter `1000 → 2000 → 4000 …`, solint
`4min → 2min → 1min …` (floored at `min_solint`), threshold
`1e-3 → 5e-4 → 2.5e-4 …` (floored at `min_threshold`). Lower it (e.g. `1.5`)
or raise `min_threshold` if images look over-cleaned. Note wsclean also runs
`-auto-threshold 3`, so early rounds are noise-limited at 3σ until the
scheduled threshold drops below that.

## Pipeline steps

| Step | What happens |
|------|-------------|
| 1 | Read MS metadata — fields, SPWs, antennas, frequencies |
| 2 | Split into `cal.ms` (calibrators) and `src.ms` (targets) per SPW |
| 3 | Bad antenna detection → `badants.txt` |
| 4 | Apply initial flags |
| 5 | RFI flagging on calibrators (catboss pooh — GPU) |
| 6 | Find best reference antenna |
| 7 | Calibration round 1 — gaincal, bandpass, polcal (parallel with source flagging) |
| 8 | Post-cal flagging (catboss pooh + catboss nimki) |
| 9 | Calibration round 2 |
| 10 | Apply solutions to targets |
| 11 | Final flagging on all data |
| 12 | Diagnostic plots (optional) |
| 13 | Imaging + self-calibration loop (optional) |
| 14 | Direction-dependent calibration — peeling (optional) |

## Container contents

The compute container (`apal52/charizard-pipeline:latest`) has:

| Tool | Version | Used for |
|------|---------|----------|
| CASA | 6.7.3 | Splitting, calibration, applycal |
| WSClean | 3.6 | Imaging, self-cal |
| casacore | 3.6.0 | MS access |
| EveryBeam | 0.7.2 | Primary beam correction |
| IDG | latest | GPU-accelerated gridding |
| catboss (pooh) | 1.0 | GPU RFI flagging (dynamic spectra) |
| catboss (nimki) | 1.0 | CPU RFI flagging (UV-domain Gabor) |
| PyBDSF | latest | Source finding for DDCal |
| foresight | 0.1 | Source masking from TGSS-NVSS |
| JAX | cuda13 | GPU compute |
| CuPy | cuda13 | GPU array operations |

## Output structure

```
working_dir/
├── jobs/               # PBS/SLURM scripts and logs
├── spw0/
│   ├── cal.ms          # calibrator data
│   ├── src.ms          # target data
│   ├── badants.txt
│   └── *.py / *.sh     # job scripts
├── spw1/
│   └── ...
├── images/
│   └── <field>/        # WSClean output images
├── ddcal_output/
│   └── <field>/        # DDCal peeled images
└── charizard_<date>.log
```

## Troubleshooting

**GPU not working after setup**

Run setup with `--cuda-lib-path` pointing to where `libcuda.so` lives on your
system:

```bash
charizard setup env --cuda-lib-path /usr/local/nvidia/lib64
```

Common locations: `/usr/lib`, `/usr/local/nvidia/lib64`,
`/usr/local/cuda/lib64`.

**catboss fails with no GPU**

Set `use_gpu: false` in your config under `flagging:`. catboss nimki (the UV
flagging step) always runs on CPU and will still work fine.

**Container not found**

Re-run `charizard setup env` — udocker containers live in `~/.udocker` and
are per-user. Every user needs to run setup once.

**Jobs fail immediately**

Check `jobs/<spw>/` for `.log` and `.err` files. The most common cause is a
bad `shell_preamble` — make sure it activates an environment with udocker.

## Authors

Arpan Pal — NCRA-TIFR
