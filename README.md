<img src="image.png" alt="CHARIZARD" width="190" align="right">

# CHARIZARD

Radio interferometry calibration pipeline for VLA, WSRT and MeerKAT data.

One command takes you from a raw measurement set to self-calibrated, primary-beam-corrected
images: splitting, RFI flagging, cross-calibration, polarisation calibration, self-cal, and
direction-dependent peeling. It runs on your login node and farms every heavy step out to
PBS or SLURM, with all the astronomy software inside a container — so your cluster needs
Python and nothing else.

```bash
pip install -e .
charizard setup env
charizard run pokedex.yaml -s scheduler.yaml
```

---

## Contents

- [How it works](#how-it-works)
- [Requirements](#requirements)
- [Installation](#installation)
- [Setting up the container](#setting-up-the-container)
- [Configuration](#configuration)
- [Running](#running)
- [What the pipeline does](#what-the-pipeline-does)
- [Output layout](#output-layout)
- [MeerKAT and linear feeds](#meerkat-and-linear-feeds)
- [Tuning self-calibration](#tuning-self-calibration)
- [Troubleshooting](#troubleshooting)
- [Known issues](#known-issues)
- [What's in the container](#whats-in-the-container)

---

## How it works

CHARIZARD itself is a scheduler. It reads your config, works out what needs doing, and
submits jobs through [housekeeper](https://github.com/arpan-52/housekeeper). Each job runs
inside the compute container via udocker — no root, no modules, no dependency conflicts.

```
charizard run pokedex.yaml
        │
        └── housekeeper ──► PBS / SLURM
                                │
                                └── udocker run charizard <job>
                                         │
                                         ├── CASA            split, calibrate, applycal
                                         ├── WSClean         imaging and self-cal
                                         ├── catboss         RFI flagging (GPU + CPU)
                                         ├── PyBDSF          source finding
                                         ├── shadems         diagnostic plots
                                         ├── QuartiCal       direction-dependent solves
                                         └── CrystalBall     sky-model prediction
```

Two lightweight steps — bad-antenna detection and reference-antenna selection — run directly
in the host Python on the compute node. They need `python-casacore` and `charizard` itself,
both of which arrive with the pip install.

## Requirements

- Python 3.8+
- [micromamba](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html) or conda
- PBS or SLURM
- An NVIDIA GPU on the compute nodes, if you want GPU flagging (optional — catboss falls back to CPU)

udocker is installed for you as a dependency.

## Installation

```bash
git clone https://github.com/arpan-52/charizard
cd charizard
pip install -e .
```

That brings in housekeeper and the host-side Python dependencies. Everything else — CASA,
WSClean, catboss, PyBDSF, shadems, QuartiCal, CrystalBall — lives in the container.

## Setting up the container

Once per machine, per user. udocker containers live under `~/.udocker`, so every user on a
shared cluster runs this themselves.

```bash
charizard setup env
```

This pulls the image, creates the container, and — if the host has a usable GPU — wires the
NVIDIA driver in and verifies the whole stack really works: driver, CuPy, JAX, a compiled
numba CUDA kernel, and catboss's own kernels. When it finishes it prints the `container:`
block to paste into your config.

To be asked about each option rather than accepting defaults:

```bash
charizard setup env -i
```

### GPU

GPU support is three-state, not a boolean:

| Flag | Behaviour |
|---|---|
| *(none)* | Auto — enable if this host has a working GPU, otherwise carry on without one |
| `--gpu` | Require it. Setup **fails** if the GPU does not verify |
| `--no-gpu` | Skip it entirely; catboss flags on CPU |

Driver libraries are auto-detected. Override with `--cuda-lib-path`:

```bash
charizard setup env --cuda-lib-path /usr/lib64
```

Setup deliberately does **not** use udocker's own `--nvidia` injection. On hosts carrying
both a 32- and a 64-bit driver tree, udocker takes `/usr/lib` first and can install a 32-bit
`libcuda` into a 64-bit container. Nothing errors — CuPy just silently fails to initialise
and catboss reports `Mode: CPU only`. CHARIZARD checks the ELF class of every library, skips
the 32-bit ones, and falls through to the next candidate directory.

### Split setup: no node has both network and a driver

A common HPC layout — the login node reaches the registry but has no GPU, the GPU node has a
driver but no outbound network. Do it in two passes:

```bash
# on the login node: pull and create, skip the GPU
charizard setup env --no-gpu

# then inside a GPU job, against the container that now exists
charizard setup env --gpu-only --gpu
```

`--gpu-only` never re-pulls or re-creates, so it is safe to repeat. `--skip-pull` is the
same idea for a node that already has the image but needs the container rebuilt.

### Other options

```bash
charizard setup env --image yourrepo/yourimage:latest --name mycontainer
charizard setup env --udocker-dir /lustre/$USER/udocker     # shared scratch
```

## Configuration

Two files. `pokedex.yaml` describes the observation and what to do with it; `scheduler.yaml`
describes the cluster.

### pokedex.yaml

```yaml
environment:
  working_dir: /scratch/myproject
  scheduler: pbs
  shell_preamble: |
    source ~/.bashrc
    micromamba activate 312data
    export UDOCKER_DIR=/home/me/udocker

  # printed by `charizard setup env` — paste it in
  container:
    image: apal52/charizard-pipeline:latest
    name: charizard
    cuda_lib_path: /usr/lib64      # only if GPU auto-detection needed help
    volumes: []                    # extra mounts, e.g. ["/lustre:/lustre"]

# Quote the walltimes. Unquoted 12:00:00 is YAML sexagesimal — an integer.
# nodes must stay 1: no job here is MPI, and nodes>1 just reserves idle hardware.
resources:
  default:  {nodes: 1, ppn: 4, walltime: "04:00:00", mem_gb: 128}
  flagging: {nodes: 1, ppn: 8, walltime: "08:00:00", mem_gb: 256}
  crosscal: {nodes: 1, ppn: 8, walltime: "12:00:00", mem_gb: 128}
  selfcal:  {nodes: 1, ppn: 4, walltime: "12:00:00", mem_gb: 128}
  imaging:  {nodes: 1, ppn: 8, walltime: "24:00:00", mem_gb: 512}

data:
  ms: /data/G71.ms
  processing_spw: 4          # split the band into N sub-bands, processed in parallel

sources:
  auto_detect: true          # match MS field names against the bundled catalog
  overrides:
    calibrators:
      amp:       [3C286, 3C48]
      phase:     1845+401
      leakage:   1845+401
      pol_angle: 3C286
      targets:   [G71+28]
    uvranges:
      3C286: "0~56klambda"

flow:
  initial_calibration_flagging:
    setup: {brotherhood: true}
    flagging:
      bad_antennas: {auto: true, list: []}
      use_gpu: true
    calibration:
      refant: C00            # omit to auto-select after flagging
      minblperant: 4
      control: {plot: true}

  imaging_selfcal:
    setup: {brotherhood: true}
    dirty_image: true
    selfcal:
      avg_flag: true
      freqbin: 10
      imaging:
        imsize: 7200
        cellsize: 1asec
      loops:
        phase: 4
        amp_phase: 2
        solint: 4min
        min_solint: 8s
        factor: 2
      clean:
        threshold: 0.001
        min_threshold: 0.0
        start_iters: 1000

  dd_cal:
    source_finding:
      flux_threshold_mJy: 50
      region_radius_arcsec: 15.0
      min_distance_fraction: 0.1
    peeling:
      de_time_interval: 120s
      de_freq_interval: 0
    final_niter: 50000
```

Setting `leakage` **and** `pol_angle` switches on full polarisation: a 4-correlation split,
`gaintype='T'` solves with `parang=True`, leakage and X–Y phase solves, and I/Q/U/V imaging.
Leave either one out and you get a Stokes I run at roughly half the data volume.

Drop `imaging_selfcal` or `dd_cal` entirely to stop after cross-calibration.

Key options worth understanding:

| Option | What it does |
|---|---|
| `brotherhood` | All SPWs treated as a unit — if one fails a step, the run stops. Set false to let survivors continue |
| `auto_detect` | Matches field names against the bundled catalog. It is **VLA-only**, so southern surveys need `false` plus explicit `overrides` |
| `minblperant` | Minimum unflagged baselines before an antenna's solution is kept. 4 is the MeerKAT convention |
| `shell_preamble` | Runs on the compute node before each job, outside the container. Must activate an environment that has udocker |
| `processing_spw` | Number of sub-bands. Each is calibrated and imaged independently, then combined for the final image |

### scheduler.yaml

```yaml
scheduler: pbs

# Where job scripts and PBS -o/-e output land. Overrides <working_dir>/jobs.
job_dir: /home/me/charizard-runs/myproject/jobs

pbs:
  resource_style: select        # OpenPBS; use "nodes" for older Torque
  queues:
    default: workq
    gpu: gpu
  gpu:
    enabled: true
    host: node04                # pin GPU jobs to the node that has one
    ngpus: 1
  directives:
    - "-V"                      # export environment (picks up micromamba)
    - "-j oe"                   # merge stdout and stderr
  modules: []
  env_vars:
    OMP_NUM_THREADS: "8"
    NUMBA_NUM_THREADS: "8"      # numba reads this, NOT OMP_NUM_THREADS
    OPENBLAS_NUM_THREADS: "1"
    MKL_NUM_THREADS: "1"
```

Two things here bite hard if you get them wrong:

**`job_dir` usually has to be on `/home`, not `/scratch`.** Jobs write *data* to scratch
perfectly well, but many PBS installations never deliver the `.out` files there. Every job
then comes back as "No log files found" and the pipeline aborts steps that actually
succeeded. Give each dataset its own `job_dir`, too — job names repeat across runs, so a
shared directory means two datasets overwrite each other's logs and share one
`housekeeper.db`.

**Keep the thread limits equal to `ppn`.** udocker does not inherit the host environment;
these reach the container only because CHARIZARD forwards them explicitly. Left unset, each
library sizes its pool from the *node* core count while the scheduler confined you to
`ncpus=8`. Measured on a 64-core node with 4 concurrent jobs: catboss preparation took
1092 s uncapped versus 13 s capped.

## Running

```bash
charizard run pokedex.yaml
charizard run pokedex.yaml -s scheduler.yaml       # with a cluster config
charizard run pokedex.yaml -m my_models.yaml       # custom polcal models
```

Progress goes to the terminal, to `charizard.log`, and to `charizard.html` (the same log with
colour and structure, nicer for sending to someone). The run checkpoints as it goes, so
re-running the same config picks up where it left off rather than starting over.

## What the pipeline does

| Step | |
|---|---|
| 1 | **Analysing MS** — fields, SPWs, antennas, frequencies, feed basis |
| 2 | **Splitting** — into `cal.ms` and `src.ms`, one pair per sub-band |
| 3 | **Bad antenna detection** — dead dishes per scan → `badants.txt` |
| 4 | **Initial flagging** — autocorrelations, zeros, scan edges |
| 5 | **RFI flagging on calibrators** — catboss `pooh`, SumThreshold |
| 6 | **Finding the reference antenna** — scored on stability and flag fraction |
| 7 | **Calibration round 1** — delays, bandpass, gains, leakage, X–Y phase |
| 8 | **Post-cal flagging** — catboss `pooh` + `nimki` on corrected data |
| 9 | **Calibration round 2** — resolve on the cleaner data |
| 10 | **Apply to targets** |
| 11 | **Final flagging** |
| 12 | **Diagnostic plots** — shadems, optional |
| 13 | **Imaging and self-calibration** — optional |
| 14 | **Direction-dependent calibration** — peeling, optional |

## Output layout

```
working_dir/
├── spw0/
│   ├── cal.ms                  calibrator data
│   ├── src.ms                  target data
│   ├── caltables/              all solution tables
│   ├── plots/                  shadems diagnostics
│   ├── badants.txt
│   ├── refant.json             scores for every antenna
│   └── <field>/                per-target self-cal MSs and tables
├── spw1/ spw2/ spw3/ …
├── images/
│   └── <field>/                dirty, pcalN, apcalN, final_{I,Q,U,V}
├── ddcal_output/
│   └── <field>/                peeled images
├── <ms>.calplan                what was decided about each field
├── charizard.log
└── charizard.html
```

Job scripts and logs go to `job_dir` (see above), mirroring the same `spw*/` structure.

## MeerKAT and linear feeds

The feed basis is read from the MS `POLARIZATION` table and drives the calibration strategy
automatically. For linear feeds CHARIZARD follows the MeerKAT / IDIA `processMeerKAT`
recipe: `gaintype='G'` while only the parallel hands matter and `'T'` once the cross-hands
do, `parang=False` on the XX/YY-only solves, `bandtype='B'` with `fillgaps=8` on the
bandpass, and `Dflls` + `XYf+QU` for leakage and absolute polarisation angle.

Flux calibrators get an explicit `setjy` standard, because CASA's default (Perley-Butler
2017) contains no southern sources and would otherwise leave a 1 Jy point source as the
model without complaining:

| Calibrator | Standard |
|---|---|
| J1939-6342 (PKS B1934-638) | `Stevens-Reynolds 2016` |
| J0408-6545 (PKS B0407-65) | `manual` — refit from the SARAO log-polynomial |
| 3C286 / 3C48 / 3C147 / 3C138 | CASA default |

J0408-6545 has no CASA standard at all, so CHARIZARD converts SARAO's published
`log10(S) = a + b·log10(ν/MHz) + …` coefficients into setjy's `fluxdensity`/`spix` form.
Both are cubics in `log10(ν)`, so this is an exact reparametrisation rather than a fit — it
reproduces SARAO's own `curve_fit` snippet to six significant figures at any reference
frequency. The coefficients live in `charizard/data/internal_models.yaml` under
`manual_flux_models`; add your own there, or in a file passed with `-m`.

Field names match loosely, so `J1939-6342`, `1934-638` and `PKS B1934-638` all resolve to
the same model. Southern phase calibrators are **not** in the bundled catalog — set
`auto_detect: false` and list them explicitly. Names that are not real fields in the MS are
reported and dropped rather than silently handed to the split.

Multi-component sky models (recommended at L-band, close to essential at UHF) are not
applied automatically. Use `crystalball` to fill `MODEL_DATA` beforehand if you need them.

## Tuning self-calibration

`factor` is a single knob applied every round: `niter` is multiplied by it, while
`clean.threshold` and `solint` are divided by it. With `factor: 2`, `start_iters: 1000`,
`solint: 4min`, `threshold: 0.001`:

```
round      1        2        3        4
niter      1000     2000     4000     8000
solint     4min     2min     1min     30s      (floored at min_solint)
threshold  1e-3     5e-4     2.5e-4   1.25e-4  (floored at min_threshold)
```

Lower it to `1.5`, or raise `min_threshold`, if images come out over-cleaned. WSClean also
runs `-auto-threshold 3`, so early rounds are noise-limited at 3σ until the scheduled
threshold drops below it.

More rounds is not automatically better. Check that each round is actually earning its
place — compare the image rms and the integrated flux of a few bright sources across
`images/<field>/pcal*-MFS-image.fits`. If the rms stops falling, or source flux starts
dropping, stop there.

## Troubleshooting

**GPU not working after setup.** Re-run the GPU step on the machine that actually has the
GPU. Setup prints a per-stage probe and the first `FAIL` names the real cause:

```bash
charizard setup env --gpu-only --gpu
```
```
  driver (libcuda)     OK    libcuda.so.1 loads
  cupy                 OK    cupy 14.1.1, 1 device(s)
  jax                  FAIL  RuntimeError: jax sees no GPU
```

If the driver stage itself fails, point setup at the right directory with
`--cuda-lib-path`. Common locations: `/usr/lib64`, `/usr/lib/x86_64-linux-gnu`, `/usr/lib`,
`/usr/local/nvidia/lib64`, `/usr/local/cuda/compat`. Prefer the 64-bit tree — if the host
has both, `/usr/lib` is usually the 32-bit one.

**catboss reports no GPU.** Set `use_gpu: false` under `flagging:`. Flagging then runs on
CPU, which is slower but produces the same result. `nimki` is CPU-only regardless.

**Container not found.** Run `charizard setup env`. Containers are per-user.

**Every job reports "No log files found".** Your `job_dir` is almost certainly on a
filesystem the scheduler will not deliver `.out` files to. Move it to `/home`.

**Jobs fail immediately.** Look in `job_dir/<spw>/` for the `.out` file. The usual cause is
a `shell_preamble` that does not activate an environment containing udocker.

**Jobs die with `Failed AlwaysAssert getcwd`.** Concurrent jobs sharing one container were
deleting each other's working directory. Fixed — make sure you are on a current checkout.

## Known issues

**Self-calibration over-flags.** RFI flagging currently runs on the target data at the start
of every self-cal round, and the flags accumulate from round to round. On a MeerKAT S-band
track this took the target from 31% flagged to 73% across five rounds, cost roughly 20–30%
of the source flux, and left arc-like sidelobe residuals in the final images. Until this is
reworked, keep `loops.phase` and `loops.amp_phase` low (1–2 each), and check the flag
fraction in the self-cal MSs if images look worse than the dirty image.

## What's in the container

`apal52/charizard-pipeline:latest`

| Tool | Version | Used for |
|---|---|---|
| CASA | 6.7.3 | Splitting, calibration, applycal |
| WSClean | 3.6 | Imaging and self-cal |
| casacore | 3.6.0 | MS access |
| EveryBeam | 0.7.2 | Primary beam correction |
| IDG | latest | GPU-accelerated gridding |
| catboss `pooh` | 1.0 | RFI flagging, dynamic spectra (GPU) |
| catboss `nimki` | 1.0 | RFI flagging, UV-domain Gabor (CPU) |
| PyBDSF | latest | Source finding for DDCal |
| shadems | latest | Diagnostic plots |
| CrystalBall | latest | Sky-model prediction for peeling |
| QuartiCal | latest | Direction-dependent solves |
| foresight | 0.1 | Source masking from TGSS–NVSS |
| JAX / CuPy | cuda13 | GPU compute |

## License

MIT. See [LICENSE](LICENSE).

## Author

Arpan Pal — NCRA-TIFR
