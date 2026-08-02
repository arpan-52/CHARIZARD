#!/bin/bash
#SBATCH --job-name=charizard_master
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8G
#SBATCH --time=2-00:00:00
#SBATCH --export=ALL
#SBATCH --mail-type=END,FAIL
#SBATCH -D /path/to/working_dir                       # UPDATE: same as working_dir in pokedex.yaml
#SBATCH --output=/path/to/working_dir/charizard_master.out
#SBATCH --error=/path/to/working_dir/charizard_master.err

source ~/.bashrc
micromamba activate 312data                           # UPDATE: env with charizard + udocker
export UDOCKER_DIR=/path/to/udocker                   # UPDATE: udocker dir

charizard run pokedex.yaml                            # add "-s scheduler.yaml" for a custom scheduler config
