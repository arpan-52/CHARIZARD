#!/bin/bash
#SBATCH --job-name=charizard_master
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=8G
#SBATCH --time=2-00:00:00
#SBATCH --export=ALL
#SBATCH --mail-type=END,FAIL
#SBATCH -D /lustre/aoc/students/apal/a725
#SBATCH --output=/lustre/aoc/students/apal/a725/charizard_master.out
#SBATCH --error=/lustre/aoc/students/apal/a725/charizard_master.err

source /lustre/aoc/students/apal/start.sh
micromamba activate 312data
export UDOCKER_DIR=/lustre/aoc/students/apal/udocker

charizard run pokedex.yaml -s scheduler.yaml
