#!/bin/bash
# Master job for the MeerKAT run on bhima (OpenPBS).
# This process only orchestrates - it submits the real work as child jobs and
# waits, so it needs 1 core and a long walltime, not much else.
#
#   qsub submit_meerkat.sh
#
#PBS -N charizard_meerkat
#PBS -l select=1:ncpus=1:mem=8gb
#PBS -l walltime=2:00:00:00
#PBS -j oe
#PBS -V
#PBS -o /home/YOUR_USER/data/charizard_meerkat.log   # UPDATE

source ~/.bashrc
micromamba activate 312data
export UDOCKER_DIR=/home/YOUR_USER/udocker           # UPDATE

cd /home/YOUR_USER/data                              # UPDATE: same as working_dir

charizard run pokedex_meerkat.yaml -s scheduler_bhima.yaml
