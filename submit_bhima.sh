#!/bin/bash
#PBS -N charizard_master
#PBS -l select=1:ncpus=1:mem=8gb
#PBS -l walltime=2:00:00:00
#PBS -j oe
#PBS -V
#PBS -o /home/YOUR_USER/data/charizard_master.log   # UPDATE

source ~/.bashrc
micromamba activate 312data
export UDOCKER_DIR=/home/YOUR_USER/udocker           # UPDATE

cd /home/YOUR_USER/data                              # UPDATE: same as working_dir in pokedex

charizard run pokedex_bhima.yaml -s scheduler_bhima.yaml
