#!/bin/bash

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

export R_LIBS=~/rlibs

set -x
mpirun -n $SLURM_NTASKS Rscript --vanilla $src/main/cluc_hpc.r $scriptPars
