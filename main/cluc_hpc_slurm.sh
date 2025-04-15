#!/bin/bash

#SBATCH --error=cluc_hpc_slurm.log
#SBATCH --output=cluc_hpc_slurm.log

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

export R_LIBS=~/rlibs

set -x
mpirun -n $SLURM_NTASKS Rscript --vanilla $src/main/cluc_hpc.r $scriptPars
