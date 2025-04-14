# This file contains the workflow to reproduce the current set of results
# It is not meant to be executed as a script, but rather to be run in the terminal
# You many need to adjust the paths or adjust variables to match your setup
# I ran all comands in zsh on a macOS or on the storrs hpc

setopt interactivecomments
bindkey -e
set -u

proj=gpta
pd=~/projects/$proj
ses=ppm_rb
wd=$pd/analysis/main/$ses
src=$pd/src

#----
#---- LULC data ----
#----

# Download the raw lulc data (to $pd/data/lulc/raw)

# lulc_reproject now calculates percent habitat, coarsens, and reprojects
$src/main/layers/lulc_reproject.r $pd/data/lulc/raw $pd/data/lulc/habmask_moll_pct

#----
#---- Prep bien ranges ----
#----

# See apr_10_ranges.sh

#----
#---- All ppm and rangebag species
#----

#----
#---- Local ----
#----

setopt interactivecomments
bindkey -e

proj=gpta
pd=~/projects/$proj
ses=ppm_rb
wd=$pd/analysis/main/$ses
src=$pd/src

cd $wd

# Set up HPC
wdr=${wd/$HOME/'~'}
pdr=${pd/$HOME/'~'}


ssh storrs "mkdir -p $pdr/data/lulc"
scp -r $pd/data/lulc/habmask_moll_pct storrs:$pdr/data/lulc

#----
#---- storrs ----
#----

ssh storrs

# Project variables
export proj=gpta
export pd=~/projects/$proj
export ses=ppm_rb
export wd=$pd/analysis/main/$ses
export src=$pd/src

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024

duckdb -c "select distinct(mod_type) from read_parquet('$ranges/manifest/*.parquet')"
# points, rangebag, ppm

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$ranges/manifest/*.parquet")
  where mod_type in ('rangebag', 'ppm')
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

cat $wd/ctfs/species.csv | wc -l #177,863

cp $pd/data/layer_map.csv $wd

# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#started before 12:00
# unsupported configuration: 500, 10G
# supported configuration: 450, 10G - pending
# 200, 8G, 4 hours - started immediately at 4:20pm. 10k in 30 min (scenario 3)
# 300, 8G, 45 min - started immediately at 8:30pm. 10k in 15 min (scenario 1)
# 350, 8G, 30 min - started immediately at 9:30pm. 10k in 15 min
# 400, 8G 6 hrs - started immediately at 1pm.
export n=400 #
export mpc=8G # #SBATCH --mem-per-cpu=20G
#export mem=30G
export p=general
export mail=NONE
export t=6:00:00

pars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc" #  # --mem $mem
exp="--export=ALL,n=$n,p=$p,mail=$mail,t=$t,mpc=$mpc" # mem=$mem

# Script parameters
ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
#TODO: output to scratch instead, it might be a little faster.
# e.g /scratch/mcu08001/bsc23001/random_10k/scenario3

#--- Scenario 1 - No dispersal, ecoregion domain
out=$wd/data/scenario1
export scriptPars="$ranges $out -k 25 -p mpi --verbose" #25 spp per task is ~5 min/task

#--- Scenario 2 - Dispersal, ecoregion domain
out=$wd/data/scenario2
export scriptPars="$ranges $out --dispersal -k 25 -p mpi --verbose"

#--- Scenario 3 - Dispersal, full domain
out=$wd/data/scenario3
export scriptPars="$ranges $out --dispersal --fulldomain -k 25 -p mpi --verbose"

#TODO: these should go to $out so I don't need to delete to run the next scenario
rm -r cluc_hpc_slurm.log
rm -r mpilogs

sbatch $pars $exp $src/poc/cluc/cluc_hpc_slurm.sh

#--- Check results
squeue --me

tail -25 cluc_hpc_slurm.log
cat cluc_hpc_slurm.log | wc -l

cat $wd/mpilogs/*.log
tail -25 $wd/mpilogs/MPI_1_*.log
tail -25 $wd/mpilogs/MPI_393_*.log
ls $wd/mpilogs/MPI_393_*.log

cat $out/spp_complete.csv | wc -l

# Errors
# TODO: distinct on the reason field
cat $out/spp_errors.csv | wc -l
cat $out/spp_errors.csv
head $out/spp_errors.csv

mlr --csv uniq -f reason $out/spp_errors.csv
mlr --csv count -g reason $out/spp_errors.csv

# Task Status
mlr --csv stats1 -a mean -f minutes $out/task_status.csv

# Memory use
mlr --csv stats1 -a max -f ps_mem_gib $out/mem_use.csv
mlr --csv stats1 -a mean -f ps_mem_gib $out/mem_use.csv

ls -l /scratch/mcu08001/bsc23001/tmp | head
df -H /scratch/mcu08001/bsc23001/tmp


scancel 16226967

ls -1 $out/attribution_ranges/tifs | wc -l

du -hsc $out
du -hsc $out/attribution_ranges/tifs

# commented b/c it was ruining syntax highlighting
duckdb -csv <<SQL
  select * from read_parquet('$out/pq/*.parquet') limit 10
SQL

#Clean up
rm cluc_hpc_slurm.log
rm -r $wd/mpilogs
rm -r $out


sacct -u $USER --format=JobID,JobName,Start,Elapsed | grep -v '\.'

#How long did a job sit before it was started?
sacct -j 16175837 --format=Elapsed
sacct -j 16175837 --format=Submit,Start

sacct -u $USER -S 2025-03-27 --partition general \
  --format=JobID,JobName,Partition,State,ExitCode,Elapsed,Start

sacct -j 16104616 --format=JobID,JobName,Partition,State,ExitCode,Elapsed,Start