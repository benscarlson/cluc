# copied cluc/attribution.sh
# testing updates to cluc_hpc.r to attribute loss or gain reason
# using land cover as a percent raster

setopt interactivecomments
bindkey -e

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct
wd=$pd/analysis/poc/$ses
src=$pd/src

# Variables for reporting
# RPT_HOME=~/projects/reports/reports/docs
# pubOutP=$RPT_HOME/$proj/$ses
# mkdir -p $pubOutP #Didn't run this yet.

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd

#---- Set up files

#NOTE: I moved the working directory to atttribution_pct/random_50

# See oct18_ranges.sh for code to download these rasters from the HPC
# Put these in the general gpta/data folder since I keep reusing them
mkdir -p $pd/data/bien_ranges/oct18_ranges
cp -r $pd/analysis/poc/cluc/oct18_ranges/random_50/data/random_50 $pd/data/bien_ranges/oct18_ranges

#Copy the species files from a previous session. I should really make this from the manifest file.
cp $pd/analysis/poc/cluc/oct18_ranges/random_50/ctfs/species.csv $wd/ctfs

# Copy the common layer_map file
cp $pd/analysis/poc/cluc/config_files/layer_map.csv $wd

#---- Run scenarios

random_50=$pd/data/bien_ranges/oct18_ranges/random_50

# Scenario 3 -- I tested using scenario3 so try this first.
out=$wd/data/scenario3
$src/poc/cluc/cluc_hpc.r $random_50 $out --dispersal --fulldomain -k 10 -p mc -c 5 #~3 min

# Try scenarios 1 and 2 just to see if they break
out=$wd/data/scenario1
$src/poc/cluc/cluc_hpc.r $random_50 $out -k 10 -p mc -c 5

out=$wd/data/scenario2
$src/poc/cluc/cluc_hpc.r $random_50 $out --dispersal -k 10 -p mc -c 5 #~2.5 min

# The scenarios worked.

#-------------------
#---- random_1k ----
#-------------------

# Try a larger dataset of 1k species ----

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_1k
wd=$pd/analysis/poc/$ses
src=$pd/src

# Variables for reporting
# RPT_HOME=~/projects/reports/reports/docs
# pubOutP=$RPT_HOME/$proj/$ses
# mkdir -p $pubOutP #Didn't run this yet.

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd


# ctfs/species.csv
# Use the common 10k data set, but create a control file with a subset of species
oct18_10k=$pd/data/bien_ranges/oct18_ranges/oct18_10k

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$oct18_10k/manifest/*.parquet")
  order by random()
  limit 1000
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

# layer_map.csv
cp $pd/analysis/poc/cluc/config_files/layer_map.csv $wd

# Try scenarios 1 and 2 just to see if they break
out=$wd/data/scenario1
$src/poc/cluc/cluc_hpc.r $oct18_10k $out -k 20 -p mc -c 9 --resume # Chunk of 20 takes 3-5 min

#--------------------
#---- random_10k ----
#--------------------

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_10k
wd=$pd/analysis/poc/$ses
src=$pd/src

# Variables for reporting
# RPT_HOME=~/projects/reports/reports/docs
# pubOutP=$RPT_HOME/$proj/$ses
# mkdir -p $pubOutP #Didn't run this yet.

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd


# ctfs/species.csv
# Use the common 10k data set, but create a control file with a subset of species
oct18_10k=$pd/data/bien_ranges/oct18_ranges/oct18_10k

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$oct18_10k/manifest/*.parquet")
  order by spp
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

# layer_map.csv
cp $pd/analysis/poc/cluc/config_files/layer_map.csv $wd

# Try scenarios 1 and 2 just to see if they break
# Started at 5:10 - should have only 4500
out=$wd/data/scenario1
$src/poc/cluc/cluc_hpc.r $oct18_10k $out -k 10 -p mc -c 9 # Chunk of 20 takes 3-5 min

# Scenario 3
# started at 11:25 - should have all 10k
out=$wd/data/scenario3
$src/poc/cluc/cluc_hpc.r $oct18_10k $out --dispersal --fulldomain -k 20 -p mc -c 9

#-------------------------
#---- random_10k_test ----
#-------------------------

setopt interactivecomments
bindkey -e

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_10k_test
wd=$pd/analysis/poc/$ses
src=$pd/src

# Variables for reporting
# RPT_HOME=~/projects/reports/reports/docs
# pubOutP=$RPT_HOME/$proj/$ses
# mkdir -p $pubOutP #Didn't run this yet.

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd


# ctfs/species.csv
# Use the common 10k data set, but create a local control file
oct18_10k=$pd/data/bien_ranges/oct18_ranges/oct18_10k

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$oct18_10k/manifest/*.parquet")
  order by spp
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

# layer_map.csv
cp $pd/analysis/poc/cluc/config_files/layer_map.csv $wd

# # Try scenarios 1 and 2 just to see if they break
# # Started at 5:10 - should have only 4500
# out=$wd/data/scenario1
# $src/poc/cluc/cluc_hpc.r $oct18_10k $out -k 10 -p mc -c 9 # Chunk of 20 takes 3-5 min

# Scenario 3
# started at 11:25 - should have all 10k
out=$wd/data/scenario3
$src/poc/cluc/cluc_hpc.r $oct18_10k $out --dispersal --fulldomain -k 5 -p mc -c 2

top -stats pid,command,rsize,mem -o rsize

pid=18490
$src/poc/monitor_mem.sh $pid $out/mem_${pid}.log

#PID=10273
#LOGFILE=~/projects/gpta/analysis/poc/cluc/attribution_pct/random_10k_test/mem_10273.log

#-------------------------
#---- random_10k on the HPC ----
#-------------------------

#----
#---- Local ----
#----

# On storrs, the oct18 ranges are in projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
# Upload the random_10k control file to the HPC and use this as an index into the oct18 ranges

setopt interactivecomments
bindkey -e

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_10k
wd=$pd/analysis/poc/$ses
src=$pd/src

cd $wd

# Set up HPC
wdr=${wd/$HOME/'~'}
pdr=${pd/$HOME/'~'}

ssh storrs "mkdir -p $wdr" #NOTE update all remote_wd to wdr
ssh storrs "mkdir -p $remote_wd/ctfs"
ssh storrs "mkdir -p $remote_wd/data"

scp $wd/ctfs/species.csv storrs:$remote_wd/ctfs
scp $wd/layer_map.csv storrs:$remote_wd #TODO: don't using this in the future. See below.

#Copy the master layer_map.csv file to storrs
# Any new session should copy from storrs instead of here.
# TODO: consider adding these core rules definition files to src?
scp $pd/data/layer_map.csv storrs:~/projects/gpta/data

ssh storrs "mkdir -p $pdr/data/lulc"
scp -r $pd/data/lulc/habmask_moll_pct storrs:$pdr/data/lulc

#---- Download results

#Download the attribution pq data
mkdir $wd/data/scenario1
scp storrs:$wdr/data/scenario1/pq.tar $wd/data/scenario1
tar -tf $wd/data/scenario1/pq.tar
tar -xf $wd/data/scenario1/pq.tar -C $wd/data/scenario1

#----
#---- On the HPC ----
#----

#Copied this from oct18_ranges.sh then updated

ssh storrs

#---
#--- Interactive, not parallel
#---

srun -n 1 --mem 20GB -p debug --pty bash

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_10k
wd=$pd/analysis/poc/$ses
src=$pd/src
R_LIBS=~/rlibs

cd $wd

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
out=$wd/data/scenario3

$src/poc/cluc/cluc_hpc.r $ranges $out --dispersal --fulldomain -k 3 -n 6 -b


#---- Interactive in parallel

srun -n 3 --mem 30GB -p debug --pty bash

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/random_10k
wd=$pd/analysis/poc/$ses
src=$pd/src
#R_LIBS=~/rlibs

cd $wd

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
out=$wd/data/scenario3

# Use shebag in script and pass in R_LIBS directly to mpirun instead of exporting
mpirun -n 3 -x R_LIBS=~/rlibs $src/poc/cluc/cluc_hpc.r $ranges $out \
  --dispersal --fulldomain  -k 3 -p mpi -n 6 --verbose

/scratch/mcu08001/bsc23001/tmp

cat data/scenario3/spp_errors.csv
cat mpilogs/MPI_1_*
tail mpilogs/MPI_1_*
cat mpilogs/*

# Commented because it was runing syntax highlighting.
duckdb -csv <<SQL
  select * from read_parquet('$out/pq/*.parquet') limit 10
SQL

#-- Clean up
rm -r $out
rm -r mpilogs

#----
#---- SLURM script, debug run
#----

ssh storrs

# Project variables
export proj=gpta
export pd=~/projects/$proj
export ses=cluc/attribution_pct/random_10k
export wd=$pd/analysis/poc/$ses
export src=$pd/src

cd $wd

# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#started before 12:00
export n=3 #
#export mpc=12G # #SBATCH --mem-per-cpu=20G
export mem=30G
export p=debug
export mail=NONE
export t=10

# These have to start with the --option b/c echo won't print - as first character
#Note that 'pars' has the names of the parameters e.g "--mem-per-cpu $mpc"
# while 'exp' is the variable name. e.g. mpc=$mpc
#TODO: check again to see if this is all necessary!
#also, I dont' need $(). Just use "", see below
# pars=$(echo --ntasks $n -p $p --time $t --mail-type $mail --mem $mem) # --mem-per-cpu $mpc
# exp=$(echo --export=ALL,n=$n,p=$p,mail=$mail,t=$t,mem=$mem) #mpc=$mpc

pars="--ntasks $n -p $p --time $t --mail-type $mail --mem $mem" # --mem-per-cpu $mpc
exp="--export=ALL,n=$n,p=$p,mail=$mail,t=$t,mem=$mem" #mpc=$mpc

# Script parameters
ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
out=$wd/data/scenario3

export scriptPars="$ranges $out --dispersal --fulldomain -k 3 -p mpi -n 6"

sbatch $pars $exp $src/poc/cluc/cluc_hpc_slurm.sh

#--- Check results
squeue -u bsc23001
cat *.log
cat $wd/mpilogs/*.log

# commented b/c it was ruining syntax highlighting
duckdb -csv <<SQL
  select * from read_parquet('$out/pq/*.parquet') limit 10
SQL

#Clean up
rm cluc_hpc_slurm.log
rm -r $wd/mpilogs
rm -r $out


#How long did a job sit before it was started?
sacct -j 8872803 --format=Elapsed
sacct -j 8872803 --format=Submit,Start

#----
#---- SLURM script, full run
#----

ssh storrs

# Project variables
export proj=gpta
export pd=~/projects/$proj
export ses=cluc/attribution_pct/random_10k
export wd=$pd/analysis/poc/$ses
export src=$pd/src

cd $wd

# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#started before 12:00
# unsupported configuration: 500, 10G
# supported configuration: 450, 10G - pending
# 200, 8G, 4 hours - started immediately at 4:20pm. 10k in 30 min (scenario 3)
# 300, 8G, 45 min - started immediately at 8:30pm. 10k in 15 min (scenario 1)
# 350, 8G, 30 min - started immediately at 9:30pm. 10k in 15 min
export n=350 #
export mpc=8G # #SBATCH --mem-per-cpu=20G
#export mem=30G
export p=general
export mail=NONE
export t=30
# 10k spp completed in 30 min

# These have to start with the --option b/c echo won't print - as first character
#Note that 'pars' has the names of the parameters e.g "--mem-per-cpu $mpc"
# while 'exp' is the variable name. e.g. mpc=$mpc
#TODO: check again to see if this is all necessary!
#also, I dont' need $(). Just use "", see below
# pars=$(echo --ntasks $n -p $p --time $t --mail-type $mail --mem $mem) # --mem-per-cpu $mpc
# exp=$(echo --export=ALL,n=$n,p=$p,mail=$mail,t=$t,mem=$mem) #mpc=$mpc

pars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc" #  # --mem $mem
exp="--export=ALL,n=$n,p=$p,mail=$mail,t=$t,mpc=$mpc" # mem=$mem

# Script parameters
ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
#TODO: output to scratch instead.
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
cat $wd/mpilogs/*.log
tail -25 $wd/mpilogs/MPI_1_*.log

cat $out/spp_complete.csv | wc -l

# Task Status
mlr --csv stats1 -a mean -f minutes $out/task_status.csv

# Memory use
mlr --csv stats1 -a max -f ps_mem_gib $out/mem_use.csv
mlr --csv stats1 -a mean -f ps_mem_gib $out/mem_use.csv

ls /scratch/mcu08001/bsc23001/tmp | head


scancel 16178829
# prterun noticed that process rank 17 with PID 3063175 on node cn517 exited on
# signal 9 (Killed).
cat $wd/mpilogs/MPI_17_bsc23001_3063175.log

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

#How long did a job sit before it was started?
sacct -j 16175837 --format=Elapsed
sacct -j 16175837 --format=Submit,Start

sacct -u $USER -S 2025-03-27 --partition general \
  --format=JobID,JobName,Partition,State,ExitCode,Elapsed,Start

sacct -j 16104616 --format=JobID,JobName,Partition,State,ExitCode,Elapsed,Start

#---- Prep results

tar -cf $wd/data/scenario1/pq.tar -C $wd/data/scenario1 pq

#----
#---- All ppm and rangebag species
#----

ssh storrs

# Project variables
export proj=gpta
export pd=~/projects/$proj
export ses=cluc/attribution_pct/all_ppm_rb
export wd=$pd/analysis/poc/$ses
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

#---- OLD CODE BELOW HERE ---

#----
#---- SLURM script with 5k species
#----

ssh storrs

# put different script runs into different session folders
# Commented b/c it was ruining syntax highlighting
# shopt -s extglob
# cp -r !(random_50) random_50/
# mv !(random_ppm_5k|random_50) random_ppm_5k/

# Project variables
export pd=~/projects/gpta
#export wd=$pd/analysis/poc/threat_range_metrics/test_100k_spp
export wd=$pd/analysis/poc/cluc/oct18_ranges/random_ppm_5k
export src=$pd/src

cd $wd

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024

# Generate the control file for 5k random ppm species
# Comented b/c it was ruining syntax highlighting
# duckdb -csv <<SQL
#   copy(
#     select *, 1 as run from (
#       select distinct spp 
#       from read_parquet('$ranges/manifest/*.parquet')
#       where mod_type = 'ppm'
#       order by random()
#       limit 5000)
#     order by spp)
#   to 'ctfs/species.csv'
#   WITH (HEADER, DELIMITER ',');
# SQL


# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#n=500 with 2500G ram is not available
#started before 12:00
# 300 w/ 10G mpc, 5k spp, 5 spp per task ran in 5 min! (<1 min per task)
export n=300 #
export mpc=10G # #SBATCH --mem-per-cpu=20G
#export mem=600GB #Try 10G per node using "mem" kept giving me invalid configuration
export p=general
export mail=NONE
export t=2:00:00

# These have to start with the --option b/c echo won't print - as first character
#Note that 'pars' has the names of the parameters e.g "--mem-per-cpu $mpc"
# while 'exp' is the variable name. e.g. mpc=$mpc
#TODO: check again to see if this is all necessary!
#also, I dont' need $(). Just use "", see below
# pars=$(echo --ntasks $n -p $p --time $t --mail-type $mail --mem $mem) # --mem-per-cpu $mpc
# exp=$(echo --export=ALL,n=$n,p=$p,mail=$mail,t=$t,mem=$mem) #mpc=$mpc

pars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc" #--mem $mem
exp="--export=ALL,n=$n,p=$p,mail=$mail,t=$t,mpc=$mpc" #mem=$mem

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024

out=$wd/data/scenario1
export scriptPars="$ranges $out -k 5 -p mpi" #Scenario 1

#export scriptPars="$ranges $out --dispersal --fulldomain -k 5 -p mpi"

sbatch $pars $exp $src/poc/cluc/cluc_hpc_slurm.sh

#--- Check results
squeue -u bsc23001
tail *.log 
# commented b/c it was ruining syntax highlighting
# duckdb -csv <<SQL
#   select * from read_parquet('$wd/data/scenario1/pq/*.parquet') limit 10
# SQL

mlr --csv filter '$success==FALSE' $wd/data/scenario1/task_status.csv

#----
#---- SLURM script with all ppm and rangebag species
#----

ssh storrs

# Project variables
export pd=~/projects/gpta
# export wd=$pd/analysis/poc/threat_range_metrics/test_100k_spp
export wd=$pd/analysis/poc/cluc/oct18_ranges/ppm_rangebag_all
export src=$pd/src

cd $wd

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024

# Generate the control file for 5k random ppm species
# Commented b/c it was ruining syntax highlighting
# duckdb -csv <<SQL
#   copy(
#       select distinct spp, 1 as run 
#       from read_parquet('$ranges/manifest/*.parquet')
#       where mod_type in ('rangebag','ppm')
#       order by spp)
#   to 'ctfs/species.csv'
#   WITH (HEADER, DELIMITER ',');
# SQL

cat ctfs/species.csv | wc -l 

# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#n=500 with 2500G ram is not available
#n=350, mpc=10G --Got this easily
#started before 12:00
export n=350 #
export mpc=10G # #SBATCH --mem-per-cpu=20G
#export mem=600GB #Try 10G per node using "mem" kept giving me invalid configuration
export p=general
export mail=NONE
export t=3:00:00

# These have to start with the --option b/c echo won't print - as first character
#Note that 'pars' has the names of the parameters e.g "--mem-per-cpu $mpc"
# while 'exp' is the variable name. e.g. mpc=$mpc
#TODO: check again to see if this is all necessary!
#also, I dont' need $(). Just use "", see below
# pars=$(echo --ntasks $n -p $p --time $t --mail-type $mail --mem $mem) # --mem-per-cpu $mpc
# exp=$(echo --export=ALL,n=$n,p=$p,mail=$mail,t=$t,mem=$mem) #mpc=$mpc

pars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc" #--mem $mem
exp="--export=ALL,n=$n,p=$p,mail=$mail,t=$t,mpc=$mpc" #mem=$mem

ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
