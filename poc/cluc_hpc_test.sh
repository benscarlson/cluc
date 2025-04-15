# Used for testing the script on the hpc before running the full dataset
# The approach is to test a subset of the full dataset (using -n)
# Using increasingly complex apporaches on the HPC, leading up to a full run

#----
#---- On the HPC ----
#----

ssh storrs

#---
#--- Interactive, not parallel
#---

srun -n 1 --mem 20GB -p debug --pty bash

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

proj=cluc
pd=~/projects/$proj
ses=main
wd=$pd/analysis/$ses
src=$pd/src

cd $wd

ranges=/shared/mcu08001/bien_ranges/BIEN_Ranges_Apr11_2025/extracted
out=$wd/data/scenario3

export R_LIBS=~/rlibs
$src/main/cluc_hpc.r $ranges $out --dispersal --fulldomain -k 3 -n 6 -b

rm -r $out

#---- Interactive in parallel

srun -n 3 --mem 30GB -p debug --pty bash

module unload gcc
module load gdal/3.8.4 cuda/11.6 r/4.4.0

proj=cluc
pd=~/projects/$proj
ses=main
wd=$pd/analysis/$ses
src=$pd/src
#R_LIBS=~/rlibs

cd $wd

ranges=/shared/mcu08001/bien_ranges/BIEN_Ranges_Apr11_2025/extracted
out=$wd/data/scenario3

# Use shebag in script and pass in R_LIBS directly to mpirun instead of exporting
mpirun -n 3 -x R_LIBS=~/rlibs $src/main/cluc_hpc.r $ranges $out \
  --dispersal --fulldomain  -k 3 -p mpi -n 6 --verbose

/scratch/mcu08001/bsc23001/tmp

#---- Spot check all results
mlr --icsv --opprint cat $out/spp_errors.csv
mlr --icsv --opprint cat $out/script_status.csv
mlr --icsv --opprint cat $out/task_status.csv

cat $out/mpilogs/MPI_1_*
tail $out/mpilogs/MPI_1_*
cat $out/mpilogs/*

duckdb -csv -c "select * from read_parquet('$out/pq/*.parquet') limit 10"

mlr --icsv cat $out/spp_complete.csv | wc -l
ls -1 $out/attribution_ranges/tifs | wc -l

#-- Clean up
rm -r $out

#!!!! START HERE !!!!

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

#---
#--- Try a streamlined approach
#---

# Project variables
proj=cluc
pd=~/projects/$proj
ses=main
wd=$pd/analysis/$ses

src=$pd/src

cd $wd

# Slurm variables
#n=300 is the most you can request with 12G mem-per-cpu, so max 3600GB mem?
#started before 12:00
n=3 #
mpc=15G # #SBATCH --mem-per-cpu=20G
#mem=30G
p=debug
mail=NONE
t=10

# Make settings that will apply to all script runs in $wd
# TODO: use scratch drive
cat <<EOF > $wd/cluc_hpc_settings.yml
terraOptions:
  memmax: 6
  memfrac: 0.2
EOF

# Script parameters
ranges=/shared/mcu08001/bien_ranges/BIEN_Ranges_Apr11_2025/extracted
out=$wd/data/scenario3

slurmPars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc \
  --output=$out/cluc_hpc_slurm.log --error=$out/cluc_hpc_slurm.log" #  --mem $mem

scriptPars="$ranges $out --dispersal --fulldomain -k 3 -p mpi -n 6"

export src scriptPars #The slurm script needs access to src and scriptPars

sbatch $slurmPars --export=ALL $src/main/cluc_hpc_slurm.sh

#--- Check results
squeue -u bsc23001
cat $out/cluc_hpc_slurm.log
tail $out/cluc_hpc_slurm.log
tail $out/mpilogs/MPI_1_*.log
tail $out/mpilogs/*

# commented b/c it was ruining syntax highlighting
duckdb -csv <<SQL
  select * from read_parquet('$out/pq/*.parquet') limit 10
SQL

#Clean up
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
