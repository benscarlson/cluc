# This file contains the workflow to reproduce the current set of results
# It is not meant to be executed as a script, but rather to be run in the terminal
# You many need to adjust the paths or adjust variables to match your setup
# I ran all comands in zsh on a macOS or on the storrs hpc

setopt interactivecomments
bindkey -e
#set -u

proj=cluc
pd=~/projects/$proj
ses=main
wd=$pd/analysis/$ses
src=$pd/src

#----
#---- Attach to a github repo
#----

git remote add origin git@github.com:benscarlson/cluc.git
git push -u origin main

#----
#---- LULC data ----
#----

# Download the raw lulc data (to $pd/data/lulc/raw)
# TODO: find the code

# lulc_reproject now calculates percent habitat, coarsens, and reprojects
$src/main/layers/lulc_reproject.r $pd/data/lulc/raw $pd/data/lulc/habmask_moll_pct

#----
#---- Prep bien ranges ----
#----

# See apr_10_ranges.sh

#----
#---- Set up project on storrs
#----

# Set up HPC
wdr=${wd/$HOME/'~'}
pdr=${pd/$HOME/'~'}

ssh storrs "
  mkdir -p $wdr
  mkdir -p $wdr/ctfs
  mkdir -p $wdr/data
  mkdir -p $pdr/data/lulc
"

# Upload the project file
scp $pd/cluc.Rproj storrs:$pdr

# Transfer LULC data
scp -r $pd/data/lulc/habmask_moll_pct storrs:$pdr/data/lulc

#----
#---- storrs ----
#----

ssh storrs

# Project variables
proj=cluc
pd=~/projects/$proj
ses=main
wd=$pd/analysis/$ses
src=$pd/src

cd $wd

#----
#---- Set up the code on storrs
#----

git clone git@github.com:benscarlson/cluc.git $src

#----
#---- Set up the species control file
#----

ranges=/shared/mcu08001/bien_ranges/BIEN_Ranges_Apr11_2025/extracted

# Note only ppm data as of now
duckdb -c "select distinct(mod_type) from read_parquet('$ranges/manifest/*.parquet')"

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$ranges/manifest/*.parquet")
  where mod_type in ('rangebag', 'ppm')
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

cat $wd/ctfs/species.csv | wc -l #78,905

#----
#---- Run the attribution script
#----

# Slurm variables
# New settings from storrs says you can only request 250 tasks.
#  I tried requesting more and did not start after 5-10 min
# 250, 8G, 2 hours -- started immediately.
# 250, 10G, 2 hours -- started immediately.
# 250, 12G, 2 hours -- started immediately.
# 250, 12G, 2 hours -- started immediately.
# 250, 15G, 3 hours -- started immediately. <--- use this one
# 250, 16G, 2 hours -- did not start
# 250, 20G, 2 hours -- configuration not availabe
# 250, 15G, 4 hours -- error about binding too many cores. But request of 2 hours worked
n=250 # #SBATCH --ntasks
mpc=15G # #SBATCH --mem-per-cpu
#mem=30G
p=general
mail=NONE # #SBATCH --mail-type
t=2:00:00 # #SBATCH --time

#Set up the scratch directory for temporary terra files
#mkdir -p /scratch/mcu08001/bsc23001/tmp

# Make settings that will apply to all script runs in $wd
# max: 6, frac: 0.3 died with 8G
# max: 5, frac: 0.2 died with 8G
# max: 5, frac: 0.2 died with 10G
# max: 7, frac: 0.2 died with 15G
# max: 7, frac: 0.1 completed
# Seems I can't set memfrac above 0.1
cat <<EOF > $wd/cluc_hpc_settings.yml
terraOptions:
  memmax: 7
  memfrac: 0.1
basetempdir: /scratch/mcu08001/bsc23001/tmp
EOF

# Script parameters
ranges=/shared/mcu08001/bien_ranges/BIEN_Ranges_Apr11_2025/extracted

#---- Scenario 1
out=$wd/data/scenario1

scriptPars="$ranges $out -k 10 -p mpi --verbose"

# Sbatch parameters
slurmPars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc \
  --output=$out/cluc_hpc_slurm.log --error=$out/cluc_hpc_slurm.log" #  --mem $mem

export src scriptPars #The slurm script needs access to src and scriptPars

sbatch $slurmPars --export=ALL $src/main/cluc_hpc_slurm.sh

#---- Scenario 3
# Note: resumed the process halfway through

out=$wd/data/scenario3

scriptPars="$ranges $out --dispersal --fulldomain -k 10 -p mpi --verbose --resume"

# Sbatch parameters
slurmPars="--ntasks $n -p $p --time $t --mail-type $mail --mem-per-cpu $mpc \
  --output=$out/cluc_hpc_slurm.log --error=$out/cluc_hpc_slurm.log" #  --mem $mem

export src scriptPars #The slurm script needs access to src and scriptPars

sbatch $slurmPars --export=ALL $src/main/cluc_hpc_slurm.sh

#---
#--- Check running process and results
#---

# See cluc_hpc_check.sh for commands to examine the running process and results

#--- Clean up

rm -r $out

#If process failed tmp might not be cleaned up
ls /scratch/mcu08001/bsc23001/tmp
rm -r /scratch/mcu08001/bsc23001/tmp 



#!!!! OLD CODE BELOW HERE !!!!!


# To test with smaller subsets
# see src/poc/cluc_hpc_test.sh

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

