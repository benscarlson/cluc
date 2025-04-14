# Set up to test errors discovered during full runn
# See inspect_errors.sh for details

setopt interactivecomments
bindkey -e

proj=cluc
pd=~/projects/$proj
ses=errors/errors1
wd=$pd/analysis/poc/$ses
src=$pd/src

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd

#--- See bien_ranges/poc/download_subset.sh for code to download specific species
# task_num,spp,reason
# 1393,Abarema adenophorum,[rast] extents do not match
# 5475,Acacia fleckeri,In argument: `rast = list(...)`.
# 3622,Erythronium rostratum,[names<-] incorrect number of names

ranges=$wd/data/ranges

#---- Create the species control file
# Use the common 10k data set, but create a control file with a subset of species

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$ranges/manifest/*.parquet")
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

mlr --icsv --opprint cat $wd/ctfs/species.csv

#!!!! OLD CODE !!!!

#---- Make a settings file that will apply to all script runs
cat <<EOF > $wd/cluc_hpc_settings.yml
terraOptions:
  memmax: 12
  memfrac: 0.5
EOF

#----
#---- Test scenario1 in serial
#----

out=$wd/data/scenario1

$src/main/cluc_hpc.r $ranges $out -k 10 --verbose

#---- Check the results

cat $out/spp_errors.csv | wc -l # No errors
cat $out/task_status.csv

#---- Clean up
rm -r $out

#----
#---- Test scenario1 in parallel
#----

out=$wd/data/scenario1

$src/main/cluc_hpc.r $ranges $out -k 10 --verbose -p mc -c 5

#---- Check the results

cat $out/spp_errors.csv | wc -l # No errors
mlr --icsv --opprint cat $out/task_status.csv

#---- Clean up
rm -r $out
