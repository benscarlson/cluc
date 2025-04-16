# Test local run of cluc_hpc.r

setopt interactivecomments
bindkey -e

proj=cluc
pd=~/projects/$proj
ses=random_50
wd=$pd/analysis/poc/$ses
src=$pd/src

mkdir -p $wd
mkdir -p $wd/ctfs
mkdir -p $wd/data

cd $wd

#Note: these are old ranges but still work for testing
ranges=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024/random_10k

#---- Create the species control file
# Use the common 10k data set, but create a control file with a subset of species

duckdb <<SQL

copy (
  select distinct spp, 1 as run
  from read_parquet("$ranges/manifest/*.parquet")
  order by random()
  limit 50
) 
to '$wd/ctfs/species.csv' (header, delimiter ',')

SQL

head $wd/ctfs/species.csv

#---- Make a settings file that will apply to all script runs
cat <<EOF > $wd/cluc_hpc_settings.yml
terraOptions:
  memmax: 5
  memfrac: 0.1
basetempdir: /tmp/cluc
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

#----

ls /tmp/cluc
ls /tmp/cluc/cluc_894544cfdf18

#---- Check the results

cat $out/spp_errors.csv | wc -l # No errors
mlr --icsv --opprint cat $out/task_status.csv

#---- Clean up
rm -r $out
