#Used to investigate errors for a full run of all ppm, rb ranges on the hpc

ssh storrs

proj=gpta
pd=~/projects/$proj
ses=cluc/attribution_pct/all_ppm_rb
wd=$pd/analysis/poc/$ses
src=$pd/src

out=$wd/data/scenario1

cd $out

head -1 spp_errors.csv
# task_num,spp,reason

mlr --csv uniq -f reason spp_errors.csv

mlr --csv count -g reason then reorder -f count,reason then sort -f count spp_errors.csv

# count, reason
# 1,[names<-] incorrect number of names
# 17,In argument: `rast = list(...)`.
# 1969,[trim] only cells with NA found
# 213,Skipping. All rasters cells are NaN
# 3054,[rast] extents do not match

mlr --csv tail -n +3923 spp_errors.csv | head -n 2

sed -n '3923p' spp_errors.csv
sed -n '3920,3925p' spp_errors.csv

# Note 'h' in one of the lines
# 6264,Vinca herbacea,[rast] extents do not match
# 6277,Passiflora chelidonea,[rast] extents do not match
# 6277,Passovia bisexualis,[trim] only cells with NA found
# h
# 6164,Mimosa balansae,[rast] extents do not match
# 6257,Vaccinium oxycoccus,[rast] extents do not match

sed -n '5p' file.txt
# It means:
# 
# -n: Don’t automatically print every line (suppresses the default behavior)
# 
# '5p': When you get to line 5, print it

mlr --csv filter '$reason == "[rast] extents do not match"' then head -n 1 spp_errors.csv

#Note: make sure to use ' not " so special characters are taken literally
error='[rast] extents do not match'
error='In argument: `rast = list(...)`.'
error='[names<-] incorrect number of names'

mlr --csv filter "\$reason == \"$error\"" then head -n 1 spp_errors.csv

#Here is the first species with each of these errors

# task_num,spp,reason
# 1393,Abarema adenophorum,[rast] extents do not match. 
# 5475,Acacia fleckeri,In argument: `rast = list(...)`. Issue: Data error. Only has present, no future ranges
# 3622,Erythronium rostratum,[names<-] incorrect number of names. Issue: Data error. Double entry for all future versions

# Need to know if any of these errors are in the ppm ranges, since I could run new versions of those

rangesP=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
sppErrPF=$out/spp_errors.csv

# Only one ppm error for extent issue. all others and all other errors are in the rangebag data
duckdb <<SQL
  SELECT distinct spp, mod_type, reason
  FROM read_parquet('${rangesP}/manifest/*.parquet') as pq
  INNER JOIN read_csv('$sppErrPF',ignore_errors=true) AS err
  using(spp)
  where err.reason = '$error'
SQL

