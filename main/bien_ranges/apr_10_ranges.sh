setopt interactivecomments
bindkey -e
set -u

ssh storrs

set -u

pd=~/projects/cluc
wd=$pd/analysis/main
src=$pd/src

mkdir -p $wd

cd $wd

rawRangesP=/shared/mcu08001/bien_ranges #/

mkdir -p $rawRangesP

# ls /shared/mcu08001/bien_ranges/BIEN_Ranges_Oct18_2024 #Older ranges

#Change dl=0 to dl=1

url="https://www.dropbox.com/scl/fo/byzcl9cae1k1x52wxkrj1/AJpPk9ycWFK4TtxKQXP0gIc?rlkey=4lkgckpekvwi2ims6zkmm5x1x&e=1&st=npo5c8st&dl=1"


#The folder is called "PPM" on dropbox. Dropbox sends all files in a single zip file.
# Use nohup and & to run the process in the background.
# 2>&1 appends standard err to std out, both go to download.log
# --progress=dot:giga is supposed to send less progress to the output, but not sure if it is working
nohup wget --progress=dot:giga -c -O $rawRangesP/BIEN_Ranges_Apr11_2025_PPM.zip "$url" > download.log 2>&1 &

# Check progress
tail -50 $wd/download.log
du -hsc $wd/download.log

# Kill if necessary
pgrep -fl wget #get the process id
kill 2390409

ls -lh $rawRangesP

#---- Inspect the zip archive
rawRangesPF=$rawRangesP/BIEN_Ranges_Apr11_2025_PPM.zip
aprRangesP=$rawRangesP/BIEN_Ranges_Apr11_2025

# Prints the size of the unzipped zip file
unzip -Zt $rawRangesPF | awk '{printf "%.2fGB\n", $3/1024/1024/1024;}'

# List the files
unzip -l $rawRangesPF

cd $rawRangesP


# Note Cory only provided ppms at this time
mkdir -p $aprRangesP/raw/ppm
nohup time unzip -o $rawRangesPF -d $aprRangesP/raw/ppm > unzip.log 2>&1 &
# UNZIP_DISABLE_ZIPBOMB_DETECTION=TRUE # Use this if you get a zip bomb error

#--- Check progress
tail unzip.log
pgrep -fl unzip #get the process id to check the status of a

#Need to also retreive 2611.zip since that was added after I downloaded the folder
url2611="https://www.dropbox.com/scl/fo/byzcl9cae1k1x52wxkrj1/AJpPk9ycWFK4TtxKQXP0gIc?e=3&preview=2611.zip&rlkey=4lkgckpekvwi2ims6zkmm5x1x&st=b78p45o1&dl=1"
time wget --progress=dot:giga -c -O $aprRangesP/raw/ppm/2611.zip "$url2611"


#--- Look at the results
cd $aprRangesP/raw/ppm
unzip -l BinaryMaps.zip | head -10

#This is how to extract the species names
#This will print all the species folders so be careful!
#I checked each folder above BinaryMaps it only contains one folder
zipinfo -1 BinaryMaps.zip | \
  grep '^Users/ctg/Documents/SDMs/BIEN_1123/_outputs/PPM/BinaryMaps' | \
  awk -F'/' '{print $9}' | \
  sort -u | head -10
  
#---- Create the manifest and extract the tifs

# See create_manifest.sh

#!!!! START HERE !!!!

#----
#---- OLD CODE below here -----
#----







#---- Local
setopt interactivecomments
bindkey -e

rangesP=~/projects/bien_ranges/data/BIEN_Ranges_Oct18_2024
rangesPR=${rangesP/$HOME/\~} # Remote version of rangesP

mkdir $rangesP
scp -r storrs:$rangesPR/manifest $rangesP

duckdb -c "DESCRIBE SELECT * FROM read_parquet('${rangesP}/manifest/*.parquet')"

#RUN this and look at 'abarema adenophorum'. It doesn't have the species name but instead start with "full_noBias..."
# Is this an error somewhere?
# Also, Abarema_cochliacarpos and Abarema_cochliocarpos differ only by the 'ia' vs. 'io'. strange
duckdb <<SQL
  SELECT * 
  FROM 'manifest/7011_ppm.parquet'
  limit 10;
SQL

┌──────────────────────┬───────┬───────┬──────────┬──────────┬──────────────────────────────────────────────────────────────────────────┐
│         spp          │  rcp  │ year  │ scenario │ mod_type │                                   path                                   │
│       varchar        │ int32 │ int32 │ varchar  │ varchar  │                                 varchar                                  │
├──────────────────────┼───────┼───────┼──────────┼──────────┼──────────────────────────────────────────────────────────────────────────┤
│ Aa mathewsii         │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Aa_mathewsii__full__noBias_1e-06_0_1e-06_0_all_all_none_all_m…  │
│ Aaronsohnia factor…  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Aaronsohnia_factorovskyi__full__noBias_1e-06_0_1e-06_0_all_al…  │
│ Aaronsohnia pubesc…  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Aaronsohnia_pubescens__full__noBias_1e-06_0_1e-06_0_all_all_n…  │
│ Abarema adenophorum  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/full__noBias_1e-06_0_1e-06_0_all_all_none_all_maxnet_none_equ…  │
│ Abarema auriculata   │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_auriculata__full__noBias_1e-06_0_1e-06_0_all_all_none…  │
│ Abarema barbouriana  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_barbouriana__full__noBias_1e-06_0_1e-06_0_all_all_non…  │
│ Abarema brachystac…  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_brachystachya__full__noBias_1e-06_0_1e-06_0_all_all_n…  │
│ Abarema cochleata    │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_cochleata__full__noBias_1e-06_0_1e-06_0_all_all_none_…  │
│ Abarema cochliacar…  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_cochliacarpos__full__noBias_1e-06_0_1e-06_0_all_all_n…  │
│ Abarema cochliocar…  │    70 │  2011 │ 7011     │ ppm      │ 7011/ppm/Abarema_cochliocarpos__full__noBias_1e-06_0_1e-06_0_all_all_n…  │
├──────────────────────┴───────┴───────┴──────────┴──────────┴──────────────────────────────────────────────────────────────────────────┤
│ 10 rows                                                                                                                     6 columns │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

