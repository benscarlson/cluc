
#---- Creates the manifest file and extracts all tifs from bien range zip archives
#---- The manifest is stored one parquet file per zip archive



rawRangesP=/shared/mcu08001/bien_ranges # location of all raw zip files
aprRangesP=$rawRangesP/BIEN_Ranges_Apr11_2025 # location of scenario zip files

manP=$aprRangesP/extracted/manifest # manifest directory
tifsP=$aprRangesP/extracted/tifs

mkdir -p $manP
mkdir -p $tifsP

cd $aprRangesP

#This the files relative to $aprRangesP/raw without cd
find "$aprRangesP/raw/ppm" -type f -exec realpath --relative-to="$aprRangesP/raw" {} \; | sort


# rngZips=(
#   PPM_BinaryMaps_101824.zip 
#   RangeBag_BinaryMaps_101824.zip 
#   Points_BinaryMaps_101824.zip
#   PPM_ConsensusMaps_2611_101824.zip
#   PPM_ConsensusMaps_2641_101824.zip
#   PPM_ConsensusMaps_2671_101824.zip
#   PPM_ConsensusMaps_7011_101824.zip
#   PPM_ConsensusMaps_7041_101824.zip
#   PPM_ConsensusMaps_7071_101824.zip
#   PPM_ConsensusMaps_8511_101824.zip
#   PPM_ConsensusMaps_8541_101824.zip
#   PPM_ConsensusMaps_8571_101824.zip
#   RangeBag_ConsensusMaps_2611_101824.zip
#   RangeBag_ConsensusMaps_2641_101824.zip
#   RangeBag_ConsensusMaps_2671_101824.zip
#   RangeBag_ConsensusMaps_7011_101824.zip
#   RangeBag_ConsensusMaps_7041_101824.zip
#   RangeBag_ConsensusMaps_7071_101824.zip
#   RangeBag_ConsensusMaps_8511_101824.zip
#   RangeBag_ConsensusMaps_8541_101824.zip
#   RangeBag_ConsensusMaps_8571_101824.zip)
#   
# rngZips=(
#   PPM_BinaryMaps_101824.zip)

rngZips=(
  ppm/BinaryMaps.zip)

rngZips=(
  ppm/2641.zip
  ppm/2671.zip
  ppm/7011.zip
  ppm/7041.zip
  ppm/7071.zip
  ppm/8511.zip
  ppm/8541.zip
  ppm/8571.zip
  ppm/BinaryMaps.zip
)

# Added 2611.zip later, so run this seperately
rngZips=(
  ppm/2611.zip
)

echo ${rngZips[0]}

log_file=create_manifest.log
zipsP=$aprRangesP/raw

rm -r $log_file

# Loop over each zip file. Takes ~20 sec to just do the manifest files.
time for rngZip in "${rngZips[@]}"; do
  
  # rngZip=${rngZips[0]} # Use for testing
  
  #The dirname is the model type
  modType=$(dirname $rngZip)

  # Determine the scenario based on the file name
  # Future scenarios have names like 2611, where 26 is the rcp, 11 means 2011
  if [[ "$rngZip" == *BinaryMaps* ]]; then
    scenario="present"
    rcp=""
    year=""
  else
    scenario=$(echo "${rngZip##*/}" | sed 's/\.zip$//')
    rcp=${scenario:0:2}
    year="20${scenario:2:2}"
  fi

  # Set directory and Parquet file based on scenario and modType
  rngDir="${scenario}/${modType}/"
  pqFile="${scenario}_${modType}.parquet"

  echo "Starting $rngZip with modType: $modType and scenario: $scenario" | tee -a $log_file
  
  # Process the zip file and create a manifest
  # head -10 |
  zipinfo -1 "$zipsP/$rngZip" | grep '.tif$'  |\
  awk -F'/' -v modType="$modType" -v rngDir="$rngDir" -v scenario="$scenario" \
    -v rcp="$rcp" -v year="$year" '{
    gsub("_", " ", $(NF-1));

    print $(NF-1) "\t" rcp "\t" year "\t" scenario "\t" modType "\t" rngDir $NF}' | \
  duckdb -c "COPY (
    SELECT * FROM read_csv_auto(
      '/dev/stdin',
      columns={'spp': 'VARCHAR', 'rcp': 'INTEGER', 'year': 'INTEGER', 
        'scenario': 'VARCHAR', 'mod_type': 'VARCHAR', 'path': 'VARCHAR'})
      order by spp
     ) TO '${manP}/${pqFile}' (FORMAT 'parquet');"

  # Display counts and sample data from the Parquet file
  echo Number of rows >> $log_file
  duckdb -csv -separator $'\t' -c "SELECT count(*) FROM '${manP}/${pqFile}'" >> "$log_file" 2>&1
  
  echo Sample data >> $log_file
  duckdb -csv -separator $'\t' -c "SELECT * FROM '${manP}/${pqFile}' limit 4" >> "$log_file" 2>&1
  
  # UNCOMMENT TO EXTRAT TIFS
  #---- Extract the tifs
  extOut=$tifsP/$scenario/$modType

  mkdir -p $extOut
  echo Unzipping the archive to $extOut
  time unzip -q -o -j $zipsP/$rngZip -d $extOut
done

#---- Check results

cat $log_file
# Count the number of files in the manifest folder
ls $manP
echo $(ls $manP | wc -l) #Should equal the line below
echo ${#rngZips[@]}

duckdb -c "SELECT * FROM '${manP}/present_ppm.parquet' limit 4"

ls $tifsP
ls $tifsP/2641

#How many tif files are in a folder
find $tifsP/2641/ppm -type f | wc -l #78,902
