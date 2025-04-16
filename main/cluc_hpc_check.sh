#---- Commands to check the running process and final results

squeue --me

ls $out

cat $out/cluc_hpc_slurm.log
tail $out/cluc_hpc_slurm.log

cat $out/mpilogs/MPI_1_*
tail $out/mpilogs/MPI_1_*

cat $out/script_status.csv
tail $out/task_status.csv
cat $out/spp_errors.csv

#---- Attribution area

# Sample of data
duckdb -csv -c "select * from read_parquet('$out/pq/*.parquet') limit 10" #78,851

# The number of species present
duckdb -csv -c "select count(distinct spp) from read_parquet('$out/pq/*.parquet')"


cat $out/spp_complete.csv | wc -l #78,768
cat $out/task_status.csv | wc -l
cat $out/spp_errors.csv | wc -l

ls -1 $out/attribution_ranges/tifs | wc -l #78,884

ls $out/attribution_ranges/tifs | head -10

# ---- check for bad rows ----

awk -F',' 'NF != 1 {print NR, $0}' $out/spp_complete.csv

awk -F',' 'NF != 3 {print NR, $0}' $out/spp_errors.csv

scancel 16537912

#Check if scratch space
df -h /scratch/mcu08001/bsc23001/tmp

#Delete huge scratch file
nohup rm -r /scratch/mcu08001/bsc23001/tmp &
ps -u $USER | grep rm

#---- Info about already completed jobs

sacct -u $USER -S 2025-04-15 --partition general \
  --format=JobID,JobName,Partition,State,ExitCode,Elapsed,Start

#---- Info about specific jobs
sacct -j 16537933
seff 16537933 

#TODO: look for duplicate names
#TODO: why do I have more species in attribution ranges/tifs than in spp_complete.csv?
#TODO: note very similar names
# Abarema_cochliacarpos.tif
# Abarema_cochliocarpos.tif



##----- Unorganized below here

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