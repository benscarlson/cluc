#---- Commands to check the running process and final results

squeue --me

ls $out

cat $out/cluc_hpc_slurm.log
tail $out/cluc_hpc_slurm.log

cat $out/mpilogs/MPI_1_*
tail $out/mpilogs/MPI_1_*

mlr --icsv --opprint cat $out/script_status.csv
mlr --icsv --opprint cat $out/task_status.csv
# This doesn't work, mlr fails if missing
mlr --icsv --opprint cat $out/spp_errors.csv

duckdb -csv -c "select * from read_parquet('$out/pq/*.parquet') limit 10"


mlr --icsv cat $out/spp_complete.csv | wc -l
mlr --icsv cat $out/task_status.csv | wc -l
mlr --icsv cat $out/spp_errors.csv | wc -l

ls -1 $out/attribution_ranges/tifs | wc -l

scancel 16537912

#Check if scratch space
df -h /scratch/mcu08001/bsc23001/tmp
rm -r /scratch/mcu08001/bsc23001/tmp
