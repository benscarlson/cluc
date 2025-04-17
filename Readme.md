# cluc - Climate Land Use Change

This project follows the [breezy](https://github.com/benscarlson/breezy) philosophy for building lightweight, loosely coupled, and repeatable personal scientific workflows.

# analysis

## main

Contains the results of the core analysis

### data

- `scenario1` - attribution results for no dispersal.
- `scenario2` - attribution results for dispersal within ecoregion.
- `scenario3` - attribution results for dispersal outside ecoregion.

In all the scenario folders, the per-species and reason area results are parquet files inside `pq`.
The folder `attribution_ranges` contains tifs with a per-pixel attribution reason.

## poc

Contains sessions used for testing and developing


# data

Core data for the project. Raw data originally downloaded from elsewhere. 
Also, data shared among different sessions. For example, the lulc is used by all sessions.

TODO: the bien_ranges are still in the old project directory.

Lulc data

- `lulc/raw` - Raw lulc data
- `lulc/habmask_moll_pct`. Converted and reprojected lulc layers

## lulc/habmask_moll_pct

-   `main/layers/lulc_reproject.r`. 

Bin into habitat/not habitat. 
Reproject the lulc data and coarsen 1km to 5km to match bien ranges. 
Pixel values are percent of habitat.

# docs

# src

The `src` folder contains the code housed in this repo.

The main workflow is at `workflows/wf-main.sh`.

Installation and setup for specific components of this project are at `workflows/wf-install.sh`

## main

This folder contains the code used to run the main analysis.

- `cluc_hpc.r` The main script that runs the attribution analysis. See below for more detailed information.
- `cluc_hpc_slurm.sh` The slurm script that launches the script on the hpc.
- `cluc_hpc_check.sh` Commmands for accessing the running process and results.

### bien_ranges

* `create_manifest.sh` - creates a manifest for the bien ranges data provided in zip files.

### layers

-   `main/layers/lulc_reproject.r`. Bin into habitat/not habitat. Reproject the lulc data and coarsen 1km to 5km to match bien ranges. Pixel values are percent of habitat.
-   `lulc_download.qmd`. This is the poc for downloading, it should be converted into a script.

## poc

### random_50

Use to locally test changes in `cluc_hpc.r`.

### random_10k

Use to test large jobs

## resources

Reference data used in scripts. 

# Info about cluc_hpc.r

#### Setting the terra tmpdir directory

Use `basetmpdir` in the settings file

On the HPC, set the terra tmp dir to the scratch directory, for example 

/scratch/mcu08001/bsc23001/tmp/cluc

Each job gets a unique folder under this directory, to avoid collisions if multiple jobs are running.

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9

Each job has multiple iterations (each iteration of the for loop). Each iteration also needs its own folder

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9/iteration_1

