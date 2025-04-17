# cluc - Climate Land Use Change

This project follows the [breezy](https://github.com/benscarlson/breezy) philosophy for building lightweight, loosely coupled, and repeatable personal scientific workflows.

A breezy project is seperated into multiple folders that hold results, data, and code. These elements are described further below.

# analysis

The analysis folder contains the results of analyses and the data required to run those analyses.
The folder contains multiple independent sessions. Usually there is a session for the main analysis in the `main` folder
and multiple sessions for proof of concept and development analyses in the `poc` folder.

## analysis/main

This session contains the results of the main analysis.

### analysis/main/data

Contains the results of the core analysis and any intermediate datasets.

- `scenario1` - attribution results for no dispersal.
- `scenario2` - attribution results for dispersal within ecoregion.
- `scenario3` - attribution results for dispersal outside ecoregion.

In all the scenario folders, the per-species and reason area results are parquet files inside `pq`.
The folder `attribution_ranges` contains tifs with a per-pixel attribution reason.

## analysis/poc

This folder contains sessions used for testing and developing. Currently there are two sessions.

* random_50. Use to locally test changes
* random_10k. Use to test large jobs

# data

The top-level `data` folder holds core data sets for the project. In general, each session should
contain all data required to run the session's scenario in its own `data` folder. However, 
sometimes it does not make sense to duplicate some datasets. In those cases, those excpetions are stored here.
Two common cases are

* Raw data originally downloaded and supplied from elsewhere.
* Large data sets shared among different sessions. For example, the lulc is used by all sessions.

TODO: the 10k bien_ranges are still in the old project directory.

Lulc data

- `lulc/raw` - Raw lulc data
- `lulc/habmask_moll_pct`. Converted and reprojected lulc layers

## data/lulc/habmask_moll_pct

-   `main/layers/lulc_reproject.r`. 

This script bins the lulc data into habitat/not habitat. 
It then re-project the lulc data and coarsens it from 1km to 5km to match bien ranges. 
Pixel values are percent of habitat.

# docs

Stores documents related to the project. In particular, this folder holds a manuscript associated with the project.

# src

The `src` folder contains the code housed in this repo.

The main workflow is at `workflows/main.sh`.

Installation and setup for specific components of this project are at `workflows/install.sh`

## src/main

This folder contains the code used to run the main analysis.

- `cluc_hpc.r` The main script that runs the attribution analysis. See below for more detailed information.
- `cluc_hpc_slurm.sh` The slurm script that launches the script on the hpc.
- `cluc_hpc_check.sh` Commmands for accessing the running process and results.

### src/main/bien_ranges

* `create_manifest.sh` - creates a manifest for the bien ranges data provided in zip files.

### src/main/layers

-   `main/layers/lulc_reproject.r`. Bin into habitat/not habitat. Reproject the lulc data and coarsen 1km to 5km to match bien ranges. Pixel values are percent of habitat.
-   `lulc_download.qmd`. This is the poc for downloading, it should be converted into a script.

## src/poc

### src/poc/random_50

Use to locally test changes in `cluc_hpc.r`.

### src/poc/random_10k

Use to test large jobs.

## src/resources

Reference data used in scripts.

## src/workflows

The starting point for all analyses. Contains shell commands to reproduce the workflow.
The scripts are intended to be run interactively, executing line-by-line, not in an automated fashion.
The idea is to have a complete set of commands to re-run the workflow, not a fully automated script.

* `main.sh` - The starting point of the analysis.
* `install.sh` - Commands to set up the environment before running the commands in main.
* `bien_ranges/apr_10_ranges.sh` - Download bien ranges from dropbox and extract individual zip folders (the download is a zip of zip folders).
* `bien_ranges/create_manifest.sh` - Extract individual zip folders and organize into a format that can be used by `cluc_hpc.r`

# Info about cluc_hpc.r

#### Setting the terra tmpdir directory

Use `basetmpdir` in the settings file

On the HPC, set the terra tmp dir to the scratch directory, for example 

/scratch/mcu08001/bsc23001/tmp/cluc

Each job gets a unique folder under this directory, to avoid collisions if multiple jobs are running.

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9

Each job has multiple iterations (each iteration of the for loop). Each iteration also needs its own folder

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9/iteration_1

