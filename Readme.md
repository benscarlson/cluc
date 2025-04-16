# cluc - Climate Land Use Change

This project follows the [breezy](https://github.com/benscarlson/breezy) philosophy for building lightweight, loosely coupled, and repeatable personal scientific workflows.

# analysis



# data

## lulc/raw

-   Point to the script I used to download the data

## lulc/habmask_moll_pct

-   `main/layers/lulc_reproject.r`

# docs


# src


The main workflow is at `workflows/wf-main.sh`.

Installation and setup for specific components of this project are at `workflows/wf-install.sh`

## main

This folder contains the code used to run the main analysis.

### bien_ranges

* `create_manifest.sh` - creates a manifest for the bien ranges data provided in zip files.

### layers

## poc

### random_50

Use to locally test changes in `cluc_hpc.r`.

### random_10k

Use to test large jobs

## Info about cluc_hpc.r

#### Setting the terra tmpdir directory

Use `basetmpdir` in the settings file

On the HPC, set the terra tmp dir to the scratch directory, for example 

/scratch/mcu08001/bsc23001/tmp/cluc

Each job gets a unique folder under this directory, to avoid collisions if multiple jobs are running.

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9

Each job has multiple iterations (each iteration of the for loop). Each iteration also needs its own folder

/scratch/mcu08001/bsc23001/tmp/cluc/cluc_98a569a1f3d9/iteration_1

