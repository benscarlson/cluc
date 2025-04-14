#!/usr/bin/env Rscript --vanilla

# This script implements the breezy philosophy: github.com/benscarlson/breezy

# ==== Breezy setup ====

'
Reproject LULC projections to mollewide to match bien ranges


Usage:
lulc_reproject.r <dat> <out>

Control files:

Parameters:
  dat: path to the input layers
  out: path to output directory.

Options:

' -> doc

#---- Input Parameters ----
if(interactive()) {
  
  .pd <- here::here()
  .wd <- file.path(.pd,'analysis/main')
  
  .datP <- file.path(.pd,'data/lulc/raw')
  .outP <- file.path(.pd,'data/lulc/habmask_moll_pct')
  
} else {

  ag <- docopt::docopt(doc)
  
  .script <-  whereami::thisfile()
  
  .pd <- rprojroot::is_rstudio_project$make_fix_file(.script)()
  .wd <- getwd()

  source(file.path(.pd,'src','funs','input_parse.r'))
  
  .datP <- makePath(ag$dat)
  .outP <- makePath(ag$out)
  
}

#---- Initialize Environment ----

pd <- function(...) file.path(.pd,...)
wd <- function(...) file.path(.wd,...)

t0 <- Sys.time()

source(pd('src/startup.r'))

suppressWarnings(
  suppressPackageStartupMessages({
    library(terra)
  }))

#Source all files in the auto load funs directory
list.files(pd('src/funs/auto'),full.names=TRUE) %>% walk(source)
# source(pd('src/funs/themes.r'))
# 
# theme_set(theme_eda)

#---- Local functions ----

#---- Local parameters ----
#.intpMethod <- 'near' #Nearest neighbor

#Just need an example range to use as the raster template
#For now, point to another directory that has 50 ranges randomly selected from the ppm archive
#TODO: this should point to a more permanent set of ranges extracted to the bien_ranges project
#.rangesP <- '~/projects/exposure/analysis/poc/scripts/sdm_quantiles/ranges_50/data/binary'

.rangePF <- '/Users/benc/projects/gpta/analysis/poc/cluc/oct18_ranges/random_50/data/random_50/tifs/2611/ppm/Asplenium_uniseriale__full__noBias_1e-06_0_1e-06_0_all_all_none_all_maxnet_none_equalWeights__2611__concensus_votes.tif'

#---- Files and folders ----

#---- Perform analysis ----

#Grab a range to use as a template for origin, grid, and projection
rng <- rast(.rangePF)

lcPFs <- list.files(.datP,full.names=TRUE)

dir.create(.outP,showWarnings=FALSE,recursive=TRUE)

# Approach
# * Convert landcover to binary habitat/not-habitat
# * Coarsen to ~5km, and calculate % habitat, but keep the projection in mercator
# * Then re-project to the range grid (mollewide), using bilinear interpolation

tic()
for(lcPF in lcPFs) {
  # lcPF <- lcPFs[1]
  message(glue('Projecting {basename(lcPF)}...'))
  
  lc <- rast(lcPF)
  
  #--- Convert to binary habitat/not-habitat
  tic()
  lc_mask <- ifel(lc %in% c(2,3),1,NA) #2=Forest, 3=Grassland. Everything else is not habitat.
  toc() #60 sec
  
  #--- Aggregate by a factor of 5 and get percent of pixels with 1
  tic()
  lc_agg <- aggregate(lc_mask,fact=5,fun='sum',na.rm=TRUE)/25
  toc() #8 sec
  
  #--- Now project the percentages to the range grid
  tic()
  lc_moll <- project(lc_agg,rng,method='bilinear',threads=TRUE,align_only=TRUE)
  toc() #1.6 sec
  
  #--- Check origin and resolution
  origin(lc_moll); origin(rng)
  res(lc_moll); res(rng)
  
  global(lc_moll,fun=c('min','max'),na.rm=TRUE)
  
  
  #==== OLD Approach ====
  
  # message(glue('Projecting {rstPF}...'))
  # 
  # rst <- rast(file.path(.datP,rstPF))
  # 
  # tic()
  # rst2 <- ifel(rst==0,NA,rst)
  # toc()
  # 
  # tic()
  # rst3 <- as.int(rst2 %in% c(2,3)) #2=Forest, 3=Grassland. Don't consider "no data" habitat
  # toc()
  # 
  # 
  # # #TODO: I should mask to hab/not hab first, then project based on majority
  # # # So, a pixel is switches only when the majority switches.
  # # # But, is this different from projecting first?
  # # tic() #align=TRUE uses rng for spatial resolution and origin, but does not clip to extent
  # # rstMoll <- project(rst,rng,method='mode',align_only=TRUE,threads=TRUE)
  # # toc() #7 sec
  # # 
  # # #Change origin and crs but don't change resolution
  # # tic()
  # # rstMoll <- project(rst,crs(rng),origin=origin(rng),method='near',threads=TRUE)
  # # toc()
  # 
  # # Compute new resolutions that will exactly fit within rng's cells:
  # new_res_x <- res(rng)[1] / 5  # 4950 / 5 = 990 m
  # new_res_y <- res(rng)[2] / 6  # 6140 / 6 ≈ 1023.33 m
  # new_res <- c(new_res_x, new_res_y)
  # 
  # # #Try with setting origin and res in project
  # # tic()
  # # rstMoll <- project(rst,crs(rng),origin=origin(rng),res=new_res,method='near',threads=TRUE)
  # # toc()
  # 
  # #Try with a template. These are probably pretty similar.
  # template <- rast(ext = rng, crs = crs(rng), resolution = new_res)
  # origin(template) <- origin(rng) #Note that template origin does not match rng origin
  # 
  # tic()
  # rstMoll <- project(rst3,template,method='near',threads=TRUE,align_only=TRUE)
  # toc()
  
  #==== Write the raster ====
  
  tic()
  writeRaster(lc_moll,file.path(.outP,basename(lcPF)),overwrite=TRUE)
  toc()
  
}
toc()


#Note Origin in gdalinfo seems to mean upper left coner, while in terra is it origin of coordinate system
#---- Finalize the script ----

message(glue('Script complete in {diffmin(t0)} minutes'))