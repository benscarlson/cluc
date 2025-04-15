#!/usr/bin/env -S Rscript --vanilla

# ==== Input parameters ====

#TODO: handle resuming to rerun only species with errors
#TODO: or resume to also ignore species with errors
# Maybe pass in a value to "resume" instead of true/false?
#TODO: save a file of input parameters
#TODO: make a file that installs all required packages

'
Usage:
cluc_hpc.r <ranges> <out> [--chunksize=<chunksize>] [--cores=<cores>] [--dispersal] [--fulldomain] [--maxcell=<maxcell>] [--mpilogs=<mpilogs>] [--numrows=<numrows>] [--parMethod=<parMethod>] [--resume] [--verbose]
cluc_hpc.r (-h | --help)

Control files:
  ctfs/species.csv

Parameters:
  ranges: species range directory.
  out: output directory that will contain range size estimates.

Options:
-h --help     Show this screen.
-v --version     Show version.
-k --chunksize=<chunksize>  The number of species to run per core (doMC) or task (doMPI). Defaults to 1.
-c --cores=<cores>  The number of cores
-d --dispersal  Whether dispersal is allowed
-f --fulldomain  Use full domain or ecoregion constrained ranges
-l --maxCell=<maxcell>  Do not process a range with more than <maxcell> cells. Useful for low-memory cores. Defaults to Inf.
-m --mpilogs=<mpilogs> Directory for the mpi log files
-n --numrows=<numrows>  Process the first n rows. Useful for testing.
-p --parMethod=<parMethod>  Either <mpi | mc>. If not passed in, script will run sequentially.
-s --resume   Run species in the species control file that are not in spp_complete.csv.
-b --verbose  Print verbose messages

Output

<out>/pq -- (.outPQ) Main results, as one parquet file per task.
<out>/spp_complete.csv -- (.sppCompPF) Successfully completed species.
<out>/spp_errors.csv -- (.errPF) Species that failed along with error messages.
<out>/task_status.csv -- (.taskPF) Task-level information.
<out>/script_status.csv -- (.statusPF) Script-level information.

The script always overwrites spp_errors.csv, task_status.csv, and script_status.csv
The script also removes pq/* and spp_complete.csv, unless resuming. If resuming, it appends.


' -> doc


if(interactive()) {

  .pd <- here::here()
  .wd <- file.path(.pd,'analysis/poc/errors/errors1')
  
  # Required
  #.rangesP <- file.path(.wd,'data/ranges')
  .rangesP <- file.path(.wd,'data/ranges')
  #.envsP <- file.path(.pd,'data/lulc/habmask_moll')
  .outP <- file.path(.wd,'data/scenario1')
  
  # Optional
  .chunkSize <- 10
  .cores <- NULL
  .dispersal <- FALSE
  .fullDomain <- FALSE
  .maxCell <- Inf
  .mpiLogP <- NULL
  .numRows <- Inf
  #.numRows <- 10
  .parMethod <- NULL
  .resume <- FALSE
  .verbose <- TRUE
  
} else {
  
  ag <- docopt::docopt(doc, version = '0.1\n')
  
  .script <-  whereami::thisfile()
  
  .pd <- rprojroot::is_rstudio_project$make_fix_file(.script)()
  .wd <- getwd()
  
  source(file.path(.pd,'src','funs','input_parse.r'))
  
  #.envsP <- makePath(ag$envs)
  .outP <- makePath(ag$out)
  .rangesP <- makePath(ag$ranges)
  
  .chunkSize <- parseParam(ag$chunksize,1)
  .cores <- ag$cores
  .dispersal <- ag$dispersal
  .fullDomain <- ag$fulldomain
  .maxCell <- parseParam(ag$maxcell,Inf)
  .mpiLogP <- makePath(ag$mpilogs,'mpilogs')
  .numRows <- parseParam(ag$numrows,Inf) #Used to read the first n rows in read_csv
  .parMethod <- ag$parMethod
  .resume <- ag$resume
  .verbose <- ag$verbose
}

# ==== Setup ====

#---- Initialize Environment ----#

t0 <- Sys.time()

source(file.path(.pd,'src/startup.r'))

suppressWarnings(
  suppressPackageStartupMessages({
    library(arrow)
    library(iterators)
    library(foreach)
    library(logger)
    library(terra)
    library(yaml)
  }))

suppressMessages(conflicts_prefer(terra::trim))

list.files(file.path(.pd,'src/funs/auto'),full.names=TRUE) %>% walk(source)

pd <- pathFactory(.pd)
wd <- pathFactory(.wd)

source(pd('src/funs/bien_ranges.r')) #Helper functions for bien ranges
source(pd('src/funs/terra.r')) #Helper functions for terra

if(.verbose) log_threshold(DEBUG)

#---- Load and apply settings if available

# First look in the output directory, then in working directory for a settings file
settingsPF <- detect(file.path(c(.outP,.wd),'cluc_hpc_settings.yml'),file.exists)

if(!is.null(settingsPF)) {
  settings <- read_yaml(settingsPF)
  
  if(hasName(settings,'terraOptions')) {
    rlang::exec(terraOptions,!!!settings$terraOptions)
  }
}

log_debug(paste(c("\n",capture.output(terraOptions())),collapse='\n'))

#---- Local functions ----

#---- Local parameters ----
.cellArea_km2 <- 30.22578
.bufDist_km_yr <- 1.5 #Assume dispersal rate of 1.5 km/yr. 
.dispYearStart <- 2011 #Year of "present day" to start dispersal masks 

rangesTfs <- file.path(.rangesP,'tifs')

.envsP <- pd('data/lulc/habmask_moll_pct')
envsTfs <- file.path(.envsP,'tifs')

ecoP <- pd('src/resources/ecoregions/GlobalEcoregions.tif') #Used as a land/sea mask

#-- Output paths (see help)
.outPQ <- file.path(.outP,'pq')
.sppCompPF <- file.path(.outP,'spp_complete.csv')
.errPF <- file.path(.outP,'spp_errors.csv')
.taskPF <- file.path(.outP,'task_status.csv')
.statusPF <- file.path(.outP,'script_status.csv')
.memUsePF <- file.path(.outP,'mem_use.csv')

#' gc() both runs garbage collection and reports the memory used by R
#'  the memory reported is after garbage collection
#' gc() misses memory used by c++ so possibly terra objects, so ps returns everything
#' TODO: could make this a function generator that takes fname, then I could move it to another file.
gcLogMem <- function(spp, fname=.memUsePF) {
  
  gcmem <- round(as.numeric(pryr::mem_used())/2^30, 9) #mem in Gib. Note pryr uses GB by default (1000^x)
  
  pid <- Sys.getpid()
  psmem <- round(as.numeric(system(sprintf("ps -o rss= -p %d", pid), intern = TRUE))/2^20,9)
  
  logDat <- tibble(
    pid=pid,
    datetime=Sys.time(),
    species=spp,
    gc_mem_gib=gcmem,
    ps_mem_gib=psmem)
  
  write_csv(logDat,fname,append=file.exists(fname))
}
  
#---- Load control files ----#

species <- read_csv(wd('ctfs/species.csv'),n_max=.numRows) %>% filter(run==1) %>% select(-run)

if(!is.infinite(.numRows)) {message(glue('Selected the first {nrow(species)} species'))}

# If resuming, just run for species that are not in the species complete file.
# If not resuming, can remove the output directory and the species completion file
if(.resume) {
  message('Resuming, filtering species list by the species completion file.')
  species <- species %>% anti_join(read_csv(.sppCompPF),by='spp')
} else {
  if(dir.exists(.outPQ)) unlink(.outPQ,recursive=TRUE)
  if(dir.exists(.sppCompPF)) invisible(file.remove(.sppCompPF))
}
#---- Load data ----#
message('Loading data...')

#NOTE: load manf for the range tifs inside the foreach loop

envsManf <- read_csv(file.path(.envsP,'manifest.csv')) %>% mutate(layer_name=gsub('.tif','',path))

layerMap <- read_csv(pd('src/resources/layer_map.csv'))

#---- Files and directories ----#
dir.create(.outPQ,showWarnings=FALSE,recursive=TRUE)

#Remove error and task output files (even if resuming) since those are tied to the specific script run
c(.errPF,.taskPF,.statusPF,.memUsePF) %>% 
walk(~{
  if(file.exists(.x)) invisible(file.remove(.x))
})

#Figure out row groups for task chunks.
rowGroups <- tibble(start=seq(1,nrow(species),by=.chunkSize)) %>%
  mutate(end=pmin(start + .chunkSize - 1, nrow(species)))

log_debug('Setup complete')

# ==== Start cluster and register backend ====
if(is.null(.parMethod)) {
  log_info('No parallel method defined, running sequentially.')
  #foreach package as %do% so it is loaded even if the parallel packages are not
  `%mypar%` <- `%do%`
} else if(.parMethod=='mpi') {
  log_info('Registering backend doMPI')
  library(doMPI)
  
  dir.create(.mpiLogP,showWarnings=FALSE,recursive=TRUE)
  #start the cluster. number of tasks, etc. are defined by slurm in the init script.
  log_info('Starting mpi cluster.')
  cl <- startMPIcluster(verbose=TRUE,logdir=.mpiLogP)
  registerDoMPI(cl)
  setRngDoMPI(cl) #set each worker to receive a different stream of random numbers
  
  `%mypar%` <- `%dopar%`
  
} else if(.parMethod=='mc') {
  #.cores <- strtoi(Sys.getenv('SLURM_CPUS_PER_TASK', unset=1)) #for testing on hpc
  log_info('Registering backend doMC with {.cores} cores')
  library(doMC)
  RNGkind("L'Ecuyer-CMRG")
  
  registerDoMC(.cores)
  
  `%mypar%` <- `%dopar%`
  
} else {
  stop('Invalid parallel method')
}

# ==== Perform analysis ====

log_info('Running for {nrow(species)} species, {nrow(rowGroups)} tasks, {.chunkSize} species per task')

tic()
foreach(i=icount(nrow(rowGroups))) %mypar% {

    # i <- 1
    tsTask <- Sys.time()
    
    log_info('Starting chunk {i} of {nrow(rowGroups)}')
    
    # Load ecoregion and envs rasters, as well as initialize range manifest here 
    # becuase they can't be exported
    eco <- rast(ecoP)
    
    manf <- open_dataset(file.path(.rangesP,'manifest'))
    
    envsR <- rast(file.path(envsTfs,envsManf$path))
    names(envsR) <- envsManf$layer_name #TODO: Should set the layer names upstream
    
    envsR <- envsR[[layerMap$env_names]] #Pick just the layers that are mapped to range scenarios
    
    start <- rowGroups$start[i]
    end <- rowGroups$end[i]
    
    speciesChunk <- species %>% slice(start:end)
    
    log_debug('Loaded all data to start task')
    #---- Run the analysis over each species in the chunk ----
    # NOTE: to add more information to the error (like elapsed time) can return
    # a tibble by wrapping the code in a tryCatch block
    
    results <- speciesChunk %>% 
      mutate(dat=map(spp,safely(~{
        # .x <- 'Aster lingulatus'
        # .x <- 'Calceolaria percaespitosa'
        # .x <- 'Abarema adenophorum' # - extent issue
        
        #---- Load and format the present range
        log_debug('*** {.x} ***')
        
        presRngPF <- manf %>% 
          filter(spp==.x & scenario=='present') %>% collect %>% pull('path') %>%
          file.path(rangesTfs,.)
        
        pres <- binPresRange(rast(presRngPF))
        rm(presRngPF)
        
        #---- Load and format the future range
        
        #Load the raw ranges.
        futRngs <- manf %>%
          filter(spp==.x & scenario != 'present') %>%
          collect %>%
          mutate(rast=map(path,~rast(file.path(rangesTfs,.x)))) %>%
          select(-c(spp,path)) %>%
          arrange(rcp,year)
        
        #Skip this species if the present range is empty (means it an error)
        # Don't check if future ranges are empty, since no pixels would mean extinct, which is a valid state
        if(all(is.nan(values(pres)))) {
          stop(glue('Skipping. The present-day raster has no values'))
        }
        
        #MPI will die if too many cells for requested memory
        #TODO: I don't really use this anymore, but it might still be useful, get rid of it or revamp it
        #TODO: need to check for this case when creating the rasters, also trim
        #TODO: I should probably check all ranges for > maxcell
        if(ncell(pres) > .maxCell) {
          stop(glue('Skipping. Too many cells ({ncell(rng)} > {.maxCell})'))
        }
        
        #----
        #---- Full or ecoregion domain
        #----
        
        futRngs <- futRngs %>%
          rowwise %>%
          mutate(rast=list({
            # rast <- futRngs$rast[[1]]
            rng <- binFutRange(rast,full_domain=.fullDomain)
            names(rng) <- glue('rcp{rcp}_{year}') #layer name
            #names(rng) <- scenario
            
            rng # Note, can't do return here, this is an anonymous block not a function
          })) %>%
          ungroup # remove rowwise mode
  
        #----
        #---- Generate dispersal masks
        #----
        
        if(.dispersal) {
          # Make the future dispersal buffer for each year
          dispersalMasks <- futRngs %>%
            distinct(year) %>% # Dispersal starts from present range so we only need years
            mutate(buf_year=year+15, #This is the midpoint of the 30 year climate time step
             dispersal_mask=map(buf_year,~{
               #browser()
               # dist per year by number of years since start. convert to meters
               bufDist_m <- .bufDist_km_yr*(.x-.dispYearStart)*1e3
               
               # mask this by the occupied ecoregion, since species can't disperse outside 
               #   the occupied ecoregions in this scenario
               #TODO: Expand pres raster to allow the buffer to extend beyond the current domain
               rngBuf <- terra::buffer(pres,width=bufDist_m) %>% 
                 ifel(1,NA) %>% #convert to mask
                 mask(crop(eco,pres)) # Mask by ecoregion file only to clip out marine pixels
               
               names(rngBuf) <- .x
               return(rngBuf)
             }))
          
        } else {
          #If no dispersal, mask by the present day range
          dispersalMasks <- futRngs %>%
            distinct(year) %>%
            mutate(dispersal_mask=rep(list(pres),n())) 
        }
        
        #==== Set up the time-series ====
        
        # Convert ranges from list-column rasters to a single SpatRast with multiple layers
        futRngsRst <- rast(futRngs$rast)
        names(futRngsRst) <- futRngs$scenario
        rngsRst <- c(pres,futRngsRst) #Include the future range
        
        log_debug(glue('ncell for range stack: {ncell(rngsRst)}'))
        
        rm(futRngs,pres)
        
        # Do the same for the dispersal masks
        dMasksRst <- rast(dispersalMasks$dispersal_mask)
        names(dMasksRst) <- dispersalMasks$year
        rm(dispersalMasks)
        
        #---- Trim based on the full time series, make binary rasters ----
        
        # We can safely crop based on the the dispersal masks, since we'll never have presence beyond those. 
        cropExt <- ext(trim(dMasksRst))
        
        #-- Climate: Crop and turn the ranges into a binary rasters
        clim <- rngsRst %>% crop(cropExt) %>% subst(NaN,0)
        
        #-- Land: Crop the land cover but keep it as a percentage
        landpct <- envsR %>% crop(cropExt) %>% subst(NaN,0)
        names(landpct) <- names(clim)  #set names so they match clim and disp
        # Make a binary land suitability raster. Any amount of habitat is considered presence.
        land <- as.int(landpct > 0)
        
        #-- Dispersal: 
        # Convert to 0/1 rasters
        # masks have one per time step but not scenario, so there are only three
        # duplicate so there are a total of three per year, for each scenario
        # also, add the present-day raster (which is the starting point for dispersal)
        # in order to make the number of layers equal the clim and land stacks (total of ten)
        disp <- dMasksRst %>% crop(cropExt) %>% subst(NaN,0) %>% rep(3)
        disp <- c(clim[['present']],disp) #Add present day range as the starting point
        names(disp) <- names(clim)
        
        # Species only occurs when climate is suitable, w/in dispersal mask, and lc pct > 0
        occ <- clim * disp * land #Layer names match clim
        
        rm(cropExt,land)
        
        # We want t2-t1
        # For 2.6, should be 2611-present, 2641-2611, 2671-2641
        # For 7.0, should be 7011-present, 7041-7011, 7071-7041
        # For 8.5 should be 8511-present, 8541-8511, 8571-8541
        #
        # t1 = present, 2611, 2641, present, 7011, 7041,...
        # t2 = 2611, 2641, 2671, 7011, 7041, 7071,...
        
        # Set up timesteps according to above. Need to replace last timestep with "present"
        # for p2, only need to remove the 'present' layer
        t1Layers <- c('present','2611','2641','present','7011','7041','present','8511','8541')
        
        c1 <- clim[[t1Layers]]; c2 <- clim[[-1]]
        d1 <- disp[[t1Layers]]; d2 <- disp[[-1]]
        lpct1 <- landpct[[t1Layers]]; lpct2 <- landpct[[-1]]
        occ1 <- occ[[t1Layers]]; occ2 <- occ[[-1]]
        
        # Calculate the change in percentage of habitat
        # pos: gain, neg: loss
        lpct_change <- lpct2 - lpct1; # names are from lpct2, don't need to set them.
        
        #=================================
        #==== Loss driver attribution ====
        #=================================
        log_debug('Loss driver attribution')
        
        # Note in loss driver layers, 1 means loss, 0 means not loss
        closs <- (c2 == 0 & c1 == 1) %>% as.int # Climate goes from suitable -> unsuitable
        lloss <- (lpct_change < 0) %>% as.int # Habitat % decreases
        occloss <- (occ2 == 0 & occ1 == 1) %>% as.int # Occ goes from suitable -> unsuitable
        
        # Combine loss drivers. 1 = climate, 2 = land, 3 = both
        # In order for a loss to occur, the pixel must have been suitable at t1
        # so multiply by occ1
        # NOTE: I think it is right to multiply by occ1, think about it again later.
        loss_cases <- ((lloss * 2 + closs * 1) * occloss)
        rm(closs, lloss, occloss)
        
        #---- Sub-pixel attribution
        # Based on the table of cases
        # * clim is % at t1
        # * land is ∆%
        # * both is ∆%, but if ∆% > t1 %, then the remainder is clim
        
        # Approach
        # Make a raster with
        # - %t1 or ∆% as appropriate
        # use zonal stats to sum the area of each case
        # posthoc, adjust the area
        # - calculate the clim only total from both
        # - subtract this from both and add it to clim
        
        # Climate only loss is loss of % habitat at t1. Multiply by -1 since it is a loss
        # Loss due to land and loss due to both are the same, the change in % habitat
        # Note this approach does not account for climate only loss in a "both" pixel. Need to add that later
        clim_loss_pct <- as.int(loss_cases==1) * lpct1 * -1 # Loss is % at t1
        land_loss_pct <- as.int(loss_cases==2) * lpct_change # Loss is change in %
        both_loss_pct <- as.int(loss_cases==3) * lpct_change # Make a separate layer for clarity

        # Sum the percent loss and convert to area, to make a single layer
        loss_km2 <- (clim_loss_pct + land_loss_pct + both_loss_pct) %>% subst(0,NaN) * .cellArea_km2
        rm(clim_loss_pct, land_loss_pct, both_loss_pct)
        
        #Sum the area for each zone over all layers
        loss_km2_total <- zonalStack(loss_km2,subst(loss_cases,0,NaN),fun='sum') %>%
          rename(reason=zone,area_km2=sum)
        rm(loss_km2); # removed loss_km2; now summarized
        
        #==== Split loss attribution ====
        
        #Climate only loss in a "both" pixel is the remainder of the loss
        #"both" is equal ∆%, so the amount remaining at t2 is attributable to only climate
        # multiply by -1 since this represents a loss.
        # multiply by cell area to get the total area
        # Don't need to use the more general zonal approach used in gain since only one
        #  category needs to be adjusted
        # TODO: could use the zonalStack function now to be consistent with other code.
        both_clim_loss_km2 <- (as.int(loss_cases==3) * lpct2 * -1 * .cellArea_km2) %>%
          global(sum,na.rm=TRUE) %>%
          as_tibble(rownames='layer_name') %>%
          mutate(reason=1) %>% #Need to attribute this to case 1, so set reason to 1
          select(layer_name,reason,both_clim_loss_km2=sum)
        
        # Now add the climate only loss to the total loss
        loss_km2_total <- loss_km2_total %>%
          left_join(both_clim_loss_km2,by=c('layer_name','reason')) %>%
          mutate(area_km2=area_km2 + coalesce(both_clim_loss_km2,0),.keep='unused')
         rm(both_clim_loss_km2);
        # loss_km2_total %>% summarize(area_km2=sum(area_km2),.by='layer_name')
        
        #=================================
        #==== Gain driver attribution ====
        #=================================
        log_debug('Gain driver attribution')
        
        # Note in loss driver layers, 1 means loss, 0 means not loss
        cgain <- (c2 == 1 & c1 == 0) %>% as.int # Climate goes from unsuitable -> suitable
        lgain <- (lpct_change > 0) %>% as.int # Habitat % increases
        dgain <- (d2 == 1 & d1 == 0) %>% as.int # Dispersal goes from unsuitable -> suitable
        occgain <- (occ2 == 1 & occ1 == 0) %>% as.int # Occ goes from unsuitable -> suitable
        
        # Combine the three "change" rasters into a single integer code
        #    using powers of two
        # Gain can only occur where a pixel was unsuitable at t1, but is now suitable at t1
        #  so multiply by ccgain
        gain_cases <- ((dgain*4 + lgain*2 + cgain*1) * occgain) #%>% subst(0,NaN)
        rm(cgain, lgain, dgain, occgain);           # removed intermediate gain indicators
        
        # 1 - Climate  
        # 2 - Land  
        # 3 - Climate & Land  
        # 4 - Dispersal  
        # 5 - Climate & Dispersal  
        # 6 - Land & Dispersal  
        # 7 - Climate & Land & Dispersal
        
        # Set up a raster with the appropriate habitat percent. It can be either t1 %, t2 %, or ∆%
        # Collapsing cases from the attribution_cases spreadsheet
        # all three - ∆%
        # clim & land - ∆%
        # clim & dispersal - t2 %
        # land & dispersal - ∆%
        # clim - t1 %
        # land - ∆%
        # dispersal - t1 %

        # Note the code below is extremely similar to the loss attribution code. Maybe combine.
        delta_pct <- as.int(gain_cases %in% c(2,3,6,7)) * lpct_change
        t1_pct <- as.int(gain_cases %in% c(1,4)) * lpct1
        t2_pct <- as.int(gain_cases %in% c(5)) * lpct2
        
        rm(lpct_change);                          # removed lpct_change; no longer needed
        
        # Sum the percent loss and convert to area, to make a single layer
        gain_km2 <- (delta_pct + t1_pct + t2_pct) %>% subst(0,NaN) * .cellArea_km2
        rm(delta_pct, t1_pct, t2_pct); # removed gain percentage intermediates
        
        # Sum the total area by area using zonal statistics
        gain_km2_total <- zonalStack(gain_km2,subst(gain_cases,0,NaN),fun='sum') %>%
          rename(reason=zone,area_km2=sum)
        rm(gain_km2); 
        
        #==== Split gain attribution ====
        
        #Add additional gain due to sub-pixel attribution
        #t2 - climate & land & dispersal -> climate & dispersal
        #t2 - climate & land -> climate
        #t1 - land & dispersal -> dispersal
        #TODO: look at t1_split. All cases are 0. Is this right?
        
        t2_split <- as.int(gain_cases %in% c(3,7)) * lpct2
        t1_split <- as.int(gain_cases %in% c(6)) * lpct1
        
        split_km2 <- (t2_split + t1_split) %>% subst(0,NaN) * .cellArea_km2
        rm(t2_split, t1_split);  # removed sub-pixel percentage intermediates
        
        #Apply zonal stats to each split reason, then remap to the new reason
        split_km2_total <- zonalStack(split_km2,subst(gain_cases,0,NaN),fun='sum') %>%
          rename(reason=zone,area_km2=sum) %>%
          filter(!is.na(area_km2)) %>% 
          mutate(reason=case_match(reason,
            7 ~ 5, #climate & land & dispersal -> climate & dispersal
            3 ~ 1, #climate & land -> climate
            6 ~ 4  #land & dispersal -> dispersal
          ))
        
        # Now add the sub-pixel attribution to the correct reason
        #TODO: the first time period only has values for 2, 5, 7. Does this make sense?
        gain_km2_total <- gain_km2_total %>%
          left_join(
            split_km2_total %>% rename(split_area_km2=area_km2),
            by=c('layer_name','reason')) %>%
          mutate(area_km2=area_km2 + coalesce(split_area_km2,0),.keep='unused')
        
        rm(split_km2_total); # removed split_km2_total
        
        # Remove subsetting variables now that attribution is complete
        rm(c1, c2, d1, d2, lpct1, lpct2, occ1, occ2, t1Layers); # removed all subsetting variables
        
        #===============================
        #==== Combine gain and loss ====
        #===============================
        log_debug('Finalizing results')
        
        #---- Make a raster that has values 1-10 for the 10 possible cases ----
        
        # lg cases has 1-3 for loss cases, and 4-10 as gain cases
        lg_cases <- loss_cases + (subst(gain_cases + 3,3,0))
        
        lg_cases <- subst(lg_cases,0,NA) %>% trim #Shouldn't need to trim, but just in case
        
        rm(loss_cases, gain_cases); # removed intermediate case variables
        
        tifOutPF <- file.path(.outP,'attribution_ranges','tifs',paste0(gsub(' ','_',.x),'.tif'))
        dir.create(dirname(tifOutPF),showWarnings=FALSE,recursive=TRUE) #TODO: move this out of the loop
        #TODO: need to return this path and somehow save it to a manifest file
        # Maybe I can do that after I know all the species that were successfully processed
        lg_cases %>% 
          writeRaster(tifOutPF, datatype = "INT1U",
            gdal = c("COMPRESS=DEFLATE","PREDICTOR=2","TILED=YES"),
            overwrite=TRUE)
        
        rm(lg_cases); 
        #---- Make a single dataframe with loss and gain area ----
        #Add 3 so that loss and gain can be stored in the same column
        # 1-3 = loss, 4-10 = gain
        res <- gain_km2_total %>%
          mutate(reason=reason+3) %>% 
          bind_rows(loss_km2_total) %>%
          mutate(dispersal=.dispersal,full_domain=.fullDomain,.before=2) %>%
          arrange(layer_name,reason)
        
        log_debug('Species complete')
        
        gcLogMem(spp=.x) #Run garbage collection and log the memory use
        
        return(res)
        
      })),
        val = map(dat, "result"), #unpack result and error slots from safely
        err = map(dat, "error")
      )
    
    #--- Extract and log errors
    errDat <- results %>% 
      filter(lengths(val) == 0)  %>%
      mutate(reason=map_chr(err,'message'),
             task_num=i) %>%
      select(task_num,spp,reason) 
    
    if(nrow(errDat) > 0) {
       errDat %>%
        write_csv(.errPF,append=file.exists(.errPF),na="") #if empty, should write nothing
    }
    
    #--- Extract and write the results
    resDat <- results %>% 
      filter(lengths(val) != 0) %>% # Only keep results that didn't error
      select(spp,val)
    
     rm(results); 
    
    if(nrow(resDat) > 0) {
      #Log the list of successfully completed species
      resDat %>% select(spp) %>% write_csv(.sppCompPF,append=file.exists(.sppCompPF))
      
      #Write the actual data to parquet files
      resDat %>%
        unnest(cols=val) %>%
        write_parquet(file.path(.outPQ,glue('task_{i}_{format(Sys.time(),format="%Y-%m-%dT%H-%M")}.parquet')))
      
      #Write the manifest for the attribution ranges
      tifManPQ <- file.path(.outP,'attribution_ranges','pq') #TODO: move this out of the loop
      dir.create(tifManPQ,showWarnings=FALSE,recursive=TRUE) #TODO: move this out of the loop
      
      #TODO: write other metadata like mod_type, etc.
      resDat %>% 
        select(spp) %>%
        mutate(path=paste0(gsub(' ','_',spp),'.tif')) %>%
        write_parquet(file.path(tifManPQ,glue('task_{i}_{format(Sys.time(),format="%Y-%m-%dT%H-%M")}.parquet')))
    }
    
    #Log task-level information
    tibble(
      task_num=i,
      n_success=nrow(resDat),
      n_fail=nrow(errDat),
      success=n_fail==0,
      start_row=start,
      end_row=end,
      minutes=as.numeric(diffmin(tsTask))) %>%
      write_csv(.taskPF,append=file.exists(.taskPF))
    
    message(glue('({hre(tsTask)} elapsed) Chunk {i} of {nrow(rowGroups)}, rows {start} through {end} complete.'))
    message(glue('There were {nrow(resDat)} successful species and {nrow(errDat)} failed.'))
    
    rm(resDat,errDat,envsR, eco); 
    
    gcLogMem(spp=NA)
    tmpFiles(remove=TRUE) #Clean up terra temporary files
    
    return(TRUE) #Return true to keep return vector small

} -> status
toc()

#---- Finalize script ----

log_info('Script complete in {hre(t0)}')

#Write the script-level information
tibble(
  n_spp_req=nrow(species),
  n_tasks=nrow(rowGroups),
  minutes=as.numeric(diffmin(t0))) %>%
  write_csv(.statusPF)

#seems nothing after mpi.quit() is executed, so make sure this is the last code
if(!is.null(.parMethod) && .parMethod=='mpi') {
  closeCluster(cl)
  mpi.finalize()
}
