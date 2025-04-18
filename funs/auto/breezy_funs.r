#----
#---- These functions are used by breezy_script.r 
#----

#' @examples 
#' t1 <- Sys.time()
#' diffmin(t1)
diffmin <- function(t,t2=Sys.time()) round(difftime(t2, t, unit = "min"),2)

#' Convert standard string formatted date to POSIXct Format: 2015-01-01T13:00:51Z
as_timestamp <- function(x) as.POSIXct(x, format='%Y-%m-%dT%H:%M:%S', tz='UTC')

fext <- function(filePath){ 
  ex <- strsplit(basename(filePath), split="\\.")[[1]]
  return(ex[length(ex)])
}

#'Saves parameters for script run from global environment to csv
saveParams <- function(parPF) {
  ls(all.names=TRUE,pattern='^\\.',envir=.GlobalEnv) %>%
    enframe(name=NULL,value='var') %>% 
    filter(!var %in% c('.Random.seed','.runid','.script','.parPF')) %>% #
    mutate(
      script=get('.script'),
      runid=get('.runid'),
      ts=strftime(Sys.time(),format='%Y-%m-%d %T', usetz=TRUE),
      value=map_chr(var,~{toString(get(.))})) %>%
    select(script,runid,ts,var,value) %>%
    arrange(var) %>%
    write_csv(parPF,append=file.exists(parPF))
}

#' Human-readable elapsed time
#' tStart: POSIXct. Start time 
#' tEnd: POSIXct. End time. Default is current time
#' @examples
#' hre(Sys.time()-60)
#' 
hre <- function(tStart, tEnd=Sys.time()) {
  # Calculate the difference in seconds
  elapsed_seconds <- as.numeric(difftime(tEnd, tStart, units = "secs"))
  
  # Calculate hours, minutes, and seconds
  hours <- floor(elapsed_seconds / 3600)
  minutes <- floor((elapsed_seconds %% 3600) / 60)
  seconds <- round(elapsed_seconds %% 60)
  
  # Construct the output string
  if (hours > 0) {
    output <- sprintf("%d hr, %d min, %d sec", hours, minutes, seconds)
  } else if (minutes > 0) {
    output <- sprintf("%d min, %d sec", minutes, seconds)
  } else {
    output <- sprintf("%d sec", seconds)
  }
  
  return(output)
}

# pd <- function(...) file.path(.pd,...)
# wd <- function(...) file.path(.wd,...)

pathFactory <- function(path) {
  if (missing(path)) stop("'path' must be provided")
  function(...) file.path(path, ...)
}


