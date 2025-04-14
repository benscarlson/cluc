# Helper functions for bien ranges

#' Convert the future ranges from default format to binary
#TODO!!!! LOOK AT THIS LATER !!!
# ppm has values like TP05, etc, while rangebag has X0.083, X0.165, X0.833
# not totally sure how these map, but for now, just take the second layer
binFutRange <- function(rng,full_domain=FALSE) { #tp='TP05',
  # rng1 <- futRngs$rast[[1]][[2]]
  #rng1 <- rng[[tp]]
  rng1 <- rng[[2]]
  if(full_domain) {
    rng1 <- abs(rng1)
  }
  rng1 <- ifel(rng1 >= 3,1,NaN)
  
  return(rng1)
}

#' Convert the present range from the default format to binary
binPresRange <- function(x) {
  x <- x - 4
  x <- ifel(x >=2,1,NaN)
  names(x) <- 'present' #TODO: maybe this should be "historical" as well. 
  return(x)
}