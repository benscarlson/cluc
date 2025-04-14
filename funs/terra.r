# Helper functions for terra

#' Maps zonal stats over all layers and returns a tibble
zonalStack <- function(ra, zones,fun='mean') {
  # ra <- loss_km2; zones <- loss_cases %>% subst(0,NaN); fun='sum'
  
  # Make sure the layers match before creating the tibble
  invisible(assert_that(all(names(ra)==names(zones))))
  
  tibble(layer_name=names(ra),
         ra=as.list(ra), #split the raster stack into a list column
         zones=as.list(zones)) %>%
    mutate(zonal_data=map2(ra,zones,~{
      # .x <- ra[[1]]; .y <- zones[[1]]
      x <- zonal(.x,.y,fun=fun,na.rm=TRUE)
      names(x) <- c('zone',fun)
      return(as_tibble(x))
    })) %>% 
    select(-ra,-zones) %>%
    unnest(cols=zonal_data) %>%
    arrange(layer_name,zone)
}