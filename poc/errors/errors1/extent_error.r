library(terra)

# Run cluc_hpc.r until you have futRngs and pres objects
#TODO: save these files so I can easliy run later

# Extract rasters
fut <- futRngs$rast[[1]]
pre <- pres

# Replace NA with 0, and change values for differentiation
fut2 <- subst(fut,1,2)  # Future = 2

#Note: Won't work if there are overlapping pixels, In this case, need to expand and add.
both <- merge(fut2, pre, algo=2)

# Optional: define a color palette for interpretation

colz <- c("#2C7BB6","#D7191C")
plot(both, col = colz, legend = FALSE)

# TODO: put this into a report instead

