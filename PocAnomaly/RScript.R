# to be added to an R Script block in ML Studio

library("magrittr")
library("ggplot2")

# load data from input
taxis <- maml.mapInputPort(1)

# build a chart
# here plotting longitude vs latitude using points
# and setting the limits on the viewport, if needed
p1 <- taxis %>%
        ggplot(aes(pickup_longitude, pickup_latitude, color = passenger_count)) +
        geom_point() +
        xlim(-75, -72) +
        ylim(40, 42)
        
# exporting the chart to an in-memory pdf        
pdf('mypdf.pdf', width = 6, height = 4, paper = 'special')
plot(p1)  
dev.off()

# using a base64 encoding in order to build the data frame to be exported
library(caTools)  
b64ePDF <- function(filename) {  
            maxFileSizeInBytes <- 15 * 1024 * 1024 # 5 MB  
            return(base64encode(readBin(filename, "raw", n = maxFileSizeInBytes)))  
}  

d2 <- data.frame(pdf = b64ePDF("mypdf.pdf"))
maml.mapOutputPort("d2");
