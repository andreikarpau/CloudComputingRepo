rm(list = ls())
library(data.table)

dataPath <- '../../../ubuntu/ClearedData/csv/'
outputPath <- '../../DataClearing/'
files <- list.files('../../../ubuntu/ClearedData/csv')

#fullPath <- file
#name <- fileName
#output <- outputPath

clearDataAndSave <- function(fullPath, name, output){
  print(name)
  tb <- fread(fullPath)
  
  outTb <- list()
  outTb$Origin <- tb$Origin
  outTb$Dest <- tb$Dest
  outTb$AirlineID <- tb$AirlineID
  outTb$AirlineCode <- tb$Carrier
  outTb$ArrDelayed <- tb$ArrDel15
  outTb$DepDelayed <- tb$DepDel15
  outTb$ArrDelayMinutes <- tb$ArrDelayMinutes
  outTb$FlightDate  <- tb$FlightDate
  outTb$DayOfWeek <- tb$DayOfWeek
  outTb$CRSDepatureTime <- tb$CRSDepTime
  outTb$ArrTime <- tb$ArrTime

  outPath <- paste(output, name, sep = "")
  write.table(outTb, outPath, sep=",", row.names=FALSE, quote=FALSE, col.names=TRUE)
  rm(tb)
  rm(outTb)
}

for (i in 233:240){
  print(i)
  
  fileName <- files[i];
  file <- paste(dataPath, fileName, sep = "")
  
  clearDataAndSave(file, fileName, outputPath);
}

#write.table(DT,"test.csv",sep=",",row.names=FALSE,quote=FALSE)
# DT1$a <- NULL
# DT$b %in% DT1$e

# mylist <- list()
# for (i in 1:nrow(DT))
#   mylist[[DT$b[i]]] <- DT$c[i]
#
# for (i in 1:nrow(DT)){
# row <- DT[i]
# mylist[[row$b]] <- row$c
# }
#

# lapply(files, function(x) {
#  t <- read.table(x, header=T) # load file
#  # apply function
#  out <- function(t)
#    # write to file
#    write.table(out, "path/to/output", sep="\t", quote=F, row.names=F, col.names=T)
#})