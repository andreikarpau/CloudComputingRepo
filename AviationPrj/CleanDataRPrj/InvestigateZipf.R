rm(list = ls())

input <- '../../../Capstone/Output/RankAirportsZipf/part-r-00000'
airports <- read.csv(input)
names(airports) <- c ("airport", "popularity")

numbers <- c(1:length(airports$popularity))

zipfProportion <- airports$popularity[1] / numbers

zipfPop <- (zipfProportion - min(zipfProportion)) / (max(zipfProportion) - min(zipfProportion))
pop <- (airports$popularity - min(airports$popularity)) / (max(airports$popularity) - min(airports$popularity))


plot(numbers,zipfPop,type="l",col="red")
lines(numbers,pop,col="green")
lines(numbers,dgeom(numbers, 0.5, log = FALSE),col="blue")
lines(numbers,dpois(numbers, 0.999, log = FALSE),col="black")

#plot(log(pop),log(zipfPop),type="l",col="red")
#lines(log(pop),log(dgeom(numbers, 0.5, log = FALSE)),col="blue")
#lines(log(pop),log(dpois(numbers, 0.999, log = FALSE)),col="black")