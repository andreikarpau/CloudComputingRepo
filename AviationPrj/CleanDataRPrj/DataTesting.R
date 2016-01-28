rm(list = ls())
library(data.table)

tb1 <- fread("Test1.csv")
tb2 <- fread("Test2.csv")

print("G1T1 ACV origin + dest:")
print(sum(tb1$V1 == "ACV") + sum(tb1$V2 == "ACV") + sum(tb2$V1 == "ACV") + sum(tb2$V2 == "ACV"))


print("G1T3 day 6 on time arrival perf:")
day <- 6
items <- tb1[tb1$V9 == day]
items <- items[!is.na(items$V5)]

items2 <- tb2[tb2$V9 == day]
items2 <- items2[!is.na(items2$V5)]

nonNaCount <- nrow(items) + nrow(items2);

print((nonNaCount - (sum(items$V5) + sum(items2$V5))) / nonNaCount)


print("G2T1 airport depature perf:")
origin <- "ATL"
airlineId <-20374

items <- tb1[tb1$V1 == origin]
items <- items[items$V3 == airlineId]
items <- items[!is.na(items$V6)]

items2 <- tb2[tb2$V1 == origin]
items2 <- items2[items2$V3 == airlineId]
items2 <- items2[!is.na(items2$V6)]

itemsCount <- nrow(items) + nrow(items2)
print((itemsCount - (sum(items$V6) + sum(items2$V6))) / itemsCount)



print("G2T3 airport origin depature arrival perf:")
origin <- "BDL"
depature <- "TPA"
airlineId <-19393

items <- tb1[tb1$V1 == origin]
items <- items[items$V2 == depature]
items <- items[items$V3 == airlineId]
items <- items[!is.na(items$V5)]

items2 <- tb2[tb2$V1 == origin]
items2 <- items2[items2$V2 == depature]
items2 <- items2[items2$V3 == airlineId]
items2 <- items2[!is.na(items2$V5)]

itemsCount <- nrow(items) + nrow(items2)
print((itemsCount - (sum(items$V5) + sum(items2$V5))) / itemsCount)



print("G2T4 mean arrival delay:")
origin <- "BWI"
depature <- "STL"

items <- tb1[tb1$V1 == origin]
items <- items[items$V2 == depature]
items <- items[!is.na(items$V7)]

items2 <- tb2[tb2$V1 == origin]
items2 <- items2[items2$V2 == depature]
items2 <- items2[!is.na(items2$V7)]

print ((sum(items$V7) + sum(items2$V7)) / (nrow(items) + nrow(items2)))

print("G3T2 XY path:")
origin <- "PHL"
depature <- "IAH"
depature2 <- "MOB"
date1 <- '2006-07-14'
date2 <- '2006-07-16'


items <- tb1[tb1$V1 == origin]
items <- items[items$V2 == depature]
items <- items[items$V8 == date1]
items <- items[items$V12 <= 1200]
items <- items[which.min(items$V11)]

items2 <- tb1[tb1$V1 == depature]
items2 <- items2[items2$V2 == depature2]
items2 <- items2[items2$V8 == date2]
items2 <- items2[1200 <= items2$V12]
items2 <- items2[which.min(items2$V11)]

print(items)
print("")
print(items2)
