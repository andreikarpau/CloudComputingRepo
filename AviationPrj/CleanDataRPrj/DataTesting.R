rm(list = ls())
library(data.table)

tb1 <- fread("On_Time_On_Time_Performance_2006_7.csv")
tb2 <- fread("On_Time_On_Time_Performance_2006_8.csv")

print("ACV origin + dest:")
print(sum(tb1$V1 == "ACV") + sum(tb1$V2 == "ACV") + sum(tb2$V1 == "ACV") + sum(tb2$V2 == "ACV"))