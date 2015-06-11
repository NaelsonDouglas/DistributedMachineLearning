## R Version (/src/R)


There are two main files in this project:
PreProc-SIHD-SUS-TBI.R is the serial version of the code

PreProc-SIHD-SUS-TBI-MapReduce.R is the parallel/Rhipe version of the same code

The other .R files were created only for annotations and tests.



In order to run the serial version of the code it's only needed to write
on the system terminal:
>$Rscript(PreProc-SIHD-SUS-TBI.R)  

or on R terminal:
>Source("PreProc-SIHD-SUS-TBI.R")

To run the parallel version all it takes is:

on the system terminal

>$Rscript(PreProc-SIHD-SUS-TBI-MapReduce.R) 

or on the system terminal or

>Source("PPreProc-SIHD-SUS-TBI-MapReduce.R") 


After that a file with the var::output name will be stored on the HDFS(not locally), to list these files you can use on R you cand type
>rhls("/[directory to be listed]")

-In order to get this file you need to type rhread(var::output) on the R console, it will return a data.frame which can be stored on a variable.

Alternatively you can download the raw file via Hadoop using the console this way:
>$hadoop dfs -get [file path on the HDFS] [dest file]



For quick tests purposes the code is set to run over the small.csv database, which contains the three last lines equal. When you run the code over it, two of these lines will be removed and the data.frame without them will be writen to the HDFS.
To change the data input, you only need to set the new input name on var::data_input.