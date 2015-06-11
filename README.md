There are two main files in this project:
PreProc-SIHD-SUS-TBI.R is the serial version of the code
PreProc-SIHD-SUS-TBI-MapReduce.R is the parallel/Rhipe version of the same code

In order to run the serial version of the code it's only needed to write
$Rscript(PreProc-SIHD-SUS-TBI.R) on the system terminal or Source("PreProc-SIHD-SUS-TBI.R") on R terminal.

To run the parallel version all it takes is:
-$Rscript(PreProc-SIHD-SUS-TBI-MapReduce.R) on the system terminal or Source("PPreProc-SIHD-SUS-TBI-MapReduce.R") on R terminal.
After that a file with the var::output name will be stored on the HDFS(not locally), to list these files you can use on rhls("/[directory to be listed]") on R.
-In order to get this file you need to type rhread(var::output) on the R console, it will return a data.frame which can be stored on a variable. Alternatively you can download the raw file via Hadoop using the console this way:
$hadoop dfs -get [file path on the HDFS] [dest file]


For quick tests purposes the code is set to run over the small.csv database, which is a tiny \


