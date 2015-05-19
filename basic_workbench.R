library(Rhipe)
rhinit()


in_mem <- rhread("small.csv",type="text",max=-1,mc=FALSE,buffsize=2*1024*1024)
header <-in_mem[1]
header <-strsplit(header[[1]],";")[[1]]
in_mem <-in_mem[2:length(in_mem)]
in_mem <-strsplit(in_mem[[1]],";")[[1]]



rhsave(in_mem,header, file="/dataBase.Rdata")





map <-expression( 
  load("dataBase.Rdata")
  )


#Reducer
reduce <- expression(
 #pre = {},
  #reduce = {},
  #post = {}
)


#Driver
t1<-proc.time()

mr <- rhwatch(
  map      = map,
  reduce   = reduce,
  input    = rhfmt("small.csv",type="text"),
  output   = rhfmt("outputWB", type = "sequence"),
  readback = TRUE,   
  shared=c("/dataBase.Rdata")
)
)
t2<-proc.time()

print(t2-t1)




