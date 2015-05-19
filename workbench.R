src="small.csv"
databaseInRdata="dataBaseInRdata"
output="outputWB"


#Lê a base de dados para poder salvar ela em um Rdata global no HDFS
Rdata <- rhread(src,type="text",max=-1,mc=FALSE)

Rdata <- strsplit(Rdata,";")

for (i in 1:length(Rdata)){
  
  line = Rdata[i]
  
  
  
}


Rdata <- subset(Rdata, (BATHROOM   > 0 & BATHROOM   <=1 &
                          ELETRICITY > 0 & ELETRICITY <=1 &
                          LITERACY   > 0 & LITERACY   <=1 &
                          URB_RUR    > 0 & URB_RUR    <=1 &
                          EDUCATION  > 0 & EDUCATION  <=1 &
                          ANO >= 2002    & ANO        <=2012
)
)
# Ordenando dados 
Rdata <- Rdata[order(Rdata$ANO, Rdata$MES, Rdata$MUNIC_RES, Rdata$IDADE, Rdata$SEXO)]

#SALVA O Rdata no HDFS
rhsave(Rdata,file=databaseInRdata)

#Organizando a base de dados



map<-expression(
  lapply(seq_along(map.keys), function(i){
    
    #para não comparar o header
    if (i>1){
      
      
    }
    else{
      outputcollect(0,map.values[i])
    }
    
    
    
    
    #Fim function(i)  
  })
  
  
  
)


reduce<-expression(
  
)


mr <- rhwatch(
  map      = map,
  reduce   = reduce,
  input    = rhfmt(src, type = "text"),
  output   = rhfmt(output, type = "sequence"),
  readback = FALSE,
  shared=c(databaseInRdata)
)

rhread(output)