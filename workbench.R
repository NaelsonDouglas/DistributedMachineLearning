#library(Rhipe)
#rhinit()

#Parâmetros do usuário
output="output"
data_input = "small.csv"
processed_input_tbl = "processed_input_tbl.csv"
processed_input_rdt = "processed_input_rdt.Rdata"
dir_on_hdfs = "/"
src="small.csv"


#lê localmente o .csv 
data <- (read.csv(src, stringsAsFactors=FALSE, sep=";", header=T))

# Remove wrong values
data <- subset(data, (BATHROOM   > 0 & BATHROOM   <=1 &
                        ELETRICITY > 0 & ELETRICITY <=1 &
                        LITERACY   > 0 & LITERACY   <=1 &
                        URB_RUR    > 0 & URB_RUR    <=1 &
                        EDUCATION  > 0 & EDUCATION  <=1 &
                        ANO >= 2002    & ANO        <=2012
)
)

# Ordenando dados 
data <- data[order(data$ANO, data$MES, data$MUNIC_RES, data$IDADE, data$SEXO)]

# remove linha que contenha qualquer NA
data<-na.omit(data)

#Salva a entrada pre-processada no disco local. Este arquivo será a entrada do maper
write.table(data, processed_input_tbl, sep=",", row.names=FALSE, col.names=TRUE) 
#Gera um Rdata com a entrada pre-processada. Este rdata será compartilhado com todos os mapers  
data <-"kek"
rhsave(data,file=processed_input_rdt)


#Exporta esta entrada pre-processada para o HDFS
#------TO-DO: Ver se tem como evitar esse write.table e mandar direto da memória para o HDFS---------
rhput(processed_input_tbl, dir_on_hdfs)

# )

#setup usado para criar a variável global com o Rdata
map.setup = expression({
  load("processed_input_rdt.Rdata") # no need to give full path
})

#Map
map<-expression(  
  
  lapply(seq_along(map.keys), function(i){
    line = strsplit(map.values[[i]],",")[[1]]    
    
    
    outputvalue<- data.frame(
      MES       <-as.numeric(line[1]),
      IDADE     <-as.numeric(line[2]),
      ANO       <-as.numeric(line[3]),
      MUNIC_RES <-as.numeric(line[4]),
      CITY      <-as.numeric(line[5]),
      UF        <-as.numeric(line[6]),
      MUNIC_MOV <-as.numeric(line[7]),
      stringsAsFactors <- FALSE
    )    
    #Esta linha abaixo é um teste, remover ela
    load("processed_input_rdt.Rdata")
    outputvalue <-data
    
    rhcollect(i,outputvalue)})   
)

#Reduce
reduce<-expression(
  
  pre={
    reduceoutputvalue <-data.frame()
  },
  reduce={
    
    reduceoutputvalue<-rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post={
    reduceoutputkey <- reduce.key[1]    
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)

#driver
mr <- rhwatch(
  #setup = expression(rhload("/processed_input_rdt.Rdata")),
  map      = map,
  reduce   = reduce,
  input    = rhfmt(processed_input_tbl, type = "text"),
  output   = rhfmt(output, type = "sequence"),
  readback = FALSE,
  setup=expression(map=map.setup),
  shared=c("/processed_input_rdt.Rdata")
  
)

