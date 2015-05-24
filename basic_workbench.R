#library(Rhipe)
#rhinit()

data_input = "small.csv"
pre_processed_input = "pre_processed_input.csv"
dir_on_hdfs = "/"



#setup<-expression(  
# Remove tudo do ambiente  
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

#Salva a entrada pre-processada no disco local  
write.table(data, pre_processed_input, sep=",", row.names=FALSE, col.names=TRUE)

#Exporta esta entrada pre-processada para o HDFS
#------TO-DO: Ver se tem como evitar esse write.table e mandar direto da memória para o HDFS---------
rhput(pre_processed_input, dir_on_hdfs)

# )

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
      MUNIC_MOV <-as.numeric(line[7])
    )    
    
    rhcollect(i,outputvalue)    
  })
  
  
  
)


reduce<-expression(
  
  pre={
    reduceoutputvalue <-data.frame()
  },
  reduce={
    outputvalue<-rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post={
    reduceoutputkey <- reduce.key[1]    
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)


mr <- rhwatch(
  #setup    = setup,
  map      = map,
  reduce   = reduce,
  input    = rhfmt(pre_processed_input, type = "text"),
  output   = rhfmt(output, type = "sequence"),
  readback = FALSE,
  
)

rhread(output)