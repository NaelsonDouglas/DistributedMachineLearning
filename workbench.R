#library(Rhipe)
#rhinit()

data_input = "small.csv"
pre_processed_input = "pre_processed_input.csv"
dir_on_hdfs = "/"



setup<-expression(
  
  # Remove tudo do ambiente  
  src="small.csv",
  closeAllConnections(),  
  rm(list=ls()),
  
  #lê localmente o .csv
  data <- (read.csv(data_input, stringsAsFactors=FALSE, sep=";", header=T)),
  
  
  # Remove wrong values
  data <- subset(data, (BATHROOM   > 0 & BATHROOM   <=1 &
                          ELETRICITY > 0 & ELETRICITY <=1 &
                          LITERACY   > 0 & LITERACY   <=1 &
                          URB_RUR    > 0 & URB_RUR    <=1 &
                          EDUCATION  > 0 & EDUCATION  <=1 &
                          ANO >= 2002    & ANO        <=2012
  )
  ),
  
  # Ordenando dados 
  data <- data[order(data$ANO, data$MES, data$MUNIC_RES, data$IDADE, data$SEXO)],
  
  # remove linha que contenha qualquer NA
  data<-na.omit(data),
  
  #Salva a entrada pre-processada no disco local  
  write.table(data, pre_processed_input, sep=",", row.names=FALSE, col.names=TRUE),
  
  #Exporta esta entrada pre-processada para o HDFS
  #------TO-DO: Ver se tem como evitar esse write.table e mandar direto da memória para o HDFS---------
  rhput(pre_processed_input, dir_on_hdfs)
  
  )

map<-expression(
  
  lapply(seq_along(map.keys), function_map(i)),
  
  
  function_map <-function(i){
    
  }
)


reduce<-expression()


mr <- rhwatch(
  setup    = setup,
  map      = map,
  reduce   = reduce,
  input    = rhfmt(pre_processed_input, type = "text"),
  output   = rhfmt(output, type = "sequence"),
  readback = FALSE,
  
)

rhread(output)