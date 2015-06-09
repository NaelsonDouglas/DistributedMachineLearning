library(Rhipe)
rhinit()

#Parâmetros do usuário
output="output"
processed_input_tbl = "processed_input_tbl.csv"
processed_input_rdt = "processed_input_rdt.Rdata"
dir_on_hdfs = "/"
data_input = "small.csv"
src="small.csv"
rhput(data_input, src)

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
data <- data[order(data$UF,data$ANO, data$MES, data$MUNIC_RES, data$IDADE, data$SEXO),]

# remove linha que contenha qualquer NA
data<-na.omit(data)

#Salva a entrada pre-processada no disco local. Este arquivo será a entrada do maper
write.table(data, processed_input_tbl, sep=",", row.names=FALSE, col.names=FALSE) 
#Gera um Rdata com a entrada pre-processada. Este rdata será compartilhado com todos os mapers  
rhsave(data,file=processed_input_rdt)


#Exporta esta entrada pre-processada para o HDFS
#------TO-DO: Ver se tem como evitar esse write.table e mandar direto da memória para o HDFS---------
rhput(processed_input_tbl, dir_on_hdfs)


#setup usado para criar a variável global com o Rdata
map.setup = expression({
  load("processed_input_rdt.Rdata") 
})

#Map
map<-expression(  
  
  lapply(seq_along(map.keys), function(i){
    line = strsplit(map.values[[i]],",")[[1]]
       
    outputvalue<- data.frame(
      UF        = as.numeric(line[1]),
      CITY      = as.numeric(line[2]),
      ANO       = as.numeric(line[3]),
      MES       = as.numeric(line[4]),
      MUNIC_MOV = as.numeric(line[5]),
      MUNIC_RES = as.numeric(line[6]),
      IDADE     = as.numeric(line[7]),
      stringsAsFactors = FALSE
    )     
    load("processed_input_rdt.Rdata") #lê a base de dados read-only para poder usar a mesma como comparação.      
    
    #se a linha atual e a próxima forem iguais, então nada é emitido  
    next_line = data[i+1,]    
    
    #No último maper next_line será uma linha cheia de NA's, se eles entrarem assim no if abaixo, da problema. Por isto preenchi tudo com um valor arbitrário
    if(is.na(next_line[1])){
      next_line[1:length(next_line)] = -1
    }      
    
    if (      
        next_line$UF         == outputvalue$UF &
        next_line$CITY       == outputvalue$CITY &
        next_line$ANO        == outputvalue$ANO &
        next_line$MES        == outputvalue$MES &
        next_line$MUNIC_MOV  == outputvalue$MUNIC_MOV &
        next_line$MUNIC_RES  == outputvalue$MUNIC_RES &
        next_line$IDADE      == outputvalue$IDADE         
      
    )
    {
      #Se as duas tuplas foram iguais, o map não emite valores.
    }
    else
    {    
      #emite a linha, caso ela seja diferente da próxima
      rhcollect(i,outputvalue)  
    }
  })   
)

#Reduce
reduce<-expression(
  
  pre={
    #cria o data.frame que será jogado na saída
    reduceoutputvalue <-data.frame()
  },
  reduce={
    #combina os vários maps dentro do dataframe
    reduceoutputvalue<-rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post={
    #define uma única key, já que teremos uma única saída
    reduceoutputkey <- reduce.key[1]    
    #salva a combinação de todos os maps no HDFS
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)

#driver
mr <- rhwatch( 
  map      = map,
  reduce   = reduce,
  input    = rhfmt(processed_input_tbl, type = "text"),
  output   = rhfmt(output, type = "sequence"),
  readback = FALSE,
  setup=expression(map=map.setup),
  shared=c("/processed_input_rdt.Rdata")  
)
rhread(output)

