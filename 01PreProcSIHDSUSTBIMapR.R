library(Rhipe)
rhinit()


map <-expression({
{
  # Remove tudo do ambiente
  closeAllConnections()
  rm(list=ls())
  
  # Ajusta o diret?rio de trabalho
  setwd("/home/username/Desktop/Elias")
  
  # Leitura do arquivo
  data <- (read.csv("TBI_SUS_SIHD.csv", stringsAsFactors=FALSE, sep=";", header=T))
  
  
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
  data <- data[order(data$ANO, data$MES, data$MUNIC_RES, data$IDADE, data$SEXO),]
  
  # remove linha que contenha qualquer NA
  data<-na.omit(data)
  
  # Vari?veis de trabalho
  0        <- 0
  CITY_ant      <- 0
  ANO_ant       <- 0
  MES_ant       <- 0
  MUNIC_RES_ant <- 0
  IDADE_ant     <- 0
  SEXO_ant      <- 0
  
  attach(data)
  
  # Cronometrando
  ptm <- proc.time()
  
  # Monta um cabe?alho
  cabec1 <- c("  SEQ     UF      CIDADE     ANO   MES        RESID.  IDADE   SEXO")
  cabec2 <- c("  ---    ----     -------   ----   ---        ------  -----   ----")
}
  
  #Executa os maps (se_along(map.keys, function(i))) --> aplica function(i) sobre todos valores de map.keys
  lapply(seq_along(map.keys, function(i){
    
    if (i == 1){
      print(cabec1)
      print(cabec2)
    }
    # Marca registros duplicados
    if (  UF_ant        == data[i,1] &
            CITY_ant      == data[i,2] &
            ANO_ant       == data[i,3] &
            MES_ant       == data[i,4] &
            MUNIC_RES_ant == data[i,7] &
            IDADE_ant     == data[i,8] &
            SEXO_ant      == data[i,9] 
    )
    { # DELETAR
      data[i,19] <- c("D") 
    }
    else
    { # MANTER
      data[i,19] <- c("M") 
    }
    
    output <- c(i, data[i,1], data[i,2], data[i,3],  data[i,4],  data[i,7],  data[i,8],  
                data[i,9], data[i,16], data[i,18], data[i,19] 
    )
    print(output)  
    
    UF_ant        <- data[i,1] 
    CITY_ant      <- data[i,2]
    ANO_ant       <- data[i,3]
    MES_ant       <- data[i,4]
    MUNIC_RES_ant <- data[i,7]
    IDADE_ant     <- data[i,8]
    SEXO_ant      <- data[i,9]    
    
  }))
})


reduce <- expression(
  pre = {
    reduceoutputvalue <- data.frame()
  },
  reduce = {
    reduceoutputvalue <- rbind(reduceoutputvalue, do.call(rbind, reduce.values))
  },
  post = {
    reduceoutputkey <- reduce.key[1]
    attr(reduceoutputvalue, "location") <- reduce.key[1:3]
    names(attr(reduceoutputvalue, "location")) <- c("FIPS","county","state")
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)



mr <- rhwatch(
  map      = map,
  reduce   = reduce,
  input    = rhfmt("in.txt", type = "text"),
  output   = rhfmt("out", type = "sequence"),
  readback = FALSE
)