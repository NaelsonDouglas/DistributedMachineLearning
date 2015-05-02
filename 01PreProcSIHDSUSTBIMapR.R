##TO-DO: Perguntar ao Elias se o .csv contém mais campos não numéricos

library(Rhipe)
rhinit()

{
# Remove tudo do ambiente
closeAllConnections()
rm(list=ls())

# Ajusta o diret?rio de trabalho
setwd("/home/username/Desktop/Elias")

# Leitura do arquivo

#Isto morre, o arquivo fica no HDFS e não no disco local
data <- (read.csv("TBI_SUS_SIHD.csv", stringsAsFactors=FALSE, sep=";", header=T), nrow=10)

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
# Cronometrando (Rhipe já tem suporte nativo para isto)
ptm <- proc.time()
}


map <-expression({
{   
  # Variáveis de trabalho
  0        <- 0
  CITY_ant      <- 0
  ANO_ant       <- 0
  MES_ant       <- 0
  MUNIC_RES_ant <- 0
  IDADE_ant     <- 0
  SEXO_ant      <- 0
  
  attach(data)
}

#Executa os maps 
lapply(seq_along(map.keys, function(i){

  
  #substituir o print pela funcção distribuida
  if (i == 1){  
    # Imprime um cabeçalho       
    print("  SEQ     UF      CIDADE     ANO   MES        RESID.  IDADE   SEXO")
    print("  ---    ----     -------   ----   ---        ------  -----   ----")  
  }
  
  #separa a linha do .csv pelos ; dela
  line = strsplit(map.values[[i]], ";")[[1]]
  
  #Este dataframe armazena uma única linha do .csv e a usa como value.
  outputvalue <- data.frame(    
      UF = as.numeric(line[1]),
      CITY = as.numeric(line[2]),
      ANO = as.numeric(line[3]),
      MES = as.numeric(line[4]),
      MUNIC_MOV = as.numeric(line[5]),
      MUNIC_RES  = line[6], ##Este campo contém caracteres não-numéricos
      IDADE = as.numeric(line[7]),
      SEXO = as.numeric(line[8]),
      DIAG_PRINC = line[9],
      DIAS_PERM = as.numeric(line[10]),
      VAL_UTI = as.numeric(line[11]),
      VAL_TOT = as.numeric(line[12]),
      MORTE = as.numeric(line[13]),
      EDUCATION = as.numeric(line[14]),
      BATHROOM = as.numeric(line[15]),
      ELETRICITY = as.numeric(line[16]),
      LITERACY = as.numeric(line[17]),
      URB_RUR = as.numeric(line[18]),
      stringsAsFactors = FALSE
    )
  # Checa se os registros são duplicados. 
  # Em caso positivo nada é feito e o map para sem nenhum valor emitido(valores repetidos não são postos à frente)
  # Em caso negativo, a coleta é feita usando o número da linha como key
  if (    UF_ant        == data[i,1] &
          CITY_ant      == data[i,2] &
          ANO_ant       == data[i,3] &
          MES_ant       == data[i,4] &
          MUNIC_RES_ant == data[i,7] &
          IDADE_ant     == data[i,8] &
          SEXO_ant      == data[i,9] 
  )
  {
    #valor repetido dispensado
  }    
  else
  { 
    #Faz a coleta
    outputkey = i
    rhcollect(outputkey,outputvalue)     
  }
  
}))
})


reduce <- expression(
  pre = {
    reduceoutputvalue <- reduce.values    
  },
  reduce = {
    #remonta todas as linhas mapeadas
    reduceoutputvalue <- rebind(reduceoutputvalue, reduce.values)    
  },
  post = {
    #joga os valores para a saída sob uma mesma key (já que temos apenas um arquivo para ser salvo)
    reduceoutputkey <- reduce.key[1]    
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