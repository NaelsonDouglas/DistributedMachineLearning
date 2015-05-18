#Referência para o bug "variable discovered, but not found: https://groups.google.com/forum/#!searchin/rhipe/Following$20variables$20were$20discovered$20but$20not$20found$3A/rhipe/DPRMo92EtIc/G0R2oe7fvBMJ"
#Este warning diz que o Rhipe tentou copiar a variável do workspace, mas ele não existia lá. Então ele cria uma nova.
library(Rhipe)
rhinit()
rhput("in.txt","/in2.txt",deletedest=TRUE)


#Maper
map <-expression(  
  #Executa os maps 
  lapply(seq_along(map.keys), function(i){      
    #separa a linha do .csv pelos ; dela
    #map.values[i] == coluna i---- map.values[[i]] == linha i
    line = strsplit(map.values[[i]], ";")[[1]]           
    
    #Este dataframe armazena uma única linha do .csv e a usa como value.
    outputvalue <- data.frame(    
      UF =  as.numeric(line[1]),
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
       
      #Faz a coleta
      outputkey = i
      rhcollect(outputkey,outputvalue)
  }))


#Reducer
reduce <- expression(
  pre = {    
    reduceoutputvalue <- data.frame()   
  },
  reduce = {
    reduceoutputvalue <- rbind(reduceoutputvalue, do.call(rbind, reduce.values))     
  },
  post = {
    #joga os valores para a saída sob uma mesma key (já que temos apenas um arquivo para ser salvo)
    reduceoutputkey <- reduce.key[1]    
    rhcollect(reduceoutputkey, reduceoutputvalue)
  }
)

#Driver
mr <- rhwatch(
  map      = map,
  reduce   = reduce,
  input    = rhfmt("in.txt", type = "text"),
  output   = rhfmt("outputWB", type = "sequence"),
  readback = TRUE  
)


#lê a saída 
rhread(outputWB)