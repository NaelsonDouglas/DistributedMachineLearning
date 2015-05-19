#Referência para o bug "variable discovered, but not found: https://groups.google.com/forum/#!searchin/rhipe/Following$20variables$20were$20discovered$20but$20not$20found$3A/rhipe/DPRMo92EtIc/G0R2oe7fvBMJ"
#Este warning diz que o Rhipe tentou copiar a variável do workspace, mas ele não existia lá. Então ele cria uma nova.
library(Rhipe)
rhinit()

rhput("small.csv","/small.csv")

map <-expression(
  
  
  
  
  #Executa os maps 
  lapply(seq_along(map.keys), function(i){  
    i2=i+1
    #separa a linha do .csv 
    #map.values[i] == coluna i  
    #map.values[[i]] == linha i
    current_line = strsplit(map.values[[i]], ";")[[1]]           
    next_line = strsplit(map.values[[i2]], ";")[[1]]
    
    if (current_line != next_line ){ 
      
  #Este dataframe armazena uma única linha do .csv e a usa como value.
  outputvalue <- data.frame(    
    UF =  as.numeric(current_line[1]),
    CITY = as.numeric(current_line[2]),
    ANO = as.numeric(current_line[3]),
    MES = as.numeric(current_line[4]),
    MUNIC_MOV = as.numeric(current_line[5]),
    MUNIC_RES  = current_line[6], ##Este campo contém caracteres não-numéricos
    IDADE = as.numeric(current_line[7]),
    SEXO = as.numeric(current_line[8]),
    DIAG_PRINC = current_line[9],
    DIAS_PERM = as.numeric(current_line[10]),
    VAL_UTI = as.numeric(current_line[11]),
    VAL_TOT = as.numeric(current_line[12]),
    MORTE = as.numeric(current_line[13]),
    EDUCATION = as.numeric(current_line[14]),
    BATHROOM = as.numeric(current_line[15]),
    ELETRICITY = as.numeric(current_line[16]),
    LITERACY = as.numeric(current_line[17]),
    URB_RUR = as.numeric(current_line[18]),
    stringsAsFactors = FALSE
  ) 
  #Faz a coleta do data-frame
  outputkey = i
  rhcollect(outputkey,outputvalue)
}

# Checa se os registros são duplicados. 
# Em caso positivo nada é feito e o map para sem nenhum valor emitido(valores repetidos não são postos à frente)
# Em caso negativo, a coleta é feita usando o número da linha como key

else{}     
  }))

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


mr <- rhwatch(
  map      = map,
  reduce   = reduce,
  input    = rhfmt("small.csv", type = "text"),
  output   = rhfmt("output", type = "text"),
  readback = TRUE  
)