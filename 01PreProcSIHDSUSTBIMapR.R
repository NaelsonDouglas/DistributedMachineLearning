
library(Rhipe)
rhinit()



map <-expression({


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
  # Ordenando dados 
  outputvalue <- outputvalue[order(outputvalue$ANO, outputvalue$MES, outputvalue$MUNIC_RES, outputvalue$IDADE, outputvalue$SEXO),]  
  # remove linha que contenha qualquer NA
  outputvalue<-na.omit(outputvalue)
  
  # Checa se os registros são duplicados. 
  # Em caso positivo nada é feito e o map para sem nenhum valor emitido(valores repetidos não são postos à frente)
  # Em caso negativo, a coleta é feita usando o número da linha como key
  if (    UF_ant        == outputvalue[i,1] &
          CITY_ant      == outputvalue[i,2] &
          ANO_ant       == outputvalue[i,3] &
          MES_ant       == outputvalue[i,4] &
          MUNIC_RES_ant == outputvalue[i,7] &
          IDADE_ant     == outputvalue[i,8] &
          SEXO_ant      == outputvalue[i,9] 
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