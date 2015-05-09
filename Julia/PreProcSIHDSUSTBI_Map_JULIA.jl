#Lê os dados
#Comentários sobre leitura iterativa de .csv (não podemos ler o arquivo todo, tem que ser linha a linha e enviar elas indiviadualmente aos mapers)
#**https://groups.google.com/forum/#!searchin/julia-users/read$20csv/julia-users/vvLRv3SN6NQ/uZcaCBcoTPQJ

#=
A ideia para fazer a leitura distribuida dos dados é a seguinte

fazer a leitura linha a linha (ou de uma quantidade n de linhas) do arquivo e dar spawn(fazer com que o processo seja em um nó escolhido pelo(pela?) Julia)
O problema até então é que não existe uma abordagem bem implementada para iterrar no .csv em Julia, daí a parte "leia a linha i do .csv " do loop tá sem código...ainda
for i=1:num_linhas_do_csv
  Referencias_remotas[i] = @spawn leia a linha i do .csv


Depois de a leitura feita e todos os blocos salvos

=#



cd("D:\\Ufal\\Ufal -Não sincronizado-\\Pesquisa\\Gitlab\\DBN\\Julia")
pwd()
data = @spawn readcsv("tiny_csv.csv")
data
A=readdlm("tiny_csv.csv", ';', skipstart=0, header=true)
A[1]


