# Planejamento

## Obervações


* Legenda
	* **Pessoa DATA** = Quem deve fazer e o prazo
* A pesquisa deverá ser reproduzível:
	* documentar códigos (comentários didáticos e em inglês)
		* código-fonte da aplicação
		* scripts de experimentação
		* scripts de configuração do ambiente de execução (Docker images, etc.)
	* o código será disponibilizado no github

## Cronograma

* André, Naelson: definir o ambiente de execução distribuída.
	* Docker no cluster (sem OpenStack)
	* Obs.: se André não conseguir colocar Julia no cluster, usaremos a OpenStack
* **André 2015-07-30**: preparar o ambiente de execução
* **Elias, Naelson 2015-08-10**
	* Revisão de literatura
		* Artigo-chave: [Scalable Strategies for Computing with Massive Data](http://www.jstatsoft.org/v55/i14/paper)
		* Selecionar artigos da [Web of Science](http://wokinfo.com) e [ACM](http://dl.acm.org)
		* Fazer resumo de todo artigo lido em LaTex: `julia-R-performance.tex`
			* Padronizar as referências bibtex
	* Definição do objetivo do artigo baseado na revisão de literatura (pode ser outro objetivo específico, mas comecem um desses dois para fazer a busca por artigos):
		* comparação de desempenho entre o pacote de R `foreach`, Rhipe, e Julia `pmap`
		* comparação de desempenho entre implementações MapReduce para computação científica/técnica (sintaxe numérica como R e Julia): Rhipe vs Julia (suporte nativo: pmap + parallel_reduce). 
* **Naelson, Elias 2015-08-20**: implementação
	* aplicação utilizando redes bayesianas (como estudo de caso, benchmark)
	* aplicação real, estudo de caso SUS ou outro do sanduíche do Elias
* **Todos 2015-08-27**: definir os cenários de avaliação
* **Elias, Naelson 2015-09-07**: implementar scripts de avaliação
* **Elias, Naelson 2015-09-14**: executar experimentos
* **Todos 2015-10-14**: redação do artigo


# TODO list (coding)

* translate the code comments to English
* add comments to #driver at the PreProc-SIHD-SUS-TBI-MapReduce.R

# Overview

This code is a pre-processing tool for removing double data from SUS skull fracture data sets.

* input
	* CSV
	* SUS data set with "trumatismo craniano"
	* 
* ouput
	* the same data without double data

## R Version (/src/R)


There are two main files in this project:
PreProc-SIHD-SUS-TBI.R is the serial version of the code

PreProc-SIHD-SUS-TBI-MapReduce.R is the parallel/Rhipe version of the same code

The other .R files were created only for annotations and tests.



In order to run the serial version of the code it's only needed to write
on the system terminal:
>$Rscript(PreProc-SIHD-SUS-TBI.R)  

or on R terminal:
>Source("PreProc-SIHD-SUS-TBI.R")

To run the parallel version all it takes is:

on the system terminal

>$Rscript(PreProc-SIHD-SUS-TBI-MapReduce.R) 

or on the system terminal or

>Source("PPreProc-SIHD-SUS-TBI-MapReduce.R") 


After that a file with the var::output name will be stored on the HDFS(not locally), to list these files you can use on R you cand type
>rhls("/[directory to be listed]")

-In order to get this file you need to type rhread(var::output) on the R console, it will return a data.frame which can be stored on a variable.

Alternatively you can download the raw file via Hadoop using the console this way:
>$hadoop dfs -get [file path on the HDFS] [dest file]



For quick tests purposes the code is set to run over the small.csv database, which contains the three last lines equal. When you run the code over it, two of these lines will be removed and the data.frame without them will be writen to the HDFS.
To change the data input, you only need to set the new input name on var::data_input.