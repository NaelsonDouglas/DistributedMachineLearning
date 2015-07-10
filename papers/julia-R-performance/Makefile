# READ-ME: `make figures` only works with -B parameters: `make -B figures`
#              Apply the -B to `make all` too: 'make -B all'

filename=julia-R-performance

with-pdf : pdf clean-but-pdf

pdf : latex
	dvipdf $(filename).dvi

latex :
	latex $(filename).tex
	bibtex $(filename)
	latex $(filename).tex
	latex $(filename).tex

#pdflatex :
#	pdflatex $(filename).tex

figures :
	inkscape arch.svg -T -E arch.eps

figures-png :
	inkscape arch.svg -d 200 -e arch.png

clean-but-pdf :
	rm -f ${filename}.{ps,log,aux,out,dvi,bbl,blg,lof,gls,ilg,ist,glo,toc,synctex.gz} *~ *.log

clean-pdf :
	rm -f ${filename}.pdf

clean-all : clean-but-pdf clean-pdf
