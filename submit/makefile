demo:
	python dcube.py 3 sample.csv 1 ari density

test:
	python test.py

clean:
	rm -f logfile
	rm -f *.log
	rm -f *.pdf

paper:
	cd report && make && cp paper.pdf ../

all.tar:
	tar cvf all.tar *.py *.csv makefile
