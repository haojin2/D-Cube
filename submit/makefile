demo:
	python dcube.py 3 sample.csv 1 ari density

test:
	python test.py

clean:
	rm -f logfile
	rm -f *.log
	rm -f *.pdf
	rm -f B_*
	rm -f *.pyc
	cd report && rm -f *.pdf

paper:
	cd report && make && mv paper.pdf ../

all.tar:
	tar cvf all.tar ./
