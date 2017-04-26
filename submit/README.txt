D-Cube
============
usage: python dcube.py <number_of_blocks> <density_measure> <policy>

D-Cube Using SQL

STEPS TO RUN
============
1. Follow the steps in http://www.cs.cmu.edu/~christos/courses/826.S17/getting-started-with-postgresql.html till you
successfully create a db with "createdb $USER". Please remember to set the port in dcube_params.py and use the same
port when you do "export PGPORT=XXXXX".

2. Make sure python 2.7 is installed in the system. This software also requires psycopg2 library to run.

3. Before you start the software, make sure you download the darpa.csv from
http://www.cs.cmu.edu/~christos/courses/826-resources/DATA-SETS-graphs/datasets/

4. Type "make demo" to see a default run with arithmetic density measure and select-by-density policy.

IMPORTANT
=========
1. Now the results of D-Cube is stored in the database after execution as table "B_ori_i" for i-th block.
2. Now the software only works with darpa.csv.
3. The database is not stopped after execution, please remember to stop it according to the commands provided at
http://www.cs.cmu.edu/~christos/courses/826.S17/getting-started-with-postgresql.html.
4. The data of the detected blocks will be printed out during the execution. If you would like to check the contents of
detected blocks, do not stop the database and type "psql -d $USER" and then type "SELECT * FROM B_ori_i" where i is the
index of the block that you would like to check.