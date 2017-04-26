D-Cube
============
usage: python dcube.py <dimension> <input file> <number_of_blocks> <density_measure> <policy>

D-Cube Using SQL

STEPS TO RUN
============
1. Follow the steps in http://www.cs.cmu.edu/~christos/courses/826.S17/getting-started-with-postgresql.html till you
successfully create a db with "createdb $USER". Please remember to set the port in dcube_params.py and use the same
port when you do "export PGPORT=XXXXX".

2. Make sure python 2.7 is installed in the system. This software also requires psycopg2 library to run.

3. Before you start the software, make sure you download the dataset you want to run from
http://www.cs.cmu.edu/~christos/courses/826-resources/DATA-SETS-graphs/datasets/

4. For datasets with useless extra dimension, we've provided a script named "reduce.py" for you to remove the extra
column. Type "python reduce.py <target dimension> <input file> <output file>" to use it.

5. Type "make demo" to see a default run with arithmetic density measure and select-by-density policy.

IMPORTANT
=========
1. Now the results of D-Cube are exported to csv files, for i-th block the block tuples are stored in "B_ori_i.csv",
the attributes on k-th dimension are stored in "B_col_k_i.csv".
2. Now the software works with all provided unlabelled datasets.
3. The database is stopped after execution, but please still remember to stop it according to the commands provided at
http://www.cs.cmu.edu/~christos/courses/826.S17/getting-started-with-postgresql.html.
4. The data of the detected blocks will be printed out during the execution. If you would like to check the contents of
detected blocks, you can either check the exported csv files or restart the database after execution and type \
"psql -d $USER" and then type "SELECT * FROM B_ori_i" where i is the index of the block that you would like to check.