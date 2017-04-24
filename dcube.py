import psycopg2
import sys
import os
import time
import numpy
from dcube_params import *

active_tables = {}

global dimension
dimension = 0

# Data Structure for caching R_n sizes
global R_n
R_n = {"src": 0, "dest": 0, "bucket": 0}

# Helper function for setting up the database
def init_database():
    os.system("pg_ctl -D $HOME/826prj/ -o '-k /tmp' -l logfile start")
    time.sleep(1)
    username = os.environ['USER']
    conn = psycopg2.connect(dbname=username, user=username, password="", port=PGPORT)
    return conn


# Helper function for closing the database
def database_clearup():
    conn.close()
    os.system("pg_ctl -D $HOME/826prj stop")
    time.sleep(1)


# Helper function for counting total number of tuples in a table
def tuple_counts(conn, name):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM %s" % name)
    except psycopg2.Error:
        #print "Error when getting count from %s" % name
        pass
    data = cur.fetchone()
    return data[0]


# Helper function for counting number of distinct values in a column of a table
def tuple_counts_distinct(conn, name, col):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT (DISTINCT %s) FROM %s" % (col, name))
    except psycopg2.Error:
        #print "Error when getting count from %s" % name
        pass
    data = cur.fetchone()
    return data[0]


# Helper function for create a fresh new table
def table_fresh_create(conn, name, columns, flag = True):
    cur = conn.cursor()
    if flag:
        drop_table(conn, name)
    try:
        # CREATE TABLE table_name (column1 datatype, column2 datatype, column3 datatype);
        cur.execute("CREATE TABLE %s (%s);" % (name, columns))
    except psycopg2.Error:
        #print "Error when Create %s" % name
        pass
    conn.commit()
    cur.close()


# Helper function for create a table from a query
def table_fresh_create_from_query(conn, name, query, drop = True):
    cur = conn.cursor()
    if drop:
        drop_table(conn, name)
    cur.execute("CREATE TABLE %s AS (%s);" % (name, query))
    conn.commit()
    cur.close()


# Helper function for loading data into a new table
def table_fresh_create_from_file(conn, name, columns, filename, flag = True):
    cur = conn.cursor()
    filename = os.path.abspath("%s" % filename)
    table_fresh_create(conn, name, columns, flag)
    try:
        cur.execute("COPY %s FROM '%s' DELIMITER ',' CSV;" % (name, filename))
    except psycopg2.Error:
        #print "Error when COPY %s FROM %s" % (name, filename)
        pass

    conn.commit()
    cur.close()


# Helper function for copying a table
def copy_table(conn, src, cpy, drop = True):
    cur = conn.cursor()
    if drop:
        drop_table(conn, cpy)
    try:
        cur.execute("CREATE TABLE %s AS TABLE %s;" % (cpy, src))
    except psycopg2.Error:
        #print "Error when copying %s to %s" % (src, cpy)
        pass
    conn.commit()
    cur.close()


# Helper function for dropping a table
def drop_table(conn, tb):
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE %s;" % tb)
    except psycopg2.Error:
        #print "Error when dropping %s" % tb
        pass
    conn.commit()
    cur.close()


def drop_index(conn, tb):
    cur = conn.cursor()
    try:
        cur.execute("DROP INDEX %s_idx;" % tb)
    except psycopg2.Error:
        pass
    conn.commit()
    cur.close()


def index_fresh_create(conn, tb, columns):
    cur = conn.cursor()
    drop_index(conn, tb)
    try:
        cur.execute("CREATE INDEX %s_idx ON %s(%s);" % (tb, tb, columns))
    except:
        print "error when creating index on %s(%s)" % (tb, columns)
    conn.commit()
    cur.close()


def get_distinct_val(conn, new_tb, tb, col):
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE %s AS SELECT DISTINCT %s FROM %s;" % (new_tb, col, tb))
    except psycopg2.Error:
        #print "Error in get_distinct_val on table %s col %s" % (tb, col)
        pass
    conn.commit()
    cur.close()


# Helper function to compute the bucketized dataset
def bucketize(conn, relation, size=BUCKET_FLAG, binary=BINARY_FLAG):
    cur = conn.cursor()
    new_name = relation + "_ori"
    drop_table(conn, new_name)
    if size == 0:
        # if binary == 0:
        #     cur.execute("""
        #                 CREATE TABLE %s AS (
        #                 SELECT src, dest, mins as bucket, COUNT(*) as cnt, 1 as flag
        #                 FROM darpa GROUP BY src, dest, mins);
        #                 """ % new_name)
        # else:
        #     cur.execute("""
        #                 CREATE TABLE %s AS (
        #                 SELECT src, dest, mins as bucket, 1 as cnt, 1 as flag
        #                 FROM darpa GROUP BY src, dest, mins);
        #                 """ % new_name)
        if binary == 0:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT %s, COUNT(*) as cnt, 1 as flag
                        FROM %s GROUP BY %s);
                        """ % (new_name, ', '.join(columns), relation, ', '.join(columns)))
        else:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT %s, 1 as cnt, 1 as flag
                        FROM %s GROUP BY %s);
                        """ % (new_name, ', '.join(columns), relation, ', '.join(columns)))
    elif size == 1:
        #print "bucketize by hour"
        if binary == 0:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 13) as bucket, COUNT(*) as cnt, 1 as flag
                        FROM darpa GROUP BY src, dest, substring(mins from 1 for 13));
                        """ % new_name)
        else:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 13) as bucket, 1 as cnt, 1 as flag
                        FROM darpa GROUP BY src, dest, substring(mins from 1 for 13));
                        """ % new_name)
    else:
        #print "bucketize by day"
        if binary == 0:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 10) as bucket, COUNT(*) as cnt, 1 as flag
                        FROM darpa GROUP BY src, dest, substring(mins from 1 for 10));
                        """ % new_name)
        else:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 10) as bucket, 1 as cnt, 1 as flag
                        FROM darpa GROUP BY src, dest, substring(mins from 1 for 10));
                        """ % new_name)
    conn.commit()
    cur.close()
    return new_name


def get_mass(conn, block_tb):
    cur = conn.cursor()
    try:
        cur.execute("SELECT SUM(cnt) FROM %s" % block_tb)
    except psycopg2.Error:
        #print "Error when getting count from %s" % block_tb
        pass
    data = cur.fetchone()
    if data[0] is None:
        return 0.
    return float(data[0])


def get_mass_with_flag(conn, block_tb):
    cur = conn.cursor()
    try:
        cur.execute("SELECT SUM(cnt) FROM %s WHERE flag = 1" % block_tb)
    except psycopg2.Error:
        #print "Error when getting count from %s" % block_tb
        pass
    data = cur.fetchone()
    if data[0] is None:
        return 0.
    return float(data[0])


## Density Measurement Functions
def rho_ari(conn, mb, block_attrs, mr, rel_attrs):
    # The density based on arithmatic mean
    temp = 0
    for col in columns:
        block_tb = block_attrs[col]
        temp += block_tb

    if temp == 0:
        return -1.
    return 3. * float(mb) / float(temp)


def rho_geo(conn, mb, block_attrs, mr, rel_attrs):
    # The density based on geometric mean
    temp = 1
    for col in columns:
        block_tb = block_attrs[col]
        temp *= block_tb
    if temp == 0:
        return -1.
    return float(mb) / float(temp) ** (1. / 3.)


def rho_susp(conn, mb, block_attrs, mr, rel_attrs):
    # The density based on geometric mean
    if (abs(mr - 0) < 1e-8):
        return -1
    if (abs(mb - 0) < 1e-8):
        return -1
    temp = (numpy.log(mb / mr) - 1) * mb
    temp1 = 1.
    for col in columns:
        block_tb = block_attrs[col]
        rel_tb = rel_attrs[col]
        temp1 *= float(block_tb) / float(rel_tb)

    temp += mr * temp1
    if temp1 == 0:
        return -1.
    temp -= mb * numpy.log(temp1)
    return temp


def filter_block(conn, tb, mass_thr):
    # Move out all attributes with lower desntity
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM %s WHERE cnt <= %f;" % (tb, mass_thr))
    except psycopg2.Error:
        #print "Error when filtering block %s with mass threshold %d" % (tb, mass_thr)
        pass
    conn.commit()
    cur.close()


def select_dimension_by_density(conn, block_attrs, rel_attrs, mass_attrs, mb, mr, density_measure):
    # Select the dimension with the largest density after remove some of its attributes
    ret = ''
    max_rho = -float("inf")
    for col in columns:
        block_tb = block_attrs[col]
        bi = tuple_counts_distinct(conn, block_tb, col)
        if bi == 0:
            continue
        block_attr_tb = mass_attrs[col]

        # get the threshold (average mass)
        mass_thr = float(mb) / float(bi)

        temp_block_attr_tb = "temptable"
        copy_table(conn, block_attr_tb, temp_block_attr_tb, drop=True)
        temp_block_attrs = dict(mass_attrs)

        # move out attributes with low density
        filter_block(conn, temp_block_attr_tb, mass_thr)

        temp_block_attrs[col] = temp_block_attr_tb
        temp_mass = get_mass(conn, temp_block_attr_tb)
        temp_block_attrs_size = {}

        # get the new B_n list
        for col1 in columns:
            temp_block_attrs_size[col1] = tuple_counts_distinct(conn, temp_block_attrs[col1], col1)

        # calculate the density
        rho = density_measure(conn, temp_mass, temp_block_attrs_size, mr, rel_attrs)

        # record the maximum of row and the corresponding dimension
        if rho >= max_rho:
            max_rho = rho
            ret = col

        drop_table(conn, temp_block_attr_tb)

    return ret


def select_dimension_by_cardinality(conn, block_attrs, rel_attrs, mass_attrs, mb, mr, density_measure):
    # Select the dimension with the largest cardinality (the largest number of remaining attributes)
    ret = ''
    max_card = -float("inf")
    for col in columns:
        block_tb = block_attrs[col]
        card = tuple_counts_distinct(conn, block_tb, col)
        if card >= max_card:
            ret = col
            max_card = card

    return ret


# Helper function to check if all of B_n is empty
def check_dimensions(conn):
    for col in columns:
        if tuple_counts(conn, "B_%s" % col) != 0:
            return True
    return False


# The implementation for find_single_block
def find_single_block(conn, R, M_R, measure=rho_ari, select_dimension=select_dimension_by_cardinality):
    cur = conn.cursor()
    # B <- R
    # copy_table(conn, R, "B")

    t0 = time.time()
    table_fresh_create_from_query(conn, "B",
                                  """SELECT %s, cnt FROM %s
                                     WHERE flag = 1""" % (', '.join(columns), R))
    t1 = time.time()
    print "B<-R used: ", t1 - t0
    # M_B <- M_R
    M_B = M_R
    # B_n <- R_n
    for col in columns:
        copy_table(conn, "R_%s" % col, "B_%s" % col)
        index_fresh_create(conn, "B_%s" % col, col)
        table_fresh_create(conn, "order_%s" % col, "%s text, ord int" % col)

    # cache B_n count for faster computation
    # B_n = {"src": R_n["src"], "dest": R_n["dest"], "bucket": R_n["bucket"]}
    print R_n
    B_n = {key: R_n[key] for key in R_n.keys()}
    print B_n
    # rho~ = rho(M_B, |B_n|, M_R, |R_n|)
    rho_wave = measure(conn, M_B, B_n, M_R, R_n)
    # r, r~ <- 0
    r = 0
    r_wave = 0

    t0 = time.time()
    while check_dimensions(conn):
        # Compute {M_B(a,i)}
        for col in columns:
            table_fresh_create_from_query(conn, "M_B_%s" % col,
                                          """SELECT B_%s.%s, CASE WHEN SUM(B.cnt) is NULL THEN 0 ELSE SUM(B.cnt) END AS cnt
                                             FROM B RIGHT JOIN B_%s ON B.%s = B_%s.%s
                                             GROUP BY B_%s.%s
                                             ORDER BY cnt ASC""" % (col, col, col, col, col, col, col, col))

        # i <- select_dimension()
        col_name = select_dimension(conn, {key: "B_%s" % key for key in columns},
                             R_n, {key: "M_B_%s" % key for key in columns},
                             M_B, M_R, measure)
        # D_i <- {M_B(a,i) <= M_B / |B_i|}
        table_fresh_create_from_query(conn, "D_%s" % col_name,
                                      "SELECT * FROM M_B_%s WHERE cnt <= %f ORDER BY cnt ASC" %
                                       (col_name, M_B * 1. / tuple_counts(conn, "B_%s" % col_name)))
        cur.execute("CREATE INDEX idx_col_%s ON D_%s USING hash(%s)" % (col_name, col_name, col_name))
        # cur.execute("CREATE INDEX idx_col_%s ON D_%s(%s)" % (col_name, col_name, col_name))
        conn.commit()
        len_D = tuple_counts(conn, "D_%s" % col_name)
        t00 = time.time()
        for j in range(len_D):
            cur.execute("""SELECT * FROM D_%s LIMIT 1 OFFSET %d""" % (col_name, j))
            attr_name, M_B_a_i, = cur.fetchone()
            # B_i <- B_i - {a}, M_B <- M_B - M_B(a,i)
            cur.execute("DELETE FROM B_%s WHERE %s = '%s'" % (col_name, col_name, attr_name))
            B_n[col_name] -= 1
            M_B = M_B - M_B_a_i
            # rho' <- rho(M_B, |B_i|, M_R, |R_i|)
            rho_prime = measure(conn, M_B, B_n, M_R, R_n)
            # order(a,n) <- r, r <- r+1
            cur.execute("INSERT INTO order_%s VALUES('%s', %d);" % (col_name, attr_name, r))
            r += 1

            if rho_prime > rho_wave:
                rho_wave = rho_prime
                r_wave = r
        t10 = time.time()
        print "Traversal of D_i used: ", t10 - t00
        conn.commit()
        # cur.execute("CREATE INDEX idx_B_%s ON B USING hash(%s);" % (col_name, col_name))
        # index_fresh_create(conn, "B", col_name)
        # # get new B
        # t00 = time.time()
        # table_fresh_create_from_query(conn, "B_temp", """SELECT * FROM B
        #                                                  WHERE %s NOT IN
        #                                                  (SELECT %s FROM D_%s)""" % (col_name, col_name, col_name))
        # t10 = time.time()
        # print "Create B_temp used: ", t10 - t00
        print "before delete from B"
        t00 = time.time()
        cur.execute("DELETE FROM B WHERE %s IN (SELECT %s FROM D_%s)" % (col_name, col_name, col_name))
        conn.commit()
        t10 = time.time()
        print "Delete from B used: ", t10 - t00
        # drop_index(conn, "B")
        # t00 = time.time()
        # copy_table(conn, "B_temp", "B")
        # t10 = time.time()
        # print "Copy B_temp to B used: ", t10 - t00
        # drop_table(conn, "B_temp")
        drop_table(conn, "D_%s" % col_name)
    t1 = time.time()
    print "while loop used: ", t1 - t0
    # get B~
    for col in columns:
        table_fresh_create_from_query(conn, "B_%s" % col, """SELECT %s
                                                       FROM order_%s
                                                       WHERE ord >= %d""" % (col, col, r_wave))
        drop_table(conn, "order_%s" % col)
    drop_table(conn, "B")
    conn.commit()
    cur.close()

    return rho_wave


# The implementation for Algo 1 in D-Cube paper
def dcube(conn, relation, k, measure, select_dimension):
    cur = conn.cursor()
    t0 = time.time()
    ori_table = bucketize(conn, relation)
    t1 = time.time()
    print "bucketize used: ", t1-t0
    copy_table(conn, ori_table, relation)

    # index_fresh_create(conn, "darpa", "src, dest, bucket")

    t_start = time.time()
    results = []

    # Create R_n tables and remember |R_n| in memory for faster computation
    for col in columns:
        table_fresh_create_from_query(conn, "R_%s" % col, """SELECT DISTINCT(%s) FROM %s""" % (col, relation))
    for col in columns:
        R_n[col] = tuple_counts(conn, "R_%s" % col)

    for i in range(k):
        M_R = get_mass_with_flag(conn, relation)
        print "M_R:", M_R
        # Blocks are returned in B_src, B_dest, B_bucket tables
        rho = find_single_block(conn, relation, M_R, measure, select_dimension)
        # Get new R by filtering out tuples in B
        # table_fresh_create_from_query(conn, "temp", """SELECT * FROM darpa
        #                                                WHERE src NOT IN (SELECT src FROM B_src)
        #                                                OR dest NOT IN (SELECT dest FROM B_dest)
        #                                                OR bucket NOT IN (SELECT bucket FROM B_bucket)""")
        # copy_table(conn, "temp", "darpa")
        t0 = time.time()
        where_clauses = ["%s IN (SELECT %s FROM B_%s)" % (col, col, col) for col in columns]
        cur.execute("""UPDATE %s SET flag = 0
                       WHERE %s;""" % (relation, ' AND '.join(where_clauses)))
        t1 = time.time()
        print "R<-R-B used: ", t1 - t0
        print "Mass after update:", get_mass_with_flag(conn, relation)

        # the i-th table is stored in B_ori_i and kept in disk when the software finishes
        t0 = time.time()
        table_fresh_create_from_query(conn, "B_ori_%d" % i,
                                      """SELECT * FROM %s
                                         WHERE %s""" % (ori_table, ' AND '.join(where_clauses)))
        results.append("B_ori_%d" % i)
        t1 = time.time()
        print "B_ori table used: ", t1 - t0

        # print results
        print "Block %d:" % (i+1)
        print "Mass: %d" % get_mass(conn, 'B_ori_%d' % i)
        print "Size: %s" % 'x'.join([str(tuple_counts(conn, "B_%s" % col)) for col in columns])
        print "Density: %f" % rho
        print

        conn.commit()
    t_end = time.time()
    print "Total time used: ", t_end - t_start
    for col in columns:
        drop_table(conn, "R_%s" % col)
    drop_table(conn, ori_table)
    conn.commit()
    cur.close()
    return results


if __name__ == '__main__':
    #a = raw_input("press to continue...\n")

    funcdict = {
      'density': select_dimension_by_density, 
      'cardinality': select_dimension_by_cardinality, 
      'ari': rho_ari, 
      'geo': rho_geo, 
      'susp': rho_susp, 
    }

    if len(sys.argv) < 6:
        print "usage: python dcube.py <dimension> <input file> <# of blocks> <measure> <policy>"
    principle = funcdict[sys.argv[5]]
    measurement = funcdict[sys.argv[4]]
    block_num = int(sys.argv[3])
    input_file = sys.argv[2]
    dimension = int(sys.argv[1])
    R_n = {'col_%d' % (i+1): 0 for i in range(dimension)}
    columns = ['col_%d' % (i+1) for i in range(dimension)]
    column_str = ' text, '.join(columns) + ' text'
    print columns
    print column_str
    a = raw_input("...")
    conn = init_database()
    table_fresh_create_from_file(conn, input_file.split('.')[0], column_str, input_file, True)
    results = dcube(conn, input_file.split('.')[0], block_num, measurement, principle)
    drop_table(conn, input_file.split('.')[0])
    database_clearup()

