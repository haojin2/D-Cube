import psycopg2
import sys
import os
import time
import numpy
import math
from dcube_params import *

active_tables = {}

R_n = {"src": "R_src", "dest": "R_dest", "bucket": "R_bucket"}

def init_database():
    os.system("pg_ctl -D $HOME/826prj/ -o '-k /tmp' start")
    time.sleep(1)
    username = os.environ['USER']
    conn = psycopg2.connect(dbname=username, user=username, password="", port=PGPORT)
    return conn


def database_clearup():
    conn.close()
    os.system("pg_ctl -D $HOME/826prj stop")
    time.sleep(1)


def tuple_counts(conn, name):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM %s" % name)
    except psycopg2.Error:
        print "Error when getting count from %s" % name
    data = cur.fetchone()
    return data[0]


def tuple_counts_distinct(conn, name, col):
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT (DISTINCT %s) FROM %s" % (col, name))
    except psycopg2.Error:
        print "Error when getting count from %s" % name
    data = cur.fetchone()
    return data[0]


def table_fresh_create(conn, name, columns, flag = True):
    cur = conn.cursor()
    if flag:
        drop_table(conn, name)
    try:
        # CREATE TABLE table_name (column1 datatype, column2 datatype, column3 datatype);
        cur.execute("CREATE TABLE %s (%s);" % (name, columns))
    except psycopg2.Error:
        print "Error when Create %s" % name
    conn.commit()
    cur.close()


def table_fresh_create_from_query(conn, name, query, drop = True):
    cur = conn.cursor()
    if drop:
        drop_table(conn, name)
    cur.execute("CREATE TABLE %s AS (%s);" % (name, query))
    conn.commit()
    cur.close()


def table_fresh_create_from_file(conn, name, columns, filename, flag = True):
    cur = conn.cursor()
    filename = os.path.abspath("%s" % filename)
    table_fresh_create(conn, name, columns, flag)
    try:
        cur.execute("COPY %s FROM '%s' DELIMITER ',' CSV;" % (name, filename))
    except psycopg2.Error:
        print "Error when COPY %s FROM %s" % (name, filename)

    conn.commit()
    cur.close()


def copy_table(conn, src, cpy, drop = True):
    cur = conn.cursor()
    if drop:
        try:
            cur.execute("DROP TABLE %s;" % cpy)
        except psycopg2.Error:
            conn.commit()
            pass
    try:
        cur.execute("CREATE TABLE %s AS TABLE %s;" % (cpy, src))
    except psycopg2.Error:
        print "Error when copying %s to %s" % (src, cpy)
    conn.commit()
    cur.close()


def drop_table(conn, tb):
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE %s;" % tb)
    except psycopg2.Error:
        print "Error when dropping %s" % tb
    conn.commit()
    cur.close()


def get_distinct_val(conn, new_tb, tb, col):
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE %s AS SELECT DISTINCT %s FROM %s;" % (new_tb, col, tb))
    except psycopg2.Error:
        print "Error in get_distinct_val on table %s col %s" % (tb, col)
    conn.commit()
    cur.close()


def bucketize(conn, relation, size=BUCKET_FLAG, binary=BINARY_FLAG):
    cur = conn.cursor()
    new_name = relation + "_ori"
    try:
        cur.execute("DROP TABLE %s;" % new_name)
    except psycopg2.Error:
        print "Error in get_distinct_val on table %s col %s" % (tb, col)
    if size == 0:
        print "bucketize by hour"
        if binary == 0:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 13) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 13));
                        """ % new_name)
        else:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 13) as bucket, 1 as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 13));
                        """ % new_name)
    else:
        print "bucketize by day"
        if binary == 0:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 10) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 10));
                        """ % new_name)
        else:
            cur.execute("""
                        CREATE TABLE %s AS (
                        SELECT src, dest, substring(mins from 1 for 10) as bucket, 1 as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 10));
                        """ % new_name)
    conn.commit()
    cur.close()
    return new_name


def get_mass(conn, block_tb):
    cur = conn.cursor()
    try:
        cur.execute("SELECT SUM(cnt) FROM %s" % block_tb)
    except psycopg2.Error:
        print "Error when getting count from %s" % block_tb
    data = cur.fetchone()
    return float(data[0])


## dimension select algorithms ##


def rho_ari(conn, mb, block_attrs, mr, rel_attrs):
    temp = 0
    for col in columns:
        block_tb = block_attrs[col]
        temp += tuple_counts_distinct(conn, block_tb, col)

    # return 1
    return 3. * float(mb) / float(temp)


def rho_geo(conn, mb, block_attrs, mr, rel_attrs):
    temp = 1
    for col in columns:
        block_tb = block_attrs[col]
        temp *= tuple_counts_distinct(conn, block_tb, col)

    return float(mb) / float(temp) ** (1. / 3.)


def rho_susp(conn, mb, block_attrs, mr, rel_attrs):
    temp = (numpy.log(mb / mr) - 1) * mb
    temp1 = 1.
    for col in columns:
        block_tb = block_attrs[col]
        rel_tb = rel_attrs[col]
        temp1 *= float(tuple_counts_distinct(conn, block_tb, col)) / float(tuple_counts_distinct(conn, rel_tb, col))

    temp += mr * temp1
    temp -= mb * numpy.log(temp1)
    return temp


def filter_block(conn, tb, mass_thr):
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM %s WHERE cnt <= %f;" % (tb, mass_thr))
    except psycopg2.Error:
        print "Error when filtering block %s with mass threshold %d" % (tb, mass_thr)
    conn.commit()
    cur.close()


def select_dimension_by_density(conn, block_attrs, rel_attrs, mass_attrs, mb, mr, density_measure):
    ret = ''
    max_rho = -float("inf")
    for col in columns:
        if mb == 0:
            continue

        block_tb = block_attrs[col]
        bi = tuple_counts_distinct(conn, block_tb, col)
        block_attr_tb = mass_attrs[col]
        print block_attr_tb
        mass_thr = float(mb) / float(bi)

        temp_block_attr_tb = "temptable"
        copy_table(conn, block_attr_tb, temp_block_attr_tb, drop=True)

        temp_block_attrs = {}

        # filter block
        filter_block(conn, temp_block_attr_tb, mass_thr)

        temp_block_attrs[col] = temp_block_attr_tb
        temp_mass = get_mass(conn, temp_block_attr_tb)

        rho = density_measure(conn, temp_mass, block_attrs, mr, rel_attrs)

        if rho > max_rho:
            max_rho = rho
            ret = col

        drop_table(conn, temp_block_attr_tb)

    return ret


def select_dimension_by_cardinality(conn, block_attrs, rel_attrs, mass_attrs, mb, mr, density_measure):
    ret = ''
    max_card = float("inf")
    for col in columns:
        block_tb = block_attrs[col]
        card = tuple_counts_distinct(conn, block_tb, col)
        if card > max_card:
            ret = col
            max_card = card

    return ret


def check_dimensions(conn):
    len_src = tuple_counts(conn, "B_src")
    len_dest = tuple_counts(conn, "B_dest")
    len_bucket = tuple_counts(conn, "B_bucket")
    return (len_src != 0) or (len_dest != 0) or (len_bucket != 0)


def find_single_block(conn, R, M_R, measure=rho_ari, select_dimension=select_dimension_by_cardinality):
    cur = conn.cursor()
    copy_table(conn, R, "B")
    M_B = M_R
    for col in columns:
        copy_table(conn, R_n[col], "B_%s" % col)
        table_fresh_create(conn, "order_%s" % col, "%s text, ord int" % col)

    rho_wave = measure(conn, M_B, {"src": "B_src", "dest": "B_dest", "bucket": "B_bucket"}, M_R, R_n)
    r = 1
    r_wave = 1
    while check_dimensions(conn):
        for col in columns:
            # CREATE TABLE t_c AS (SELECT t_b.id, CASE WHEN SUM(t_a.val) is NULL THEN 0 ELSE SUM(t_a.val) END AS cnt FROM t_a RIGHT JOIN t_b ON t_a.id = t_b.id GROUP BY t_b.id);
            print "B_%s count:" % col, tuple_counts(conn, "B_%s" % col)
            table_fresh_create_from_query(conn, "M_B_%s" % col,
                                          """SELECT B_%s.%s, CASE WHEN SUM(B.cnt) is NULL THEN 0 ELSE SUM(B.cnt) END AS cnt
                                              FROM B RIGHT JOIN B_%s ON B.%s = B_%s.%s
                                              GROUP BY B_%s.%s ORDER BY cnt ASC""" % (col, col, col, col, col, col, col, col))
            print "M_B_%s count:" % col, tuple_counts(conn, "M_B_%s" % col)

        col_name = select_dimension(conn, {"src": "B_src", "dest": "B_dest", "bucket": "B_bucket"},
                             R_n, {"src": "M_B_src", "dest": "M_B_dest", "bucket": "M_B_bucket"},
                             M_B, M_R, measure)

        table_fresh_create_from_query(conn, "D_%s" % col_name,
                                      "SELECT * FROM M_B_%s WHERE cnt <= %f ORDER BY cnt ASC" %
                                       (col_name, M_B * 1. / tuple_counts(conn, "B_%s" % col_name)))
        len_D = tuple_counts(conn, "D_%s" % col_name)
        for j in range(len_D):
            table_fresh_create_from_query(conn, "B_%s_temp" % col_name,
                                          """SELECT %s FROM M_B_%s
                                             OFFSET %d"""
                                          % (col_name, col_name, j+1))
            cur.execute("""SELECT * FROM D_%s LIMIT 1 OFFSET %d""" % (col_name, j+1))
            attr_name, M_B_a_i, = cur.fetchone()
            M_B = M_B - M_B_a_i
            rho_prime = measure(conn, M_B, {"src": "B_src", "dest": "B_dest", "bucket": "B_bucket"},
                                      M_R, R_n)
            cur.execute("INSERT INTO order_%s VALUES('%s', %d);" % (col_name, attr_name, r))
            r += 1
            if rho_prime > rho_wave:
                rho_wave = rho_prime
                r_wave = r
        table_fresh_create_from_query(conn, "B_temp", """SELECT * FROM B
                                                         WHERE %s NOT IN
                                                         (SELECT %s FROM D_%s)""" % (col_name, col_name, col_name))
        copy_table(conn, "B_temp", "B")
        drop_table(conn, "B_temp")
        drop_table(conn, "D_%s" % col_name)

    for col in columns:
        table_fresh_create_from_query(conn, "B_%s", """SELECT %s
                                                       FROM order_%s
                                                       WHERE ord >= %d""" % (col, col, r_wave))
        drop_table(conn, "order_%s" % col)
    drop_table(conn, "B")
    conn.commit()
    cur.close()


def dcube(conn, relation, k, measure, select_dimension):
    cur = conn.cursor()
    ori_table = bucketize(conn, relation)
    copy_table(conn, ori_table, "darpa")
    table_fresh_create_from_query(conn, "R_src", """SELECT DISTINCT(src) FROM darpa""")
    table_fresh_create_from_query(conn, "R_dest", """SELECT DISTINCT(dest) FROM darpa""")
    table_fresh_create_from_query(conn, "R_bucket", """SELECT DISTINCT(bucket) FROM darpa""")
    print tuple_counts(conn, "R_src")
    print tuple_counts(conn, "R_dest")
    print tuple_counts(conn, "R_bucket")
    results = []
    for i in range(k):
        M_R = get_mass(conn, "darpa")
        find_single_block(conn, "darpa", M_R, measure, select_dimension)
        table_fresh_create_from_query(conn, "temp", """SELECT * FROM darpa
                                                       WHERE src NOT IN (SELECT src FROM B_src)
                                                       OR dest NOT IN (SELECT dest FROM B_dest)
                                                       OR bucket NOT IN (SELECT bucket FROM B_bucket)""")
        copy_table(conn, "temp", "darpa")
        print get_mass(conn, "darpa")
        table_fresh_create_from_query(conn, "B_ori_%d" % i,
                                      """SELECT * FROM %s
                                         WHERE src IN (SELECT src FROM B_src)
                                         OR dest IN (SELECT dest FROM B_dest)
                                         OR bucket IN (SELECT bucket FROM B_bucket)""" % ori_table)
        results.append("B_ori_%d" % i)
        drop_table(conn, "temp")
    drop_table(conn, "R_bucket")
    drop_table(conn, "R_dest")
    drop_table(conn, "R_src")
    drop_table(conn, ori_table)
    conn.commit()
    cur.close()
    return results


if __name__ == '__main__':
    conn = init_database()
    a = raw_input("press to continue...\n")
    table_fresh_create_from_file(conn, "darpa", "src text, dest text, mins text", "darpa.csv", True)
    results = dcube(conn, "darpa", 1, rho_ari, select_dimension_by_density)
    drop_table(conn, "darpa")
    database_clearup()

