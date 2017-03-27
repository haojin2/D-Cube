import psycopg2
import sys
import os
import time
import numpy
import math
from dcube_params import *

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
        cur.execute("SELECT COUNT(*) FROM %s" % (name))
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
        try:
            cur.execute("DROP TABLE %s;" % name)
        except psycopg2.Error:
            conn.commit()
            pass
    try:
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


def bucketize(conn, relation, size = BUCKET_FLAG, binary = BINARY_FLAG):
    cur = conn.cursor()
    new_name = relation + "_ori"
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
        cur.execute("SELECT SUM(cnt) FROM %s" % (block_tb))
    except psycopg2.Error:
        print "Error when getting count from %s" % block_tb
    data = cur.fetchone()
    return float(data[0])


def dcube(conn, relation, k, measure):
    cur = conn.cursor()
    ori_table = bucketize(conn, relation, BUCKET_FLAG)
    copy_table(conn, ori_table, "darpa")
    table_fresh_create_from_query(conn, "R_src", """SELECT DISTINCT(src) FROM darpa""")
    table_fresh_create_from_query(conn, "R_dest", """SELECT DISTINCT(dest) FROM darpa""")
    table_fresh_create_from_query(conn, "R_bucket", """SELECT DISTINCT(bucket) FROM darpa""")
    print tuple_counts(conn, "R_src")
    print tuple_counts(conn, "R_dest")
    print tuple_counts(conn, "R_bucket")
    for i in range(k):
        M_R = get_mass(conn, ori_table)
        table_fresh_create(conn, "B_src", "src text")
        table_fresh_create(conn, "B_dest", "dest text")
        table_fresh_create(conn, "B_bucket", "bucket text")
        table_fresh_create_from_query(conn, "temp", """SELECT * FROM darpa
                                                       WHERE src NOT IN (SELECT src FROM B_src)
                                                       OR dest NOT IN (SELECT dest FROM B_dest)
                                                       OR bucket NOT IN (SELECT bucket FROM B_bucket)""")
        copy_table(conn, "temp", "darpa")
        print get_mass(conn, "darpa")
        drop_table(conn, "B_src")
        drop_table(conn, "B_dest")
        drop_table(conn, "B_bucket")
        drop_table(conn, "temp")
    drop_table(conn, "R_bucket")
    drop_table(conn, "R_dest")
    drop_table(conn, "R_src")
    drop_table(conn, ori_table)
    conn.commit()
    cur.close()

conn = init_database()
a = raw_input("press to continue...\n")
table_fresh_create_from_file(conn, "darpa", "src text, dest text, mins text", "darpa.csv", False)
dcube(conn, "darpa", 1, None)
drop_table(conn, "darpa")
database_clearup()


## dimension select algorithms ##


def rho_ari(conn, block_tb):
    m = get_mass(conn, block_tb)
    temp = 0
    for col in columns:
        temp += tuple_counts_distinct(conn, block_tb, col)

    return 3. * float(m) / float(temp)
        
def rho_geo(conn, block_tb):
    m = get_mass(conn, block_tb)
    temp = 1
    for col in columns:
        temp *= tuple_counts_distinct(conn, block_tb, col)

    return float(m) / float(temp)**(1./3.)
        
def rho_susp(conn, block_tb, rel_tb):
    mb = get_mass(conn, block_tb)
    mr = get_mass(conn, rel_tb)
    temp = (numpy.log(mb/mr) - 1) * mb
    temp1 = 1
    for col in columns:
        temp1 *= tuple_counts_distinct(conn, block_tb, col)/tuple_counts_distinct(conn, rel_tb, col)
    
    temp += mr * temp1
    temp -= mb * numpy.log(temp1)


# def select_dimension_by_density(conn, block_tb, rel_tb, density_measure):
#     mb = get_mass(conn, block_tb)
#     mr = get_mass(conn, rel_tb)
#     ret = ''
#     max_rho = -float("inf")
#     for col in columns:
#         if mb == 0:
#             continue
#
#         filter_block(conn, block_tb, rel_tb,
#
#         if rho > max_rho:
#             max_rho = rho
#             ret = col
#
#     return ret

