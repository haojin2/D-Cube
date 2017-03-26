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


def bucketize(conn, relation, cols, flag = 0):
    cur = conn.cursor()
    new_name = relation + "_ori"
    if flag == 0:
        print "bucketize by hour"
        # """SELECT src, dest, substring(mins from 1 for 13) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 13);"""
    else:
        print "bucketize by day"
    cur.close()
    return new_name


def dcube(conn, relation, k, measure):
    cur = conn.cursor()
    ori_table = bucketize(conn, relation, [], BUCKET_FLAG)
    conn.commit()
    cur.close()

conn = init_database()
a = raw_input("press to continue...\n")
table_fresh_create_from_file(conn, "darpa", "src text, dest text, mins text", "darpa.csv", False)
# dcube(conn, "darpa", 1, None)
print tuple_counts(conn, "darpa")
#drop_table(conn, "darpa")
database_clearup()


## dimension select algorithms ##

def get_mass(conn, block_tb):
    cur = conn.cursor()
    try:
        cur.execute("SELECT SUM(cnt) FROM %s" % (block_tb))
    except psycopg2.Error:
        print "Error when getting count from %s" % block_tb
    data = cur.fetchone()
    return float(data[0])
    

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


def select_dimension_by_density(conn, block_tb, rel_tb, density_measure):
    mb = get_mass(conn, block_tb)
    mr = get_mass(conn, rel_tb)
    ret = ''
    max_rho = -float("inf")
    for col in columns:
        if mb == 0:
            continue
        
        filter_block(conn, block_tb, rel_tb, 

        if rho > max_rho:
            max_rho = rho
            ret = col

    return ret

