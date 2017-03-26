import psycopg2
import sys
import os
import time
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


def bucketize(conn, relation, flag = 0):
    cur = conn.cursor()
    new_name = relation + "_ori"
    if flag == 0:
        print "bucketize by hour"
        # """SELECT src, dest, substring(mins from 1 for 13) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 13);"""
        cur.execute("""
                    CREATE TABLE %s AS (
                    SELECT src, dest, substring(mins from 1 for 13) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 13));
                    """ % new_name)
    else:
        print "bucketize by day"
        # """SELECT src, dest, substring(mins from 1 for 10) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 10);"""
        cur.execute("""
                    CREATE TABLE %s AS (
                    SELECT src, dest, substring(mins from 1 for 10) as bucket, COUNT(*) as cnt FROM darpa GROUP BY src, dest, substring(mins from 1 for 10));
                    """ % new_name)
    conn.commit()
    cur.close()
    return new_name


def dcube(conn, relation, k, measure):
    cur = conn.cursor()
    ori_table = bucketize(conn, relation, BUCKET_FLAG)
    conn.commit()
    cur.close()

conn = init_database()
a = raw_input("press to continue...\n")
table_fresh_create_from_file(conn, "darpa", "src text, dest text, mins text", "darpa.csv", False)
# dcube(conn, "darpa", 1, None)
print tuple_counts(conn, "darpa")
drop_table(conn, "darpa")
database_clearup()
