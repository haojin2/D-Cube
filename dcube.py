import psycopg2
import sys
import os
import time
from dcube_params import *

global conn
conn = None


def init_database():
    username = os.environ['USER']
    conn = psycopg2.connect(dbname=username, user=username, password="", port=PGPORT)
    time.sleep(1)
    return conn


def database_clearup():
    conn.close()
    os.system("/usr/lib/postgresql/9.2/bin/pg_ctl -D $HOME/826prj stop")
    time.sleep(1)


def table_fresh_create(conn, name, columns, flag = True):
    cur = conn.cursor()
    if flag:
        try:
            cur.execute("DROP TABLE %s;" % name)
            conn.commit()
        except psycopg2.Error:
            print ""
            pass
    try:
        cur.execute("CREATE TABLE %s (%s);" % (name, columns))
    except:
        print
    conn.commit()
    cur.close()


def copy_table(src, cpy):
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE %s AS TABLE %d;" % (src, cpy))
    except psycopg2.Error:
        print "Error when copying %s to %s" % (src, cpy)
    conn.commit()
    cur.close()


def drop_table(tb):
    cur = conn.cursor()
    try:
        cur.execute("DROP TABLE %s;" % tb)
    except psycopg2.Error:
        print "Error when dropping %s" % tb
    conn.commit()
    cur.close()


def get_distinct_val(new_tb, tb, col):
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE %s AS SELECT DISTINCT %s FROM %s;" % (new_tb, col, tb))
    except psycopg2.Error:
        print "Error in get_distinct_val on table %s col %s" % (tb, col)
    conn.commit()
    cur.close()

conn = init_database()
a = raw_input("press to continue...\n")
database_clearup()