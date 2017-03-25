import psycopg2
import sys
import os

global conn
conn = None


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

os.system("/usr/lib/postgresql/9.2/bin/pg_ctl -D $HOME/826prj/ -o '-k /tmp' start")
query = raw_input("press to continue...\n")
username = os.environ['USER']
port = 15000
conn = psycopg2.connect(dbname=username, user=username, password="", port = port)
copy_table("darpa", )
os.system("/usr/lib/postgresql/9.2/bin/pg_ctl -D $HOME/826prj stop")
