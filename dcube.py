import psycopg2
import sys
import os

class Table:
    def __init__(self, cur, tb):
        self.cur = cur
        self.tb = tb
    def __del__(self):
        cur.execute("DROP TABLE %s;" % self.tb)


def copy_table(cur, src, cpy):
    try:
        cur.execute("CREATE TABLE %s AS TABLE %d;" % (src, cpy))
    except:
        print "Error when copying %s to %s" % (src, cpy)


def drop_table(cur, tb):
    try:
        cur.execute("DROP TABLE %s;" % tb)
    except:
        print "Error when dropping %s" % tb


def get_distinct_val_table(cur, new_tb, tb, col):
    try:
        cur.execute("CREATE TABLE %s AS SELECT DISTINCT %s FROM %s;" % (new_tb, col, tb))
    except:
        print "Error in get_distinct_val on table %s col %s" % (tb, col)

os.system("/usr/lib/postgresql/9.2/bin/pg_ctl -D $HOME/826prj/ -o '-k /tmp' start")
query = raw_input("press to continue...")
username = os.environ['USER']
port = 15000
conn = psycopg2.connect(dbname=username, user=username, password="", port=port)
cur = conn.cursor()
query = raw_input("input the query to execute...\n")
cur.execute(query)
rows = cur.fetchall()
for row in rows:
    print row[0]
os.system("/usr/lib/postgresql/9.2/bin/pg_ctl -D $HOME/826prj stop")
