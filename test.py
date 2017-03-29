from dcube import *
import math
import psycopg2
import unittest

class TestAlg4(unittest.TestCase):
    
    conn = ''

    def setUp(self):
        os.system("pg_ctl -D $HOME/826prj/ -o '-k /tmp' start")
        time.sleep(1)
        username = os.environ['USER']
        self.conn = psycopg2.connect(dbname=username, user=username, password="", port=PGPORT)
        #a = raw_input("press to continue...\n")
        table_fresh_create(self.conn, "testalg4", "src text, cnt int" , False)


    def test_get_mass(self):
        print "===============test_get_mass==================="
        copy_table(self.conn, "testalg4", "testmass")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO testmass (src, cnt) VALUES ('0.0.0.0', 1);")
        cur.execute("INSERT INTO testmass (src, cnt) VALUES ('0.0.0.1', 2);")
        cur.execute("INSERT INTO testmass (src, cnt) VALUES ('0.0.0.2', 3);")
        cur.execute("INSERT INTO testmass (src, cnt) VALUES ('0.0.0.3', 4);")
        self.conn.commit()
        cur.close()
        mass = get_mass(self.conn, "testmass")
        drop_table(self.conn, "testmass")
        self.assertEqual(mass, 10)
        

    def test_tuple_count(self):
        print "===============test_tuple_count==================="
        copy_table(self.conn, "testalg4", "testcount")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO testcount (src, cnt) VALUES ('0.0.0.0', 1);")
        cur.execute("INSERT INTO testcount (src, cnt) VALUES ('0.0.0.1', 2);")
        cur.execute("INSERT INTO testcount (src, cnt) VALUES ('0.0.0.2', 3);")
        cur.execute("INSERT INTO testcount (src, cnt) VALUES ('0.0.0.3', 4);")
        self.conn.commit()
        cur.close()
        count = tuple_counts(self.conn, "testcount")
        drop_table(self.conn, "testcount")
        self.assertEqual(count, 4)

    def test_tuple_count_distinct(self):
        print "===============test_tuple_count_distinct==================="
        copy_table(self.conn, "testalg4", "testdistinctcount")
        cur = self.conn.cursor()
        cur.execute("INSERT INTO testdistinctcount (src, cnt) VALUES ('0.0.0.0', 1);")
        cur.execute("INSERT INTO testdistinctcount (src, cnt) VALUES ('0.0.0.1', 2);")
        cur.execute("INSERT INTO testdistinctcount (src, cnt) VALUES ('0.0.0.1', 3);")
        cur.execute("INSERT INTO testdistinctcount (src, cnt) VALUES ('0.0.0.3', 4);")
        self.conn.commit()
        cur.close()
        count = tuple_counts_distinct(self.conn, "testdistinctcount", "src")
        self.assertEqual(count, 3)
        count = tuple_counts_distinct(self.conn, "testdistinctcount", "cnt")
        self.assertEqual(count, 4)
        drop_table(self.conn, "testdistinctcount")

    def test_rho(self):
        print "===============test_rho==================="
        #table_fresh_create(self.conn, "testsrc", "src text")
        #table_fresh_create(self.conn, "testdest", "dest text")
        #table_fresh_create(self.conn, "testbucket", "bucket text")
        #table_fresh_create(self.conn, "testrelsrc", "src text")
        #table_fresh_create(self.conn, "testreldest", "dest text")
        #table_fresh_create(self.conn, "testrelbucket", "bucket text")
        #cur = self.conn.cursor()
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.0');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.1');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.2');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.3');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.4');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.5');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.6');")
        #cur.execute("INSERT INTO testsrc (src) VALUES ('0.0.0.7');")

        #cur.execute("INSERT INTO testdest (dest) VALUES ('0.0.0.0');")
        #cur.execute("INSERT INTO testdest (dest) VALUES ('0.0.0.1');")
        #cur.execute("INSERT INTO testdest (dest) VALUES ('0.0.0.2');")
        #cur.execute("INSERT INTO testdest (dest) VALUES ('0.0.0.3');")

        #cur.execute("INSERT INTO testbucket (bucket) VALUES (4);")
        #cur.execute("INSERT INTO testbucket (bucket) VALUES (0);")

        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.0');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.1');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.2');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.3');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.4');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.5');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.6');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('0.0.0.7');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('1.0.0.0');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('1.0.0.1');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('1.0.0.2');")
        #cur.execute("INSERT INTO testrelsrc (src) VALUES ('1.0.0.3');")


        #cur.execute("INSERT INTO testreldest (dest) VALUES ('0.0.0.0');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('0.0.0.1');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('0.0.0.2');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('0.0.0.3');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('1.0.0.0');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('1.0.0.1');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('1.0.0.2');")
        #cur.execute("INSERT INTO testreldest (dest) VALUES ('1.0.0.3');")

        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (4);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (0);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (1);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (2);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (3);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (5);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (6);")
        #cur.execute("INSERT INTO testrelbucket (bucket) VALUES (7);")

        #self.conn.commit()
        #cur.close()
        block_attrs = {"src":8, "dest":4, "bucket":2}
        rel_attrs = {"src":12, "dest":8, "bucket":8}
        rho1 = rho_ari(self.conn, 7., block_attrs, 10., {})
        rho2 = rho_geo(self.conn, 10., block_attrs, 10., {})
        rho3 = rho_susp(self.conn, 7., block_attrs, 10., rel_attrs)
        rho3_correct = 7.*(math.log(7./10.)-1) - 7.*math.log(1./12.) + 10*(1./12.)

        drop_table(self.conn, "testsrc")
        drop_table(self.conn, "testdest")
        drop_table(self.conn, "testbucket")
        drop_table(self.conn, "testrelsrc")
        drop_table(self.conn, "testreldest")
        drop_table(self.conn, "testrelbucket")
        self.assertTrue(abs(rho1-1.5) < 1e-8)
        self.assertTrue(abs(rho2-2.5) < 1e-8)
        print rho3_correct, rho3
        self.assertTrue(abs(rho3-rho3_correct) < 1e-8)

    def tearDown(self):
        drop_table(self.conn, "testalg4")
        self.conn.close()
        os.system("pg_ctl -D $HOME/826prj stop")
        time.sleep(1)

if __name__ == '__main__':
    unittest.main()
