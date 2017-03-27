from dcube import *
import psycopg2
import unittest

class TestAlg4(unittest.TestCase):
    
    conn = ''

    def setUp(self):
        os.system("pg_ctl -D $HOME/826prj/ -o '-k /tmp' start")
        time.sleep(1)
        username = os.environ['USER']
        self.conn = psycopg2.connect(dbname=username, user=username, password="", port=PGPORT)
        print self.conn.cursor
        #a = raw_input("press to continue...\n")
        table_fresh_create(self.conn, "testalg4", "src text, cnt int" , False)


    def test_get_mass(self):
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
        


    def tearDown(self):
        drop_table(self.conn, "testalg4")
        self.conn.close()
        os.system("pg_ctl -D $HOME/826prj stop")
        time.sleep(1)

if __name__ == '__main__':
    unittest.main()
