import time
from tornado.testing import gen_test
from tornado import gen

import tornado_mysql
from tornado_mysql.tests import base


class TestConnection(base.PyMySQLTestCase):
    @gen_test
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = yield tornado_mysql.connect(**arg)

    @gen_test
    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = self.connections[0].cursor()
        yield cur.execute("SELECT @@max_allowed_packet")
        if cur.fetchone()[0] < 16*1024*1024 + 10:
            print("Set max_allowed_packet to bigger than 17MB")
        else:
            t = 'a' * (16*1024*1024)
            yield cur.execute("SELECT '" + t + "'")
            assert cur.fetchone()[0] == t

    @gen_test
    def test_escape_string(self):
        con = self.connections[0]
        cur = con.cursor()

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
        yield cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    @gen_test
    def test_autocommit(self):
        con = self.connections[0]
        self.assertFalse(con.get_autocommit())

        cur = con.cursor()
        yield cur.execute("SET AUTOCOMMIT=1")
        self.assertTrue(con.get_autocommit())

        yield con.autocommit(False)
        self.assertFalse(con.get_autocommit())
        yield cur.execute("SELECT @@AUTOCOMMIT")
        self.assertEqual(cur.fetchone()[0], 0)

    @gen_test
    def test_select_db(self):
        con = self.connections[0]
        current_db = self.databases[0]['db']
        other_db = self.databases[1]['db']

        cur = con.cursor()
        yield cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], current_db)

        yield con.select_db(other_db)
        yield cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], other_db)

    @gen_test
    def test_connection_gone_away(self):
        """
        http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
        http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html#error_cr_server_gone_error
        """
        con = self.connections[0]
        cur = con.cursor()
        yield cur.execute("SET wait_timeout=1")
        time.sleep(2)
        with self.assertRaises(tornado_mysql.OperationalError) as cm:
            yield cur.execute("SELECT 1+1")
        # error occures while reading, not writing because of socket buffer.
        #self.assertEquals(cm.exception.args[0], 2006)
        self.assertIn(cm.exception.args[0], (2006, 2013))


if __name__ == "__main__":
    import unittest
    unittest.main()
