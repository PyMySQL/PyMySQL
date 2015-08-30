import datetime
import decimal
import time
import sys
import unittest2
import pymysql
from pymysql.tests import base


class TestConnection(base.PyMySQLTestCase):
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = pymysql.connect(**arg)

    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = self.connections[0].cursor()
        cur.execute("SELECT @@max_allowed_packet")
        if cur.fetchone()[0] < 16*1024*1024 + 10:
            print("Set max_allowed_packet to bigger than 17MB")
            return
        t = 'a' * (16*1024*1024)
        cur.execute("SELECT '" + t + "'")
        assert cur.fetchone()[0] == t

    def test_autocommit(self):
        con = self.connections[0]
        self.assertFalse(con.get_autocommit())

        cur = con.cursor()
        cur.execute("SET AUTOCOMMIT=1")
        self.assertTrue(con.get_autocommit())

        con.autocommit(False)
        self.assertFalse(con.get_autocommit())
        cur.execute("SELECT @@AUTOCOMMIT")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_select_db(self):
        con = self.connections[0]
        current_db = self.databases[0]['db']
        other_db = self.databases[1]['db']

        cur = con.cursor()
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], current_db)

        con.select_db(other_db)
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], other_db)

    def test_connection_gone_away(self):
        """
        http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
        http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html#error_cr_server_gone_error
        """
        con = self.connections[0]
        cur = con.cursor()
        cur.execute("SET wait_timeout=1")
        time.sleep(2)
        with self.assertRaises(pymysql.OperationalError) as cm:
            cur.execute("SELECT 1+1")
        # error occures while reading, not writing because of socket buffer.
        #self.assertEquals(cm.exception.args[0], 2006)
        self.assertIn(cm.exception.args[0], (2006, 2013))

    def test_init_command(self):
        conn = pymysql.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            **self.databases[0]
        )
        c = conn.cursor()
        c.execute('select "foobar";')
        self.assertEqual(('foobar',), c.fetchone())
        conn.close()
        with self.assertRaises(pymysql.err.Error):
            conn.ping(reconnect=False)

    def test_read_default_group(self):
        conn = pymysql.connect(
            read_default_group='client',
            **self.databases[0]
        )
        self.assertTrue(conn.open)

    def test_context(self):
        with self.assertRaises(ValueError):
            c = pymysql.connect(**self.databases[0])
            with c as cur:
                cur.execute('create table test ( a int )')
                c.begin()
                cur.execute('insert into test values ((1))')
                raise ValueError('pseudo abort')
                c.commit()
        c = pymysql.connect(**self.databases[0])
        with c as cur:
            cur.execute('select count(*) from test')
            self.assertEqual(0, cur.fetchone()[0])
            cur.execute('insert into test values ((1))')
        with c as cur:
            cur.execute('select count(*) from test')
            self.assertEqual(1,cur.fetchone()[0])
            cur.execute('drop table test')

    def test_set_charset(self):
        c = pymysql.connect(**self.databases[0])
        c.set_charset('utf8')
        # TODO validate setting here

    def test_defer_connect(self):
        import socket
        for db in self.databases:
            d = db.copy()
            try:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.connect(d['unix_socket'])
            except KeyError:
                sock = socket.create_connection(
                                (d.get('host', 'localhost'), d.get('port', 3306)))
            for k in ['unix_socket', 'host', 'port']:
                try:
                    del d[k]
                except KeyError:
                    pass

            c = pymysql.connect(defer_connect=True, **d)
            self.assertFalse(c.open)
            c.connect(sock)
            c.close()

    @unittest2.skipUnless(sys.version_info[0:2] >= (3,2), "required py-3.2")
    def test_no_delay_warning(self):
        current_db = self.databases[0].copy()
        current_db['no_delay'] =  True
        with self.assertWarns(DeprecationWarning) as cm:
            conn = pymysql.connect(**current_db)


# A custom type and function to escape it
class Foo(object):
    value = "bar"


def escape_foo(x, d):
    return x.value


class TestEscape(base.PyMySQLTestCase):
    def test_escape_string(self):
        con = self.connections[0]
        cur = con.cursor()

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
        # added NO_AUTO_CREATE_USER as not including it in 5.7 generates warnings
        cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES,NO_AUTO_CREATE_USER'")
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    def test_escape_builtin_encoders(self):
        con = self.connections[0]
        cur = con.cursor()

        val = datetime.datetime(2012, 3, 4, 5, 6)
        self.assertEqual(con.escape(val, con.encoders), "'2012-03-04 05:06:00'")

    def test_escape_custom_object(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = {Foo: escape_foo}
        self.assertEqual(con.escape(Foo(), mapping), "bar")

    def test_escape_fallback_encoder(self):
        con = self.connections[0]
        cur = con.cursor()

        class Custom(str):
            pass

        mapping = {pymysql.text_type: pymysql.escape_string}
        self.assertEqual(con.escape(Custom('foobar'), mapping), "'foobar'")

    def test_escape_no_default(self):
        con = self.connections[0]
        cur = con.cursor()

        self.assertRaises(TypeError, con.escape, 42, {})

    def test_escape_dict_value(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape({'foo': Foo()}, mapping), {'foo': "bar"})

    def test_escape_list_item(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape([Foo()], mapping), "(bar)")
