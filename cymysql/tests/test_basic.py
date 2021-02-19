import cymysql
from cymysql.tests import base

import time
import datetime
import struct
import sys


def u(x):
    if sys.version_info[0] < 3:
        import codecs
        return codecs.unicode_escape_decode(x)[0]
    else:
        return x


def int2byte(i):
    return struct.pack("!B", i)


class TestConversion(base.PyMySQLTestCase):
    def test_datatypes(self):
        """ test every data type """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute(
            """create table test_datatypes
                (b bit,
                i int,
                l bigint,
                f real,
                s varchar(32),
                u varchar(32),
                bb blob,
                d date,
                dt datetime,
                ts timestamp,
                td time,
                t time,
                st datetime)"""
        )
        try:
            # insert values

            v = (
                True,
                -3,
                123456789012,
                5.7,
                "hello'\" world",
                u"Espa\xc3\xb1ol",
                "binary\x00data".encode(conn.encoding),
                datetime.date(1988, 2, 2),
                datetime.datetime(2014, 5, 15, 7, 45, 57),
                datetime.timedelta(5, 6),
                datetime.time(16, 32),
                time.localtime()
            )
            c.execute(
                "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                v
            )
            c.execute("select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
            r = c.fetchone()
            self.assertEqual(int2byte(1), r[0])
            self.assertEqual(v[1:10], r[1:10])
            self.assertEqual(datetime.timedelta(0, 60 * (v[10].hour * 60 + v[10].minute)), r[10])
            self.assertEqual(datetime.datetime(*v[-1][:6]), r[-1])

            c.execute("delete from test_datatypes")

            # check nulls
            c.execute(
                "insert into test_datatypes (b,i,l,f,s,u,bb,d,dt,td,t,st) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [None] * 12
            )
            c.execute("select b,i,l,f,s,u,bb,d,dt,td,t,st from test_datatypes")
            r = c.fetchone()
            self.assertEqual(tuple([None] * 12), r)

            c.execute("delete from test_datatypes")

            # check sequence type
            c.execute("insert into test_datatypes (i, l) values (2,4), (6,8), (10,12)")
            c.execute("select l from test_datatypes where i in %s order by i", ((2, 6), ))
            r = c.fetchall()
            self.assertEqual([(4, ), (8, )], r)
        finally:
            c.execute("drop table test_datatypes")

    def test_dict(self):
        """ test dict escaping """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_dict (a integer, b integer, c integer)")
        try:
            c.execute("insert into test_dict (a,b,c) values (%(a)s, %(b)s, %(c)s)", {"a": 1, "b": 2, "c": 3})
            c.execute("select a,b,c from test_dict")
            self.assertEqual((1, 2, 3), c.fetchone())
        finally:
            c.execute("drop table test_dict")

    def test_string(self):
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_dict (a text)")
        test_value = "I am a test string"
        try:
            c.execute("insert into test_dict (a) values (%s)", test_value)
            c.execute("select a from test_dict")
            self.assertEqual((test_value,), c.fetchone())
        finally:
            c.execute("drop table test_dict")

    def test_integer(self):
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_dict (a integer)")
        test_value = 12345
        try:
            c.execute("insert into test_dict (a) values (%s)", test_value)
            c.execute("select a from test_dict")
            self.assertEqual((test_value,), c.fetchone())
        finally:
            c.execute("drop table test_dict")

    def test_big_blob(self):
        """ test tons of data """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_big_blob (b blob)")
        try:
            data = "cymysql" * 1024
            c.execute("insert into test_big_blob (b) values (%s)", (data,))
            c.execute("select b from test_big_blob")
            self.assertEqual(data.encode(conn.encoding), c.fetchone()[0])
        finally:
            c.execute("drop table test_big_blob")

    def test_untyped(self):
        """ test conversion of null, empty string """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("select null,''")
        self.assertEqual((None, u('')), c.fetchone())
        c.execute("select '',null")
        self.assertEqual((u(''), None), c.fetchone())

    def test_datetime(self):
        """ test conversion of null, empty string """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("select time('12:30'), time('23:12:59'), time('23:12:59.05100')")
        if conn.server_version[:3] > '5.5':
            # support Fractional Seconds in Time
            # http://dev.mysql.com/doc/refman/5.6/en/fractional-seconds.html
            self.assertEqual((
                datetime.timedelta(0, 45000),
                datetime.timedelta(0, 83579),
                datetime.timedelta(0, 83579, 5100)
                ),
                c.fetchone()
            )
        else:
            self.assertEqual((
                datetime.timedelta(0, 45000),
                datetime.timedelta(0, 83579),
                datetime.timedelta(0, 83579, 51000)
                ),
                c.fetchone()
            )

    def test_callproc(self):
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("drop procedure if exists test_proc")
        c.execute("create procedure test_proc(IN s varchar(20)) begin select lower(s); end")
        c.callproc('test_proc', ('Foo', ))
        r = c.fetchall()
        self.assertEqual(len(r), 1)
        self.assertEqual(r[0], (u'foo', ))


class TestCursor(base.PyMySQLTestCase):
    def test_fetch_no_result(self):
        """ test a fetchone() with no rows """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_nr (b varchar(32))")
        try:
            data = "cymysql"
            c.execute("insert into test_nr (b) values (%s)", (data,))
            self.assertEqual(None, c.fetchone())
        finally:
            c.execute("drop table test_nr")

    def test_aggregates(self):
        """ test aggregate functions """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            c.execute('create table test_aggregates (i integer)')
            for i in range(0, 10):
                c.execute('insert into test_aggregates (i) values (%s)', (i,))
            c.execute('select sum(i) from test_aggregates')
            r, = c.fetchone()
            self.assertEqual(sum(range(0, 10)), r)
        finally:
            c.execute('drop table test_aggregates')

    def test_single_tuple(self):
        """ test a single tuple """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            c.execute("create table mystuff (id integer primary key)")
            c.execute("insert into mystuff (id) values (1)")
            c.execute("insert into mystuff (id) values (2)")
            c.execute("select id from mystuff where id in %s", ((1,),))
            self.assertEqual([(1,)], list(c.fetchall()))
        finally:
            c.execute("drop table mystuff")

    def test_cyclic_reference_leak(self):
        """
        Ensure that a cylic reference between Connection and Cursor isn't leaking
        objects.
        """
        import gc
        gc.collect()

        for i in range(0, 5):
            conn = cymysql.connect(**self.databases[0])
            c = conn.cursor()
            c.execute('show tables in %s' % (self.databases[0]['db']))
            c.close()
            conn.close()
        gc.collect()

        conns = 0
        for obj in gc.garbage:
            if 'Connection' in repr(obj):
                conns += 1

        if conns > 0:
            raise Exception('%d connections were leaked.' % (conns))

    def test_close_cursor(self):
        conn = self.connections[0]
        c = conn.cursor()
        c.close()
        try:
            c.execute("select 1")
        except cymysql.ProgrammingError:
            pass


class TestCharset(base.PyMySQLTestCase):
    def test_charset(self):
        conn = cymysql.connect(
            host=self.test_host, user="root", passwd=self.test_passwd, db="mysql",
            charset="utf8mb4"
        )
        c = conn.cursor()
        c.execute("select user from user where user='root'")
        self.assertEqual(c.fetchone()[0], 'root')
        conn.close()


__all__ = ["TestConversion", "TestCursor", "TestCharset"]

if __name__ == "__main__":
    import unittest
    unittest.main()
