import pytest

import pymysql
from pymysql import util
from pymysql.tests import base
from pymysql.constants import CLIENT


class TestNextset(base.PyMySQLTestCase):

    def test_nextset(self):
        con = self.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            client_flag=CLIENT.MULTI_STATEMENTS)
        cur = con.cursor()
        cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        r = cur.nextset()
        self.assertTrue(r)

        self.assertEqual([(2,)], list(cur))
        self.assertIsNone(cur.nextset())

    def test_skip_nextset(self):
        cur = self.connect(client_flag=CLIENT.MULTI_STATEMENTS).cursor()
        cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        cur.execute("SELECT 42")
        self.assertEqual([(42,)], list(cur))

    def test_nextset_error(self):
        con = self.connect(client_flag=CLIENT.MULTI_STATEMENTS)
        cur = con.cursor()

        for i in range(3):
            cur.execute("SELECT %s; xyzzy;", (i,))
            self.assertEqual([(i,)], list(cur))
            with self.assertRaises(pymysql.ProgrammingError):
                cur.nextset()
            self.assertEqual((), cur.fetchall())

    def test_ok_and_next(self):
        cur = self.connect(client_flag=CLIENT.MULTI_STATEMENTS).cursor()
        cur.execute("SELECT 1; commit; SELECT 2;")
        self.assertEqual([(1,)], list(cur))
        self.assertTrue(cur.nextset())
        self.assertTrue(cur.nextset())
        self.assertEqual([(2,)], list(cur))
        self.assertFalse(bool(cur.nextset()))

    @pytest.mark.xfail
    def test_multi_cursor(self):
        con = self.connect(client_flag=CLIENT.MULTI_STATEMENTS)
        cur1 = con.cursor()
        cur2 = con.cursor()

        cur1.execute("SELECT 1; SELECT 2;")
        cur2.execute("SELECT 42")

        self.assertEqual([(1,)], list(cur1))
        self.assertEqual([(42,)], list(cur2))

        r = cur1.nextset()
        self.assertTrue(r)

        self.assertEqual([(2,)], list(cur1))
        self.assertIsNone(cur1.nextset())

    def test_multi_statement_warnings(self):
        con = self.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            client_flag=CLIENT.MULTI_STATEMENTS)
        cursor = con.cursor()

        try:
            cursor.execute('DROP TABLE IF EXISTS a; '
                           'DROP TABLE IF EXISTS b;')
        except TypeError:
            self.fail()

    #TODO: How about SSCursor and nextset?
    # It's very hard to implement correctly...
