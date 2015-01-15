from tornado.testing import gen_test
from tornado import gen
import unittest

from tornado_mysql.tests import base
from tornado_mysql import util


class TestNextset(base.PyMySQLTestCase):

    def setUp(self):
        super(TestNextset, self).setUp()
        self.con = self.connections[0]

    @gen_test
    def test_nextset(self):
        cur = self.con.cursor()
        yield cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        r = yield cur.nextset()
        self.assertTrue(r)

        self.assertEqual([(2,)], list(cur))
        res = yield cur.nextset()
        self.assertIsNone(res)

    @gen_test
    def test_skip_nextset(self):
        cur = self.con.cursor()
        yield cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        yield cur.execute("SELECT 42")
        self.assertEqual([(42,)], list(cur))

    @gen_test
    def test_ok_and_next(self):
        cur = self.con.cursor()
        yield cur.execute("SELECT 1; commit; SELECT 2;")
        self.assertEqual([(1,)], list(cur))
        res = yield cur.nextset()
        self.assertTrue(res)
        res = yield cur.nextset()
        self.assertTrue(res)
        self.assertEqual([(2,)], list(cur))
        res = yield cur.nextset()
        self.assertIsNone(res)

    @unittest.expectedFailure
    @gen_test
    def test_multi_cursor(self):
        cur1 = self.con.cursor()
        cur2 = self.con.cursor()

        yield cur1.execute("SELECT 1; SELECT 2;")
        yield cur2.execute("SELECT 42")

        self.assertEqual([(1,)], list(cur1))
        self.assertEqual([(42,)], list(cur2))

        res = yield cur1.nextset()
        self.assertTrue(res)

        self.assertEqual([(2,)], list(cur1))
        res = yield cur1.nextset()
        self.assertIsNone(res)

    #TODO: How about SSCursor and nextset?
    # It's very hard to implement correctly...
