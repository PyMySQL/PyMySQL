from pymysql.tests import base
from pymysql import util

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class TestNextset(base.PyMySQLTestCase):

    def setUp(self):
        super(TestNextset, self).setUp()
        self.con = self.connections[0]

    def test_nextset(self):
        cur = self.con.cursor()
        cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        r = cur.nextset()
        self.assertTrue(r)

        self.assertEqual([(2,)], list(cur))
        self.assertIsNone(cur.nextset())

    def test_skip_nextset(self):
        cur = self.con.cursor()
        cur.execute("SELECT 1; SELECT 2;")
        self.assertEqual([(1,)], list(cur))

        cur.execute("SELECT 42")
        self.assertEqual([(42,)], list(cur))

    def test_ok_and_next(self):
        cur = self.con.cursor()
        cur.execute("SELECT 1; commit; SELECT 2;")
        self.assertEqual([(1,)], list(cur))
        self.assertTrue(cur.nextset())
        self.assertTrue(cur.nextset())
        self.assertEqual([(2,)], list(cur))
        self.assertFalse(bool(cur.nextset()))

    @unittest.expectedFailure
    def test_multi_cursor(self):
        cur1 = self.con.cursor()
        cur2 = self.con.cursor()

        cur1.execute("SELECT 1; SELECT 2;")
        cur2.execute("SELECT 42")

        self.assertEqual([(1,)], list(cur1))
        self.assertEqual([(42,)], list(cur2))

        r = cur1.nextset()
        self.assertTrue(r)

        self.assertEqual([(2,)], list(cur1))
        self.assertIsNone(cur1.nextset())

    #TODO: How about SSCursor and nextset?
    # It's very hard to implement correctly...
