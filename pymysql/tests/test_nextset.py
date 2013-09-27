from pymysql.tests import base
from pymysql import util


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
