from pymysql.tests import base
import pymysql.cursors

import datetime


class TestDictCursor(base.PyMySQLTestCase):
    bob = {'name': 'bob', 'age': 21, 'DOB': datetime.datetime(1990, 2, 6, 23, 4, 56)}
    jim = {'name': 'jim', 'age': 56, 'DOB': datetime.datetime(1955, 5, 9, 13, 12, 45)}
    fred = {'name': 'fred', 'age': 100, 'DOB': datetime.datetime(1911, 9, 12, 1, 1, 1)}

    cursor_type = pymysql.cursors.DictCursor

    def setUp(self):
        super(TestDictCursor, self).setUp()
        self.conn = conn = self.connections[0]
        c = conn.cursor(self.cursor_type)

        # create a table ane some data to query
        c.execute("drop table if exists dictcursor")
        c.execute("""CREATE TABLE dictcursor (name char(20), age int , DOB datetime)""")
        data = [("bob", 21, "1990-02-06 23:04:56"),
                ("jim", 56, "1955-05-09 13:12:45"),
                ("fred", 100, "1911-09-12 01:01:01")]
        c.executemany("insert into dictcursor values (%s,%s,%s)", data)

    def tearDown(self):
        c = self.conn.cursor()
        c.execute("drop table dictcursor")
        super(TestDictCursor, self).tearDown()

    def test_DictCursor(self):
        bob, jim, fred = self.bob.copy(), self.jim.copy(), self.fred.copy()
        #all assert test compare to the structure as would come out from MySQLdb
        conn = self.conn
        c = conn.cursor(self.cursor_type)

        # try an update which should return no rows
        c.execute("update dictcursor set age=20 where name='bob'")
        bob['age'] = 20
        # pull back the single row dict for bob and check
        c.execute("SELECT * from dictcursor where name='bob'")
        r = c.fetchone()
        self.assertEqual(bob, r, "fetchone via DictCursor failed")
        # same again, but via fetchall => tuple)
        c.execute("SELECT * from dictcursor where name='bob'")
        r = c.fetchall()
        self.assertEqual([bob], r, "fetch a 1 row result via fetchall failed via DictCursor")
        # same test again but iterate over the
        c.execute("SELECT * from dictcursor where name='bob'")
        for r in c:
            self.assertEqual(bob, r, "fetch a 1 row result via iteration failed via DictCursor")
        # get all 3 row via fetchall
        c.execute("SELECT * from dictcursor")
        r = c.fetchall()
        self.assertEqual([bob,jim,fred], r, "fetchall failed via DictCursor")
        #same test again but do a list comprehension
        c.execute("SELECT * from dictcursor")
        r = list(c)
        self.assertEqual([bob,jim,fred], r, "DictCursor should be iterable")
        # get all 2 row via fetchmany
        c.execute("SELECT * from dictcursor")
        r = c.fetchmany(2)
        self.assertEqual([bob, jim], r, "fetchmany failed via DictCursor")

    def test_custom_dict(self):
        class MyDict(dict): pass

        class MyDictCursor(self.cursor_type):
            dict_type = MyDict

        keys = ['name', 'age', 'DOB']
        bob = MyDict([(k, self.bob[k]) for k in keys])
        jim = MyDict([(k, self.jim[k]) for k in keys])
        fred = MyDict([(k, self.fred[k]) for k in keys])

        cur = self.conn.cursor(MyDictCursor)
        cur.execute("SELECT * FROM dictcursor WHERE name='bob'")
        r = cur.fetchone()
        self.assertEqual(bob, r, "fetchone() returns MyDictCursor")

        cur.execute("SELECT * FROM dictcursor")
        r = cur.fetchall()
        self.assertEqual([bob, jim, fred], r,
                         "fetchall failed via MyDictCursor")

        cur.execute("SELECT * FROM dictcursor")
        r = list(cur)
        self.assertEqual([bob, jim, fred], r,
                         "list failed via MyDictCursor")

        cur.execute("SELECT * FROM dictcursor")
        r = cur.fetchmany(2)
        self.assertEqual([bob, jim], r,
                         "list failed via MyDictCursor")


class TestSSDictCursor(TestDictCursor):
    cursor_type = pymysql.cursors.SSDictCursor


if __name__ == "__main__":
    import unittest
    unittest.main()
