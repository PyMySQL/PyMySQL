from pymysql.tests import base
import pymysql.cursors

import datetime

class TestOrderedDictCursor(base.PyMySQLTestCase):

    def test_OrderedDictCursor(self):
        #all assert test compare to the structure as would come out from MySQLdb 
        conn = self.connections[0]
        c = conn.cursor(pymysql.cursors.OrderedDictCursor)
        # create a table ane some data to query
        c.execute("""CREATE TABLE ordereddictcursor (name char(20), age int , DOB datetime)""")
        data = (("bob",21,"1990-02-06 23:04:56"),
                ("jim",56,"1955-05-09 13:12:45"),
                ("fred",100,"1911-09-12 01:01:01"))
        bob =  {'name':'bob','age':21,'DOB':datetime.datetime(1990, 02, 6, 23, 04, 56)}
        jim =  {'name':'jim','age':56,'DOB':datetime.datetime(1955, 05, 9, 13, 12, 45)}
        fred = {'name':'fred','age':100,'DOB':datetime.datetime(1911, 9, 12, 1, 1, 1)}
        try:
            c.executemany("insert into ordereddictcursor values (%s,%s,%s)", data)
            # try an update which should return no rows
            c.execute("update ordereddictcursor set age=20 where name='bob'")
            bob['age'] = 20
            # pull back the single row dict for bob and check
            c.execute("SELECT * from ordereddictcursor where name='bob'")
            r = c.fetchone()
            self.assertEqual(bob,r,"fetchone via OrderedDictCursor failed")
            # same again, but via fetchall => tuple)
            c.execute("SELECT * from ordereddictcursor where name='bob'")
            r = c.fetchall()
            self.assertEqual((bob,),r,"fetch a 1 row result via fetchall failed via OrderedDictCursor")
            # same test again but iterate over the 
            c.execute("SELECT * from ordereddictcursor where name='bob'")
            for r in c:
                self.assertEqual(bob, r,"fetch a 1 row result via iteration failed via OrderedDictCursor")
            # get all 3 row via fetchall
            c.execute("SELECT * from ordereddictcursor")
            r = c.fetchall()
            self.assertEqual((bob,jim,fred), r, "fetchall failed via OrderedDictCursor")
            #same test again but do a list comprehension
            c.execute("SELECT * from ordereddictcursor")
            r = [x for x in c]
            self.assertEqual([bob,jim,fred], r, "list comprehension failed via OrderedDictCursor")
            # get all 2 row via fetchmany
            c.execute("SELECT * from ordereddictcursor")
            r = c.fetchmany(2)
            self.assertEqual((bob,jim), r, "fetchmany failed via OrderedDictCursor")
        finally:
            c.execute("drop table ordereddictcursor")

__all__ = ["TestOrderedDictCursor"]

if __name__ == "__main__":
    import unittest
    unittest.main()
