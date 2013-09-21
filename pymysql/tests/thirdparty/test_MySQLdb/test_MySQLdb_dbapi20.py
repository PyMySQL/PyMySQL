#!/usr/bin/env python
from . import dbapi20
import pymysql
from pymysql.tests import base

try:
    import unittest2 as unittest
except ImportError:
    import unittest


class test_MySQLdb(dbapi20.DatabaseAPI20Test):
    driver = pymysql
    connect_args = ()
    connect_kw_args = base.PyMySQLTestCase.databases[0].copy()
    connect_kw_args.update(dict(read_default_file='~/.my.cnf',
                                charset='utf8',
                                sql_mode="ANSI,STRICT_TRANS_TABLES,TRADITIONAL"))

    def test_setoutputsize(self): pass
    def test_setoutputsize_basic(self): pass
    def test_nextset(self): pass

    """The tests on fetchone and fetchall and rowcount bogusly
    test for an exception if the statement cannot return a
    result set. MySQL always returns a result set; it's just that
    some things return empty result sets."""

    def test_fetchall(self):
        con = self._connect()
        try:
            cur = con.cursor()
            # cursor.fetchall should raise an Error if called
            # without executing a query that may return rows (such
            # as a select)
            self.assertRaises(self.driver.Error, cur.fetchall)

            self.executeDDL1(cur)
            for sql in self._populate():
                cur.execute(sql)

            # cursor.fetchall should raise an Error if called
            # after executing a a statement that cannot return rows
##             self.assertRaises(self.driver.Error,cur.fetchall)

            cur.execute('select name from %sbooze' % self.table_prefix)
            rows = cur.fetchall()
            self.assertTrue(cur.rowcount in (-1,len(self.samples)))
            self.assertEqual(len(rows),len(self.samples),
                'cursor.fetchall did not retrieve all rows'
                )
            rows = [r[0] for r in rows]
            rows.sort()
            for i in range(0,len(self.samples)):
                self.assertEqual(rows[i],self.samples[i],
                'cursor.fetchall retrieved incorrect rows'
                )
            rows = cur.fetchall()
            self.assertEqual(
                len(rows),0,
                'cursor.fetchall should return an empty list if called '
                'after the whole result set has been fetched'
                )
            self.assertTrue(cur.rowcount in (-1,len(self.samples)))

            self.executeDDL2(cur)
            cur.execute('select name from %sbarflys' % self.table_prefix)
            rows = cur.fetchall()
            self.assertTrue(cur.rowcount in (-1,0))
            self.assertEqual(len(rows),0,
                'cursor.fetchall should return an empty list if '
                'a select query returns no rows'
                )

        finally:
            con.close()

    def test_fetchone(self):
        con = self._connect()
        try:
            cur = con.cursor()

            # cursor.fetchone should raise an Error if called before
            # executing a select-type query
            self.assertRaises(self.driver.Error,cur.fetchone)

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            self.executeDDL1(cur)
##             self.assertRaises(self.driver.Error,cur.fetchone)

            cur.execute('select name from %sbooze' % self.table_prefix)
            self.assertEqual(cur.fetchone(),None,
                'cursor.fetchone should return None if a query retrieves '
                'no rows'
                )
            self.assertTrue(cur.rowcount in (-1,0))

            # cursor.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
                ))
##             self.assertRaises(self.driver.Error,cur.fetchone)

            cur.execute('select name from %sbooze' % self.table_prefix)
            r = cur.fetchone()
            self.assertEqual(len(r),1,
                'cursor.fetchone should have retrieved a single row'
                )
            self.assertEqual(r[0],'Victoria Bitter',
                'cursor.fetchone retrieved incorrect data'
                )
##             self.assertEqual(cur.fetchone(),None,
##                 'cursor.fetchone should return None if no more rows available'
##                 )
            self.assertTrue(cur.rowcount in (-1,1))
        finally:
            con.close()

    # Same complaint as for fetchall and fetchone
    def test_rowcount(self):
        con = self._connect()
        try:
            cur = con.cursor()
            self.executeDDL1(cur)
##             self.assertEqual(cur.rowcount,-1,
##                 'cursor.rowcount should be -1 after executing no-result '
##                 'statements'
##                 )
            cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
                ))
##             self.assertTrue(cur.rowcount in (-1,1),
##                 'cursor.rowcount should == number or rows inserted, or '
##                 'set to -1 after executing an insert statement'
##                 )
            cur.execute("select name from %sbooze" % self.table_prefix)
            self.assertTrue(cur.rowcount in (-1,1),
                'cursor.rowcount should == number of rows returned, or '
                'set to -1 after executing a select statement'
                )
            self.executeDDL2(cur)
##             self.assertEqual(cur.rowcount,-1,
##                 'cursor.rowcount not being reset to -1 after executing '
##                 'no-result statements'
##                 )
        finally:
            con.close()

    def test_callproc(self):
        pass # performed in test_MySQL_capabilities

    def help_nextset_setUp(self,cur):
        ''' Should create a procedure called deleteme
            that returns two result sets, first the
            number of rows in booze then "name from booze"
        '''
        sql="""
           create procedure deleteme()
           begin
               select count(*) from %(tp)sbooze;
               select name from %(tp)sbooze;
           end
        """ % dict(tp=self.table_prefix)
        cur.execute(sql)

    def help_nextset_tearDown(self,cur):
        'If cleaning up is needed after nextSetTest'
        cur.execute("drop procedure deleteme")

    @unittest.expectedFailure
    def test_nextset(self):
        from warnings import warn
        con = self._connect()
        try:
            cur = con.cursor()
            if not hasattr(cur,'nextset'):
                return

            try:
                self.executeDDL1(cur)
                sql=self._populate()
                for sql in self._populate():
                    cur.execute(sql)

                self.help_nextset_setUp(cur)

                cur.callproc('deleteme')
                numberofrows=cur.fetchone()
                assert numberofrows[0]== len(self.samples)
                assert cur.nextset()
                names=cur.fetchall()
                assert len(names) == len(self.samples)
                s=cur.nextset()
                if s:
                    empty = cur.fetchall()
                    self.assertEqual(len(empty), 0,
                                      "non-empty result set after other result sets")
                    #warn("Incompatibility: MySQL returns an empty result set for the CALL itself",
                    #     Warning)
                #assert s == None,'No more return sets, should return None'
            finally:
                self.help_nextset_tearDown(cur)

        finally:
            con.close()


if __name__ == '__main__':
    unittest.main()
