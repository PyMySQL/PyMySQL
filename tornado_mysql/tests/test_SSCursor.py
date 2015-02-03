import warnings
from tornado_mysql import ProgrammingError
from tornado import gen
from tornado.testing import gen_test
from tornado_mysql import NotSupportedError

import tornado_mysql.cursors
from tornado_mysql.tests import base


class TestSSCursor(base.PyMySQLTestCase):

    data = [
        ('America', '', 'America/Jamaica'),
        ('America', '', 'America/Los_Angeles'),
        ('America', '', 'America/Lima'),
        ('America', '', 'America/New_York'),
        ('America', '', 'America/Menominee'),
        ('America', '', 'America/Havana'),
        ('America', '', 'America/El_Salvador'),
        ('America', '', 'America/Costa_Rica'),
        ('America', '', 'America/Denver'),
        ('America', '', 'America/Detroit'),]

    @gen_test
    def test_SSCursor(self):
        affected_rows = 18446744073709551615

        conn = self.connections[0]
        cursor = conn.cursor(tornado_mysql.cursors.SSCursor)

        # Create table
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield cursor.execute('DROP TABLE IF EXISTS tz_data;')
        yield cursor.execute('CREATE TABLE tz_data ('
                             'region VARCHAR(64),'
                             'zone VARCHAR(64),'
                             'name VARCHAR(64))')

        try:
            # Test INSERT
            for i in self.data:
                yield cursor.execute('INSERT INTO tz_data VALUES (%s, %s, %s)', i)
                self.assertEqual(conn.affected_rows(), 1, 'affected_rows does not match')
            yield conn.commit()
            # Test fetchone()
            iter = 0
            yield cursor.execute('SELECT * FROM tz_data;')
            while True:
                row = yield cursor.fetchone()

                if row is None:
                    break
                iter += 1

                # Test cursor.rowcount
                self.assertEqual(cursor.rowcount, affected_rows,
                                 'cursor.rowcount != %s' % (str(affected_rows)))

                # Test cursor.rownumber
                self.assertEqual(cursor.rownumber, iter,
                                 'cursor.rowcount != %s' % (str(iter)))

                # Test row came out the same as it went in
                self.assertEqual((row in self.data), True,
                                 'Row not found in source self.data')

            # Test fetchall
            yield cursor.execute('SELECT * FROM tz_data')
            r = yield cursor.fetchall()
            self.assertEqual(len(r), len(self.data),
                             'fetchall failed. Number of rows does not match')

            # Test fetchmany
            yield cursor.execute('SELECT * FROM tz_data')
            r = yield cursor.fetchmany(2)
            self.assertEqual(len(r), 2,
                             'fetchmany failed. Number of rows does not match')

            # So MySQLdb won't throw "Commands out of sync"
            while True:
                res = yield cursor.fetchone()
                if res is None:
                    break

            # Test update, affected_rows()
            yield cursor.execute('UPDATE tz_data SET zone = %s', ['Foo'])
            yield conn.commit()
            self.assertEqual(cursor.rowcount, len(self.data),
                             'Update failed. affected_rows != %s' % (str(len(self.data))))

            # Test executemany
            yield cursor.executemany('INSERT INTO tz_data VALUES (%s, %s, %s)', self.data)
            self.assertEqual(cursor.rowcount, len(self.data),
                             'executemany failed. cursor.rowcount != %s' % (str(len(self.data))))

            # Test multiple datasets
            yield cursor.execute('SELECT 1; SELECT 2; SELECT 3')
            res = yield cursor.fetchall()
            self.assertListEqual(res, [(1, )])
            res = yield cursor.nextset()
            self.assertTrue(res)
            res = yield cursor.fetchall()
            self.assertListEqual(res, [(2, )])
            res = yield cursor.nextset()
            self.assertTrue(res)
            res = yield cursor.fetchall()
            self.assertListEqual(res, [(3, )])
            res = yield cursor.nextset()
            self.assertFalse(res)
        finally:
            yield cursor.execute('DROP TABLE tz_data')
            yield cursor.close()

    @gen.coroutine
    def _prepare(self):
        conn = self.connections[0]
        cursor = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield cursor.execute('DROP TABLE IF EXISTS tz_data;')
        yield cursor.execute('CREATE TABLE tz_data ('
                             'region VARCHAR(64),'
                             'zone VARCHAR(64),'
                             'name VARCHAR(64))')

        yield cursor.executemany(
            'INSERT INTO tz_data VALUES (%s, %s, %s)', self.data)
        yield conn.commit()
        yield cursor.close()

    @gen.coroutine
    def _cleanup(self):
        conn = self.connections[0]
        cursor = conn.cursor()
        yield cursor.execute('DROP TABLE IF EXISTS tz_data;')
        yield cursor.close()

    @gen_test
    def test_sscursor_executemany(self):
        conn = self.connections[0]
        yield self._prepare()
        cursor = conn.cursor(tornado_mysql.cursors.SSCursor)
        # Test executemany
        yield cursor.executemany(
            'INSERT INTO tz_data VALUES (%s, %s, %s)', self.data)
        yield cursor.close()
        msg = 'executemany failed. cursor.rowcount != %s'
        self.assertEqual(cursor.rowcount, len(self.data),
                         msg % (str(len(self.data))))
        yield self._cleanup()

    @gen_test
    def test_sscursor_scroll_relative(self):
        conn = self.connections[0]
        yield self._prepare()
        cursor = conn.cursor(tornado_mysql.cursors.SSCursor)
        yield cursor.execute('SELECT * FROM tz_data;')
        yield cursor.scroll(1)
        ret = yield cursor.fetchone()
        self.assertEqual(('America', '', 'America/Los_Angeles'), ret)
        yield cursor.close()
        yield self._cleanup()

    @gen_test
    def test_sscursor_scroll_absolute(self):
        conn = self.connections[0]
        yield self._prepare()
        cursor = conn.cursor(tornado_mysql.cursors.SSCursor)
        yield cursor.execute('SELECT * FROM tz_data;')
        yield cursor.scroll(2, mode='absolute')
        ret = yield cursor.fetchone()
        self.assertEqual(('America', '', 'America/Lima'), ret)
        yield cursor.close()
        yield self._cleanup()

    @gen_test
    def test_sscursor_scroll_errors(self):
        yield self._prepare()
        conn = self.connections[0]
        cursor = conn.cursor(tornado_mysql.cursors.SSCursor)

        yield cursor.execute('SELECT * FROM tz_data;')

        with self.assertRaises(NotSupportedError):
            yield cursor.scroll(-2, mode='relative')

        yield cursor.scroll(2, mode='absolute')

        with self.assertRaises(NotSupportedError):
            yield cursor.scroll(1, mode='absolute')
        with self.assertRaises(ProgrammingError):
            yield cursor.scroll(3, mode='not_valid_mode')
        yield cursor.close()
        yield self._cleanup()


__all__ = ["TestSSCursor"]


if __name__ == "__main__":
    import unittest
    unittest.main()
