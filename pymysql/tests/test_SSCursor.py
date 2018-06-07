import sys

try:
    from pymysql.tests import base
    import pymysql.cursors
    from pymysql.constants import CLIENT
except Exception:
    # For local testing from top-level directory, without installing
    sys.path.append('../pymysql')
    from pymysql.tests import base
    import pymysql.cursors
    from pymysql.constants import CLIENT

class TestSSCursor(base.PyMySQLTestCase):
    def test_SSCursor(self):
        affected_rows = 18446744073709551615

        conn = self.connect(client_flag=CLIENT.MULTI_STATEMENTS)
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

        cursor = conn.cursor(pymysql.cursors.SSCursor)

        # Create table
        cursor.execute('CREATE TABLE tz_data ('
            'region VARCHAR(64),'
            'zone VARCHAR(64),'
            'name VARCHAR(64))')

        conn.begin()
        # Test INSERT
        for i in data:
            cursor.execute('INSERT INTO tz_data VALUES (%s, %s, %s)', i)
            self.assertEqual(conn.affected_rows(), 1, 'affected_rows does not match')
        conn.commit()

        # Test fetchone()
        iter = 0
        cursor.execute('SELECT * FROM tz_data')
        while True:
            row = cursor.fetchone()
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
            self.assertEqual((row in data), True,
                'Row not found in source data')

        # Test fetchall
        cursor.execute('SELECT * FROM tz_data')
        self.assertEqual(len(cursor.fetchall()), len(data),
            'fetchall failed. Number of rows does not match')

        # Test fetchmany
        cursor.execute('SELECT * FROM tz_data')
        self.assertEqual(len(cursor.fetchmany(2)), 2,
            'fetchmany failed. Number of rows does not match')

        # So MySQLdb won't throw "Commands out of sync"
        while True:
            res = cursor.fetchone()
            if res is None:
                break

        # Test update, affected_rows()
        cursor.execute('UPDATE tz_data SET zone = %s', ['Foo'])
        conn.commit()
        self.assertEqual(cursor.rowcount, len(data),
            'Update failed. affected_rows != %s' % (str(len(data))))

        # Test executemany
        cursor.executemany('INSERT INTO tz_data VALUES (%s, %s, %s)', data)
        self.assertEqual(cursor.rowcount, len(data),
            'executemany failed. cursor.rowcount != %s' % (str(len(data))))

        # Test multiple datasets
        cursor.execute('SELECT 1; SELECT 2; SELECT 3')
        self.assertListEqual(list(cursor), [(1, )])
        self.assertTrue(cursor.nextset())
        self.assertListEqual(list(cursor), [(2, )])
        self.assertTrue(cursor.nextset())
        self.assertListEqual(list(cursor), [(3, )])
        self.assertFalse(cursor.nextset())

        cursor.execute('DROP TABLE IF EXISTS tz_data')
        cursor.close()

__all__ = ["TestSSCursor"]

if __name__ == "__main__":
    import unittest
    unittest.main()
