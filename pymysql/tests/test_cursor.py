import warnings

from pymysql.tests import base
import pymysql.cursors

class CursorTest(base.PyMySQLTestCase):
    def setUp(self):
        super(CursorTest, self).setUp()

        conn = self.connections[0]
        self.safe_create_table(
            conn,
            "test", "create table test (data varchar(10))",
        )
        cursor = conn.cursor()
        cursor.execute(
            "insert into test (data) values "
            "('row1'), ('row2'), ('row3'), ('row4'), ('row5')")
        cursor.close()
        self.test_connection = pymysql.connect(**self.databases[0])
        self.addCleanup(self.test_connection.close)

    def test_cleanup_rows_unbuffered(self):
        conn = self.test_connection
        cursor = conn.cursor(pymysql.cursors.SSCursor)

        cursor.execute("select * from test as t1, test as t2")
        for counter, row in enumerate(cursor):
            if counter > 10:
                break

        del cursor
        self.safe_gc_collect()

        c2 = conn.cursor()

        with warnings.catch_warnings(record=True) as log:
            warnings.filterwarnings("always")

            c2.execute("select 1")

        self.assertGreater(len(log), 0)
        self.assertEqual(
            "Previous unbuffered result was left incomplete",
            str(log[-1].message))
        self.assertEqual(
            c2.fetchone(), (1,)
        )
        self.assertIsNone(c2.fetchone())

    def test_cleanup_rows_buffered(self):
        conn = self.test_connection
        cursor = conn.cursor(pymysql.cursors.Cursor)

        cursor.execute("select * from test as t1, test as t2")
        for counter, row in enumerate(cursor):
            if counter > 10:
                break

        del cursor
        self.safe_gc_collect()

        c2 = conn.cursor()

        c2.execute("select 1")

        self.assertEqual(
            c2.fetchone(), (1,)
        )
        self.assertIsNone(c2.fetchone())

