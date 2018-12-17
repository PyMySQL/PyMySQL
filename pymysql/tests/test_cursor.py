import warnings

from pymysql.tests import base
import pymysql.cursors

class CursorTest(base.PyMySQLTestCase):
    def setUp(self):
        super(CursorTest, self).setUp()

        conn = self.connect()
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

        c2.execute("select 1")
        self.assertEqual(c2.fetchone(), (1,))
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

    def test_executemany(self):
        conn = self.test_connection
        cursor = conn.cursor(pymysql.cursors.Cursor)

        m = pymysql.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%s, %s)")
        self.assertIsNotNone(m, 'error parse %s')
        self.assertEqual(m.group(3), '', 'group 3 not blank, bug in RE_INSERT_VALUES?')

        m = pymysql.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id)s, %(name)s)")
        self.assertIsNotNone(m, 'error parse %(name)s')
        self.assertEqual(m.group(3), '', 'group 3 not blank, bug in RE_INSERT_VALUES?')

        m = pymysql.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s)")
        self.assertIsNotNone(m, 'error parse %(id_name)s')
        self.assertEqual(m.group(3), '', 'group 3 not blank, bug in RE_INSERT_VALUES?')

        m = pymysql.cursors.RE_INSERT_VALUES.match("INSERT INTO TEST (ID, NAME) VALUES (%(id_name)s, %(name)s) ON duplicate update")
        self.assertIsNotNone(m, 'error parse %(id_name)s')
        self.assertEqual(m.group(3), ' ON duplicate update', 'group 3 not ON duplicate update, bug in RE_INSERT_VALUES?')

        # https://github.com/PyMySQL/PyMySQL/pull/597
        m = pymysql.cursors.RE_INSERT_VALUES.match("INSERT INTO bloup(foo, bar)VALUES(%s, %s)")
        assert m is not None

        # cursor._executed must bee "insert into test (data) values (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)"
        # list args
        data = range(10)
        cursor.executemany("insert into test (data) values (%s)", data)
        self.assertTrue(cursor._executed.endswith(b",(7),(8),(9)"), 'execute many with %s not in one query')

        # dict args
        data_dict = [{'data': i} for i in range(10)]
        cursor.executemany("insert into test (data) values (%(data)s)", data_dict)
        self.assertTrue(cursor._executed.endswith(b",(7),(8),(9)"), 'execute many with %(data)s not in one query')

        # %% in column set
        cursor.execute("""\
            CREATE TABLE percent_test (
                `A%` INTEGER,
                `B%` INTEGER)""")
        try:
            q = "INSERT INTO percent_test (`A%%`, `B%%`) VALUES (%s, %s)"
            self.assertIsNotNone(pymysql.cursors.RE_INSERT_VALUES.match(q))
            cursor.executemany(q, [(3, 4), (5, 6)])
            self.assertTrue(cursor._executed.endswith(b"(3, 4),(5, 6)"), "executemany with %% not in one query")
        finally:
            cursor.execute("DROP TABLE IF EXISTS percent_test")
