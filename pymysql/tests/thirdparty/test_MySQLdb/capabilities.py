""" Script to test database capabilities and the DB-API interface
    for functionality and memory leaks.

    Adapted from a script by M-A Lemburg.

"""
import sys
from time import time
import unittest

PY2 = sys.version_info[0] == 2


class DatabaseTest(unittest.TestCase):

    db_module = None
    connect_args = ()
    connect_kwargs = dict(use_unicode=True, charset="utf8mb4", binary_prefix=True)
    create_table_extra = "ENGINE=INNODB CHARACTER SET UTF8MB4"
    rows = 10
    debug = False

    def setUp(self):
        # db = self.connect()
        # self.connection = db
        # self.cursor = db.cursor()
        self.BLOBText = "".join([chr(i) for i in range(256)] * 100)
        if PY2:
            self.BLOBUText = unicode().join(unichr(i) for i in range(16834))
        else:
            self.BLOBUText = "".join(chr(i) for i in range(16834))
        data = bytearray(range(256)) * 16
        self.BLOBBinary = self.db_module.Binary(data)

    leak_test = False

    def connect(self):
        connection = self.db_module.connect(*self.connect_args, **self.connect_kwargs)
        # print("\n" + method_name + " " + str(connection.thread_id()))
        return connection

    # def tearDown(self):
    # self.connection.close()
    # if self.leak_test:
    #     import gc

    #     # del self.cursor
    #     orphans = gc.collect()
    #     self.assertFalse(
    #         orphans, "%d orphaned objects found after deleting cursor" % orphans
    #     )

    #     del self.connection
    #     orphans = gc.collect()
    #     self.assertFalse(
    #         orphans, "%d orphaned objects found after deleting connection" % orphans
    #     )

    def table_exists(self, cursor, name):
        try:
            cursor.execute("select * from %s where 1=0" % name)
        except Exception:
            return False
        else:
            return True

    def quote_identifier(self, ident):
        return '"%s"' % ident

    def new_table_name(self):
        connection = self.connect()
        with connection.cursor() as cursor:
            i = id(cursor)
            while True:
                name = self.quote_identifier("tb%08x" % i)
                if not self.table_exists(cursor, name):
                    connection.close()
                    return name
                i = i + 1

    def create_table(self, columndefs):

        """ Create a table using a list of column definitions given in
            columndefs.

            generator must be a function taking arguments (row_number,
            col_number) returning a suitable data object for insertion
            into the table.

        """
        self.table = self.new_table_name()
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.execute(
                "CREATE TABLE %s (%s) %s"
                % (self.table, ",\n".join(columndefs), self.create_table_extra)
            )
        connection.close()

    def check_data_integrity(self, columndefs, generator):
        # insert
        self.create_table(columndefs)
        insert_statement = "INSERT INTO %s VALUES (%s)" % (
            self.table,
            ",".join(["%s"] * len(columndefs)),
        )
        data = [
            [generator(i, j) for j in range(len(columndefs))] for i in range(self.rows)
        ]
        if self.debug:
            print(data)
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.executemany(insert_statement, data)
            connection.commit()
            # verify
        with connection.cursor() as cursor:
            cursor.execute("select * from %s" % self.table)
            l = cursor.fetchall()
            if self.debug:
                print(l)
            self.assertEqual(len(l), self.rows)
            try:
                for i in range(self.rows):
                    for j in range(len(columndefs)):
                        self.assertEqual(l[i][j], generator(i, j))
            finally:
                if not self.debug:
                    cursor.execute("drop table %s" % (self.table))
        connection.close()

    def test_transactions(self):
        columndefs = ("col1 INT", "col2 VARCHAR(255)")

        def generator(row, col):
            if col == 0:
                return row
            else:
                return ("%i" % (row % 10)) * 255

        self.create_table(columndefs)
        insert_statement = "INSERT INTO %s VALUES (%s)" % (
            self.table,
            ",".join(["%s"] * len(columndefs)),
        )
        data = [
            [generator(i, j) for j in range(len(columndefs))] for i in range(self.rows)
        ]
        connection = self.connect()
        with connection.cursor() as cursor:
            cursor.executemany(insert_statement, data)
            # verify
            connection.commit()
            cursor.execute("select * from %s" % self.table)
            l = cursor.fetchall()
            self.assertEqual(len(l), self.rows)
            for i in range(self.rows):
                for j in range(len(columndefs)):
                    self.assertEqual(l[i][j], generator(i, j))
            delete_statement = "delete from %s where col1=%%s" % self.table
            cursor.execute(delete_statement, (0,))
            cursor.execute("select col1 from %s where col1=%s" % (self.table, 0))
            l = cursor.fetchall()
            self.assertFalse(l, "DELETE didn't work")
            connection.rollback()
            cursor.execute("select col1 from %s where col1=%s" % (self.table, 0))
            l = cursor.fetchall()
            self.assertTrue(len(l) == 1, "ROLLBACK didn't work")
            cursor.execute("drop table %s" % (self.table))
        connection.close()

    def test_truncation(self):
        columndefs = ("col1 INT", "col2 VARCHAR(255)")

        def generator(row, col):
            if col == 0:
                return row
            else:
                return ("%i" % (row % 10)) * ((255 - self.rows // 2) + row)

        self.create_table(columndefs)
        insert_statement = "INSERT INTO %s VALUES (%s)" % (
            self.table,
            ",".join(["%s"] * len(columndefs)),
        )
        connection = self.connect()
        with connection.cursor() as cursor:
            try:
                cursor.execute(insert_statement, (0, "0" * 256))
            except Warning:
                if self.debug:
                    print(cursor.messages)
            except connection.DataError:
                pass
            else:
                self.fail(
                    "Over-long column did not generate warnings/exception with single insert"
                )

            connection.rollback()

            try:
                for i in range(self.rows):
                    data = []
                    for j in range(len(columndefs)):
                        data.append(generator(i, j))
                    cursor.execute(insert_statement, tuple(data))
            except Warning:
                if self.debug:
                    print(cursor.messages)
            except connection.DataError:
                pass
            else:
                self.fail(
                    "Over-long columns did not generate warnings/exception with execute()"
                )

            connection.rollback()

            try:
                data = [
                    [generator(i, j) for j in range(len(columndefs))]
                    for i in range(self.rows)
                ]
                cursor.executemany(insert_statement, data)
            except Warning:
                if self.debug:
                    print(cursor.messages)
            except connection.DataError:
                pass
            else:
                self.fail(
                    "Over-long columns did not generate warnings/exception with executemany()"
                )

            connection.rollback()
            cursor.execute("drop table %s" % (self.table))

    def test_CHAR(self):
        # Character data
        def generator(row, col):
            return ("%i" % ((row + col) % 10)) * 255

        self.check_data_integrity(("col1 char(255)", "col2 char(255)"), generator)

    def test_INT(self):
        # Number data
        def generator(row, col):
            return row * row

        self.check_data_integrity(("col1 INT",), generator)

    def test_DECIMAL(self):
        # DECIMAL
        def generator(row, col):
            from decimal import Decimal

            return Decimal("%d.%02d" % (row, col))

        self.check_data_integrity(("col1 DECIMAL(5,2)",), generator)

    def test_DATE(self):
        ticks = time()

        def generator(row, col):
            return self.db_module.DateFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(("col1 DATE",), generator)

    def test_TIME(self):
        ticks = time()

        def generator(row, col):
            return self.db_module.TimeFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(("col1 TIME",), generator)

    def test_DATETIME(self):
        ticks = time()

        def generator(row, col):
            return self.db_module.TimestampFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(("col1 DATETIME",), generator)

    def test_TIMESTAMP(self):
        ticks = time()

        def generator(row, col):
            return self.db_module.TimestampFromTicks(ticks + row * 86400 - col * 1313)

        self.check_data_integrity(("col1 TIMESTAMP",), generator)

    def test_fractional_TIMESTAMP(self):
        ticks = time()

        def generator(row, col):
            return self.db_module.TimestampFromTicks(
                ticks + row * 86400 - col * 1313 + row * 0.7 * col / 3.0
            )

        self.check_data_integrity(("col1 TIMESTAMP",), generator)

    def test_LONG(self):
        def generator(row, col):
            if col == 0:
                return row
            else:
                return self.BLOBUText  # 'BLOB Text ' * 1024

        self.check_data_integrity(("col1 INT", "col2 LONG"), generator)

    def test_TEXT(self):
        def generator(row, col):
            if col == 0:
                return row
            else:
                return self.BLOBUText[:5192]  # 'BLOB Text ' * 1024

        self.check_data_integrity(("col1 INT", "col2 TEXT"), generator)

    def test_LONG_BYTE(self):
        def generator(row, col):
            if col == 0:
                return row
            else:
                return self.BLOBBinary  # 'BLOB\000Binary ' * 1024

        self.check_data_integrity(("col1 INT", "col2 LONG BYTE"), generator)

    def test_BLOB(self):
        def generator(row, col):
            if col == 0:
                return row
            else:
                return self.BLOBBinary  # 'BLOB\000Binary ' * 1024

        self.check_data_integrity(("col1 INT", "col2 BLOB"), generator)
