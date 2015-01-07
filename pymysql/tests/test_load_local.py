from pymysql.tests import base
from pymysql.err import OperationalError

import os

__all__ = ["TestLoadLocal"]


class TestLoadLocal(base.PyMySQLTestCase):
    def test_no_file(self):
        """Test load local infile when the file does not exist"""
        conn = self.connections[2]
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        try:
            with self.assertRaisesRegexp(
                    OperationalError, "Can't find file 'no_data.txt'"):
                c.execute(
                    "LOAD DATA LOCAL INFILE 'no_data.txt' INTO TABLE " +
                    "test_load_local fields terminated by ','"
                )
        finally:
            c.execute("DROP TABLE test_load_local")
            c.close()

    def test_load_file(self):
        """Test load local infile with a valid file"""
        conn = self.connections[2]
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'data',
                                'load_local_data.txt')
        try:
            c.execute(
                ("LOAD DATA LOCAL INFILE '{}' INTO TABLE " +
                 "test_load_local FIELDS TERMINATED BY ','").format(filename)
            )
            c.execute("SELECT COUNT(*) FROM test_load_local")
            self.assertEquals(22749, c.fetchone()[0])
        finally:
            c.execute("DROP TABLE test_load_local")

    def test_load_warnings(self):
        """Test load local infile produces the appropriate warnings"""
        import sys
        from StringIO import StringIO

        saved_stdout = sys.stdout
        out = StringIO()
        sys.stdout = out
        conn = self.connections[2]
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'data',
                                'load_local_warn_data.txt')

        try:
            c.execute(
                ("LOAD DATA LOCAL INFILE '{}' INTO TABLE " +
                 "test_load_local FIELDS TERMINATED BY ','").format(filename)
            )
            output = out.getvalue().strip().split('\n')
            self.assertEquals(2, len(output))
            self.assertEqual(
                ("  Warning: Incorrect integer value: '' for column 'a' at " +
                 "row 8 in file '{}'").format(filename),
                output[1]
            )

        finally:
            sys.stdout = saved_stdout
            c.execute("DROP TABLE test_load_local")


if __name__ == "__main__":
    import unittest
    unittest.main()
