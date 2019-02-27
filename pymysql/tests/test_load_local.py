from pymysql import cursors, OperationalError, Warning
from pymysql.tests import base

import os

__all__ = ["TestLoadLocal"]

class CountDown(object):

    def read(self, max_packet_size):
        if self._start:
            self._start = self._start - 1
            return (str(self._start) + "\n").encode()
        return None

    def __getitem__(self, obj):
        try:
            self._start = int(obj)
            return self
        except ValueError:
            raise KeyError('Integers only')

class TestLoadLocal(base.PyMySQLTestCase):
    def test_no_file(self):
        """Test load local infile when the file does not exist"""
        conn = self.connect()
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        try:
            self.assertRaises(
                OperationalError,
                c.execute,
                ("LOAD DATA LOCAL INFILE 'no_data.txt' INTO TABLE "
                 "test_load_local fields terminated by ','")
            )
        finally:
            c.execute("DROP TABLE test_load_local")
            c.close()

    def test_load_file(self):
        """Test load local infile with a valid file"""
        conn = self.connect()
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'data',
                                'load_local_data.txt')
        try:
            c.execute(
                ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
                 "test_load_local FIELDS TERMINATED BY ','").format(filename)
            )
            c.execute("SELECT COUNT(*) FROM test_load_local")
            self.assertEqual(22749, c.fetchone()[0])
        finally:
            c.execute("DROP TABLE test_load_local")

    def test_unbuffered_load_file(self):
        """Test unbuffered load local infile with a valid file"""
        conn = self.connect()
        c = conn.cursor(cursors.SSCursor)
        c.execute("CREATE TABLE test_load_local (a INTEGER, b INTEGER)")
        filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                'data',
                                'load_local_data.txt')
        try:
            c.execute(
                ("LOAD DATA LOCAL INFILE '{0}' INTO TABLE " +
                 "test_load_local FIELDS TERMINATED BY ','").format(filename)
            )
            c.execute("SELECT COUNT(*) FROM test_load_local")
            self.assertEqual(22749, c.fetchone()[0])
        finally:
            c.close()
            conn.close()
            conn.connect()
            c = conn.cursor()
            c.execute("DROP TABLE test_load_local")

    def test_generated_load_file(self):
        """Test generated load local infile"""
        conn = self.connect(local_infile=CountDown())
        c = conn.cursor(cursors.SSCursor)
        c.execute("CREATE TABLE test_load_local (a INTEGER)")

        try:
            c.execute(
                "LOAD DATA LOCAL INFILE '99' INTO TABLE test_load_local"
            )
            c.execute("SELECT COUNT(*) FROM test_load_local")
            self.assertEqual(99, c.fetchone()[0])
        finally:
            c.close()
            conn.close()
            conn.connect()
            c = conn.cursor()
            c.execute("DROP TABLE test_load_local")

    def test_generated_no_file(self):
        """Test load local infile when the generator rejects the filename"""
        conn = self.connect(local_infile=CountDown())
        c = conn.cursor()
        c.execute("CREATE TABLE test_load_local (a INTEGER)")
        try:
            self.assertRaises(
                OperationalError,
                c.execute,
                ("LOAD DATA LOCAL INFILE 'not_an_int' INTO TABLE test_load_local")
            )
        finally:
            c.execute("DROP TABLE test_load_local")
            c.close()

if __name__ == "__main__":
    import unittest
    unittest.main()
