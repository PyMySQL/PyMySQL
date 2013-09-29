import pymysql
from pymysql.tests import base


class TestConnection(base.PyMySQLTestCase):
    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = pymysql.connect(**arg)

    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = self.connections[0].cursor()
        cur.execute("SELECT @@max_allowed_packet")
        if cur.fetchone()[0] < 16*1024*1024 + 10:
            print("Set max_allowed_packet to bigger than 17MB")
            return
        t = 'a' * (16*1024*1024)
        cur.execute("SELECT '" + t + "'")
        assert cur.fetchone()[0] == t


if __name__ == "__main__":
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
    unittest.main()
