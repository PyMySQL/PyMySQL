import unittest

import cymysql
_mysql = cymysql
from cymysql.constants import FIELD_TYPE
from cymysql.tests import base


class TestDBAPISet(unittest.TestCase):
    def test_set_equality(self):
        self.assertTrue(cymysql.STRING == cymysql.STRING)

    def test_set_inequality(self):
        self.assertTrue(cymysql.STRING != cymysql.NUMBER)

    def test_set_equality_membership(self):
        self.assertTrue(FIELD_TYPE.VAR_STRING == cymysql.STRING)

    def test_set_inequality_membership(self):
        self.assertTrue(FIELD_TYPE.DATE != cymysql.STRING)


class CoreModule(unittest.TestCase):
    """Core _mysql module features."""

    def test_NULL(self):
        """Should have a NULL constant."""
        self.assertEqual(_mysql.NULL, 'NULL')

    def test_version(self):
        """Version information sanity."""
        self.assertTrue(isinstance(_mysql.__version__, str))


class CoreAPI(unittest.TestCase):
    """Test _mysql interaction internals."""

    def setUp(self):
        kwargs = base.PyMySQLTestCase.databases[0].copy()
        kwargs["read_default_file"] = "~/.my.cnf"
        self.conn = _mysql.connect(**kwargs)

    def tearDown(self):
        self.conn.close()

    def test_thread_id(self):
        tid = self.conn.thread_id()
        self.assertTrue(isinstance(tid, int),
                        "thread_id didn't return an int.")

        self.assertRaises(TypeError, self.conn.thread_id, ('evil',),
                          "thread_id shouldn't accept arguments.")

    def test_affected_rows(self):
        self.assertEquals(self.conn.affected_rows(), 0,
                          "Should return 0 before we do anything.")


    #def test_debug(self):
        ## FIXME Only actually tests if you lack SUPER
        #self.assertRaises(cymysql.OperationalError,
                          #self.conn.dump_debug_info)

    def test_charset_name(self):
        self.assertTrue(isinstance(self.conn.character_set_name(), str),
                        "Should return a string.")

    def test_host_info(self):
        self.assertTrue(isinstance(self.conn.get_host_info(), str),
                        "Should return a string.")

    def test_proto_info(self):
        self.assertTrue(isinstance(self.conn.get_proto_info(), int),
                        "Should return an int.")

    def test_server_info(self):
        self.assertTrue(isinstance(self.conn.get_server_info(), basestring),
                        "Should return an str.")

if __name__ == "__main__":
    unittest.main()
