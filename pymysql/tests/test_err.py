import unittest2

from pymysql import err


__all__ = ["TestRaiseException"]


class TestRaiseException(unittest2.TestCase):

    def test_raise_mysql_exception(self):
        data = b"\xff\x15\x04Access denied"
        with self.assertRaises(err.OperationalError) as cm:
            err.raise_mysql_exception(data)
        self.assertEqual(cm.exception.args, (1045, 'Access denied'))

    def test_raise_mysql_exception_client_protocol_41(self):
        data = b"\xff\x15\x04#28000Access denied"
        with self.assertRaises(err.OperationalError) as cm:
            err.raise_mysql_exception(data)
        self.assertEqual(cm.exception.args, (1045, 'Access denied'))
