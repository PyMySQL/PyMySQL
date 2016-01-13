from unittest import TestCase

from pymysql._compat import PY2
from pymysql import converters


__all__ = ["TestConverter"]


class TestConverter(TestCase):

    def test_escape_string(self):
        self.assertEqual(
            converters.escape_string(u"foo\nbar"),
            u"foo\\nbar"
        )

    if PY2:
        def test_escape_string_bytes(self):
            self.assertEqual(
                converters.escape_string(b"foo\nbar"),
                b"foo\\nbar"
            )
