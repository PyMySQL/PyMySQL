from io import StringIO
from unittest import TestCase
from pymysql.optionfile import Parser


__all__ = ["TestParser"]


_cfg_file = r"""
[default]
string = foo
quoted = "bar"
single_quoted = 'foobar'
skip-slave-start
"""


class TestParser(TestCase):
    def test_string(self):
        parser = Parser()
        parser.read_file(StringIO(_cfg_file))
        self.assertEqual(parser.get("default", "string"), "foo")
        self.assertEqual(parser.get("default", "quoted"), "bar")
        self.assertEqual(parser.get("default", "single_quoted"), "foobar")
