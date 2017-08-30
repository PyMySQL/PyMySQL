from pymysql.optionfile import Parser
from unittest import TestCase
from pymysql._compat import PY2

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO


__all__ = ['TestParser']


_cfg_file = (r"""
[default]
string = foo
quoted = "bar"
single_quoted = 'foobar'
skip-slave-start
""")


class TestParser(TestCase):

    def test_string(self):
        parser = Parser()
        if PY2:
            parser.readfp(StringIO(_cfg_file))
        else:
            parser.read_file(StringIO(_cfg_file))
        self.assertEqual(parser.get("default", "string"), "foo")
        self.assertEqual(parser.get("default", "quoted"), "bar")
        self.assertEqual(parser.get("default", "single_quoted"), "foobar")
