import sys

PY2 = sys.version_info[0] == 2
PYPY = hasattr(sys, 'pypy_translation_info')

if PY2:
    range_type = xrange
    text_type = unicode
    long_type = long
    str_type = basestring
else:
    range_type = range
    text_type = str
    long_type = int
    str_type = str
