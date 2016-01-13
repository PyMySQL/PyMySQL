import sys

PY2 = sys.version_info[0] == 2
PYPY = hasattr(sys, 'pypy_translation_info')
JYTHON = sys.platform.startswith('java')
IRONPYTHON = sys.platform == 'cli'
CPYTHON = not PYPY and not JYTHON and not IRONPYTHON

if PY2:
    import __builtin__
    range_type = xrange
    text_type = unicode
    long_type = long
    str_type = basestring
    unichr = __builtin__.unichr
else:
    range_type = range
    text_type = str
    long_type = int
    str_type = str
    unichr = chr
