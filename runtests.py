#!/usr/bin/env python
import unittest2

from pymysql._compat import PYPY, JYTHON, IRONPYTHON

if not (PYPY or JYTHON or IRONPYTHON):
    import atexit
    import gc
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    @atexit.register
    def report_uncollectable():
        import gc
        if not gc.garbage:
            print("No garbages!")
            return
        print('uncollectable objects')
        for obj in gc.garbage:
            print(obj)
            if hasattr(obj, '__dict__'):
                print(obj.__dict__)
            for ref in gc.get_referrers(obj):
                print("referrer:", ref)
            print('---')

import pymysql.tests
unittest2.main(pymysql.tests, verbosity=2)
