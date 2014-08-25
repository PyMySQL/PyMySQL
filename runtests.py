#!/usr/bin/env python
import unittest

from tornado_mysql import PYPY, JYTHON, IRONPYTHON

if not (PYPY or JYTHON or IRONPYTHON):
    import atexit
    import gc
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    @atexit.register
    def report_uncollectable():
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
unittest.main(pymysql.tests)
