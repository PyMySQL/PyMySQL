#!/usr/bin/env python
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import sys
if not hasattr(sys, 'pypy_version_info'):
    import atexit
    import gc
    gc.set_debug(gc.DEBUG_UNCOLLECTABLE)

    @atexit.register
    def report_uncollectable():
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
