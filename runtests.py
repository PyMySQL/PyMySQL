#!/usr/bin/env python
try:
    import unittest2 as unittest
except ImportError:
    import unittest

import pymysql.tests
unittest.main(pymysql.tests)
