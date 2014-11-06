from pymysql.tests.test_issues import *
from pymysql.tests.test_basic import *
from pymysql.tests.test_nextset import *
from pymysql.tests.test_DictCursor import *
from pymysql.tests.test_connection import TestConnection
from pymysql.tests.test_SSCursor import *

from pymysql.tests.thirdparty import *

if __name__ == "__main__":
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
    unittest.main()
