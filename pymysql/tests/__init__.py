from pymysql.tests.test_issues import *
from pymysql.tests.test_basic import *
from pymysql.tests.test_DictCursor import *

import sys
from pymysql.tests.thirdparty import *

if __name__ == "__main__":
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
    unittest.main()
