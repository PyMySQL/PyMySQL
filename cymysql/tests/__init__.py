from cymysql.tests.test_issues import *
from cymysql.tests.test_example import *
from cymysql.tests.test_basic import *
from cymysql.tests.test_DictCursor import *

import sys
if sys.version_info[0] == 2:
    # MySQLdb tests were designed for Python 3
    from cymysql.tests.thirdparty import *

if __name__ == "__main__":
    import unittest
    unittest.main()
