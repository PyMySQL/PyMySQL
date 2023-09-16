import sys

from cymysql.tests.test_issues import * # noqa
from cymysql.tests.test_example import * # noqa
from cymysql.tests.test_basic import * # noqa
from cymysql.tests.test_DictCursor import * # noqa
if sys.version_info[0] > 2:
    from cymysql.tests.test_async import * # noqa


if __name__ == "__main__":
    import unittest
    unittest.main()
