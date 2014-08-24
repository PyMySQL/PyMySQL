from .test_MySQLdb import *

if __name__ == "__main__":
    try:
        import unittest2 as unittest
    except ImportError:
        import unittest
    unittest.main()
