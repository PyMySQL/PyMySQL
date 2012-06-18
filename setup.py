try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup, Command

import sys

class TestCommand(Command):
    user_options = [ ]

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        '''
        Finds all the tests modules in tests/, and runs them.
        '''
        from pymysql import tests
        import unittest
        unittest.main(tests, argv=sys.argv[:1])

version_tuple = __import__('pymysql').VERSION

if version_tuple[2] is not None:
    version = "%d.%d_%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

setup(
    name = "PyMySQL",
    version = version,
    url = 'https://github.com/petehunt/PyMySQL/',
    author = 'yutaka.matsubara',
    author_email = 'yutaka.matsubara@gmail.com',
    maintainer = 'Pete Hunt',
    maintainer_email = 'floydophone@gmail.com',
    description = 'Pure Python MySQL Driver ',
    license = "MIT",
    packages = ['pymysql', 'pymysql.constants', 'pymysql.tests'],
    cmdclass = {'test': TestCommand},
)
