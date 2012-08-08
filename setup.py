try:
    from setuptools import setup, Command
except ImportError:
    from distutils.core import setup, Command

import sys, os, re

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

v = open(os.path.join(os.path.dirname(__file__), 'pymysql', '__init__.py'))
version = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

extra = {}
if sys.version_info >= (3,):
    extra['use_2to3'] = True

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
    **extra
)
