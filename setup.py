import sys
from distutils.core import setup, Command
from distutils.extension import Extension

try:
    from Cython.Distutils import build_ext
    cmdclass = {'build_ext': build_ext}
    ext_modules = [Extension("cymysql.packetx", ["cymysql/packetx.pyx"])],
except ImportError:
    cmdclass = {}
    ext_modules = None


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
        from cymysql import tests
        import unittest
        unittest.main(tests, argv=sys.argv[:1])

cmdclass['test'] = TestCommand

version_tuple = __import__('cymysql').VERSION

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
    packages = ['cymysql', 'cymysql.constants', 'cymysql.tests'],
    cmdclass = cmdclass,
    ext_modules = ext_modules,
)
