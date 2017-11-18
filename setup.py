import sys
from distutils.core import setup, Command
from distutils.extension import Extension

try:
    from Cython.Build import cythonize
    ext_modules = cythonize([
        Extension("cymysql.packet", ["cymysql/packet.pyx"]),

        Extension("cymysql.charset", ["cymysql/charset.py"]),
        Extension("cymysql.converters", ["cymysql/converters.py"]),
        Extension("cymysql.connections", ["cymysql/connections.py"]),
        Extension("cymysql.cursors", ["cymysql/cursors.py"]),
        Extension("cymysql.err", ["cymysql/err.py"]),
        Extension("cymysql.times", ["cymysql/times.py"]),
    ])
except ImportError:
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

cmdclass = {'test': TestCommand}

version_tuple = __import__('cymysql').VERSION

if version_tuple[2] is not None:
    version = "%d.%d.%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

classifiers = [
    'Development Status :: 4 - Beta',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Programming Language :: Python :: 2',
    'Programming Language :: Python :: 3',
    'Topic :: Database',
]

setup(
    name = "cymysql",
    version = version,
    url = 'https://github.com/nakagami/CyMySQL/',
    classifiers=classifiers,
    keywords=['MySQL'],
    author = 'Yutaka Matsubara',
    author_email = 'yutaka.matsubara@gmail.com',
    maintainer = 'Hajime Nakagami',
    maintainer_email = 'nakagami@gmail.com',
    description = 'Python MySQL Driver using Cython',
    long_description=open('README.rst').read(),
    license = "MIT",
    packages = ['cymysql', 'cymysql.constants', 'cymysql.tests'],
    cmdclass = cmdclass,
    ext_modules = ext_modules,
)
