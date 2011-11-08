
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version_tuple = __import__('pymysql').VERSION

if version_tuple[2] is not None:
    version = "%d.%d_%s" % version_tuple
else:
    version = "%d.%d" % version_tuple[:2]

setup(
    name = "PyMySQL",
    version = version,
    url = 'http://code.google.com/p/pymysql',
    author = 'yutaka.matsubara',
    author_email = 'yutaka.matsubara@gmail.com',
    maintainer = 'Pete Hunt',
    maintainer_email = 'floydophone@gmail.com',
    description = 'Pure Python MySQL Driver ',
    license = "MIT",
    packages = ['pymysql', 'pymysql.constants', 'pymysql.tests']
)
