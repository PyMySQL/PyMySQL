#!/usr/bin/env python
from setuptools import setup, find_packages, Extension

version = "1.0.2"

with open("./README.rst", encoding="utf-8") as f:
    readme = f.read()

setup(
    name="PyMySQL",
    version=version,
    url="https://github.com/PyMySQL/PyMySQL/",
    project_urls={
        "Documentation": "https://pymysql.readthedocs.io/",
    },
    description="Pure Python MySQL Driver",
    long_description=readme,
    packages=find_packages(exclude=["tests*", "pymysql.tests*"]),
    python_requires=">=3.7",
    extras_require={
        "rsa": ["cryptography"],
        "ed25519": ["PyNaCl>=1.4.0"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Database",
    ],
    ext_modules=[
          Extension(
              '_pymysqlsv', ['src/accel.c'], py_limited_api=True,
          ),
    ],
    keywords="MySQL",
)
