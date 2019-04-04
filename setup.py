from __future__ import absolute_import

########################################################################
#       File based on https://github.com/Blosc/bcolz
########################################################################
#
# License: BSD
# Created: October 5, 2015
#       Author:  Carst Vaartjes - cvaartjes@visualfabriq.com
#
########################################################################
import codecs
import os

from setuptools import setup, Extension, find_packages
from os.path import abspath
from sys import version_info as v
from setuptools.command.build_ext import build_ext as _build_ext


# Check this Python version is supported
if any([v < (2, 6), (3,) < v < (3, 5)]):
    raise Exception("Unsupported Python version %d.%d. Requires Python >= 2.7 "
                    "or >= 3.5." % v[:2])


class build_ext(_build_ext):
    def finalize_options(self):
        _build_ext.finalize_options(self)
        # Prevent numpy from thinking it is still in its setup process:
        __builtins__.__NUMPY_SETUP__ = False
        import numpy
        self.include_dirs.append(numpy.get_include())


HERE = os.path.abspath(os.path.dirname(__file__))

def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


def get_version():
    version = {}
    with open("bqueryd/version.py") as fp:
        exec (fp.read(), version)
    return version


# Sources & libraries
inc_dirs = [abspath('bqueryd')]
try:
    import numpy as np
    inc_dirs.append(np.get_include())
except ImportError as e:
    pass
lib_dirs = []
libs = []
def_macros = []
sources = []

cmdclass = {'build_ext': build_ext}

optional_libs = ['numexpr>=2.6.9']
install_requires = [
    'bquery>=0.2.10',
    'pyzmq>=17.1.2',
    'redis>=3.0.1',
    'boto3>=1.9.82',
    'smart_open>=1.8.0',
    'netifaces>=0.10.9',
    'configobj>=5.0.6',
    'psutil>=5.0.0',
]
setup_requires = []
tests_requires = [
    'pandas>=0.23.0',
    'pytest>=4.0.0',
    'pytest-cov>=2.6.0',
    'codacy-coverage>=1.3.7',
]
extras_requires = []
ext_modules = []
package_data = {}
classifiers = [
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ]

setup(
    name="bqueryd",
    version=get_version()['__version__'],
    description='A distribution framework for Bquery',
    long_description=read("README.md"),
    classifiers=classifiers,
    author='Carst Vaartjes',
    author_email='cvaartjes@visualfabriq.com',
    maintainer='Carst Vaartjes',
    maintainer_email='cvaartjes@visualfabriq.com',
    url='https://github.com/visualfabriq/bqueryd',
    license='GPL2',
    platforms=['any'],
    ext_modules=ext_modules,
    cmdclass=cmdclass,
    install_requires=install_requires,
    setup_requires=setup_requires,
    tests_require=tests_requires,
    extras_require=dict(
        optional=extras_requires,
        test=tests_requires
    ),
    packages=find_packages(),
    package_data=package_data,
    include_package_data=True,
    zip_safe=True,
    entry_points={
        'console_scripts': [
            'bqueryd = bqueryd.node:main'
        ]
    },
)

