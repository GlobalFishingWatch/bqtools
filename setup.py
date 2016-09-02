#!/usr/bin/env python


"""
Setup script for bqtools
"""

# This setup file derived from bqapi setup.py:
# https://github.com/GlobalFishingWatch/bqapi/blob/master/setup.py


import os

from setuptools import find_packages
from setuptools import setup


with open('README.rst') as f:
    readme = f.read().strip()


def parse_dunder_line(string):

    """
    Take a line like:

        "__version__ = '0.0.8'"

    and turn it into a tuple:

        ('__version__', '0.0.8')

    Not very fault tolerant.
    """

    # Split the line and remove outside quotes
    variable, value = (s.strip() for s in string.split('=')[:2])
    value = value[1:-1].strip()
    return variable, value


with open(os.path.join('bqtools', '__init__.py')) as f:
    dunders = dict((
        parse_dunder_line(line) for line in f if line.strip().startswith('__')))
    version = dunders['__version__']
    author = dunders['__author__']
    email = dunders['__email__']
    source = dunders['__source__']


setup(
    name='bqtools',
    author=author,
    author_email=email,
    classifiers=[
        'Topic :: Utilities',
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
#         'License :: OSI Approved :: BSD License', # XXX add later
        'Topic :: Software Development :: Libraries',
#         'Programming Language :: Python :: 2.7', # Check
#         'Programming Language :: Python :: 3.3',
#         'Programming Language :: Python :: 3.4',
#         'Programming Language :: Python :: 3.5',
#         'Programming Language :: Python :: Implementation :: PyPy',
    ],
    description="Experimental BigQuery tools for Python",
    include_package_data=True,
    install_requires=[
        'oauth2client',
        'google-api-python-client',
#         'click'
    ],
#     extras_require={
#         'dev': [
#             'pytest',
#             'pytest-cov',
#             'coveralls',
#         ],
#     },
#     entry_points="""
#         [console_scripts]
#         bqapi=bqapi.cli:main
#     """,
    keywords='BigQuery Python',
#     license="Apache 2.0",
    long_description=readme,
    packages=find_packages(exclude=['tests']),
    url=source,
    version=version,
    zip_safe=True
)
