#!/usr/bin/env python

from setuptools import find_packages
from setuptools import setup

setup(
    name="migration-tools",
    version='0.1.0',
    author='engineering@globalfishingwatch.org',
    packages=find_packages(exclude=['test*.*', 'tests']),
    entry_points={
        'console_scripts': [
            'migration_tools = src.cli:main',
        ]
    },
)
