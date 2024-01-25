#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup, find_packages

import src

if __name__ == '__main__':

    with open('README.md', 'r') as f:
        long_description = f.read()

    setup(
        name=src.__name__,
        version=src.__version__,
        description=src.__description__,
        long_description=long_description,
        long_description_content_type='text/markdown',
        author=src.__author__,
        python_requires=src.__python_requires__,
        package_dir={'': 'src'},
        packages=find_packages('src', include=[
            'opin_lib_testes_conexoes*'
        ], exclude=[
            'tests*'
        ]),
        install_requires=[
            'jproperties==2.1.1'
        ],
        include_package_data=True,
        package_data={
            # If any package contains *.ini, *.json and *.properties files, include them
            '': ['*.ini', '*.json', '*.properties'],
        },
        setup_requires=['pytest-runner'],
        tests_require=['pytest', 'pyspark-test', 'databricks-test'],
        test_suite='tests'
    )
