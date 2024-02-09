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
        python_requires=">=3.8.5",
        package_dir={'': 'src'},
        packages=find_packages('src', include=[
            'opin_lib_cap_ingestao_sro_dados*'
        ], exclude=[
            'tests*'
        ]),
        install_requires=[
            'jproperties==2.1.1'
            # 'opencensus-ext-azure==1.0.5'
        ],
        include_package_data=True,
        package_data={
            # If any package contains *.ini, *.json and *.properties files, include them
            '': ['*.ini', '*.json', '*.properties'],
        },
        setup_requires=['pytest-runner'],
        tests_require=['pytest', 'pyspark-test', 'databricks-test'],
        test_suite='tests',
    )