#!/usr/bin/env python
from setuptools import setup, find_packages

setup(
    name="tap-companies-house",
    version="1.0.1",
    description="Singer.io tap for extracting data from the Companies House API and Bulk data",
    author="RFA",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_companies_house"],
    install_requires=[
        "setuptools-rust",
        "singer-python>=5.0.12",
        "pyOpenSSL",
        "cryptography",
        "urllib3[secure]",
        "requests",
        "tqdm",
        "selenium",
        "chromedriver-binary",
        "pandas",
        "webdriver-manager"
    ],
    entry_points='''
    [console_scripts]
    tap-companies-house=tap_companies_house:main
    ''',
    packages=find_packages(),
    package_data={
        'tap_companies_house': [
            'schemas/*.json', 'files/*.zip'
        ]
    })
