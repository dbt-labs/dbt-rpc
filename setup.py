#!/usr/bin/env python
import os
from setuptools import setup, find_namespace_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


package_name = "dbt-rpc"
package_version = "0.21.0"
description = """ TODO """


setup(
    name=package_name,
    version=package_version,
    description=description,
    long_description=description,
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt",
    packages=find_namespace_packages(include=['dbt_rpc', 'dbt_rpc.*']),
    include_package_data=True,
    test_suite='tests',
    entry_points={
        'console_scripts': [
            'dbt-rpc = dbt_rpc.__main__:main',
        ],
    },
    install_requires=[
        'dbt-core==0.21.0',
        'dbt-postgres==0.21.0',
        'dbt-bigquery==0.21.0',
        'dbt-snowflake==0.21.0',
        'dbt-redshift==0.21.0',
    ],
    zip_safe=False,
    classifiers=[
        'Development Status :: 5 - Production/Stable',

        'License :: OSI Approved :: Apache Software License',

        'Operating System :: Microsoft :: Windows',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX :: Linux',

        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    python_requires=">=3.6.3",
)