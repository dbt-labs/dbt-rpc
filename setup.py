#!/usr/bin/env python
import os
from setuptools import setup, find_namespace_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


package_name = "dbt-rpc"
package_version = "0.1.0"
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
    # TODO: use packages from PyPI once available for dbt-core 1.0
    install_requires=[
        'dbt-core @ git+https://github.com/dbt-labs/dbt.git#egg=dbt-core&subdirectory=core',
        'dbt-postgres @ git+https://github.com/dbt-labs/dbt.git#egg=dbt-postgres&subdirectory=plugins/postgres',
        'dbt-bigquery @ git+https://github.com/dbt-labs/dbt-bigquery.git#egg=dbt-bigquery',
        'dbt-snowflake @ git+https://github.com/dbt-labs/dbt-snowflake.git#egg=dbt-snowflake',
        'dbt-redshift @ git+https://github.com/dbt-labs/dbt-redshift.git#egg=dbt-redshift',
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
