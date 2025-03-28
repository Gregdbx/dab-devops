"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the demo_bundle project.
"""

from setuptools import setup, find_packages

import sys

sys.path.append("./bundle_demo")

import datetime
import demo_bundle

local_version = datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S")

setup(
    name="demo_bundle",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=demo_bundle.__version__ + "+" + local_version,
    url="https://databricks.com",
    author="gregoire.portier@databricks.com",
    description="wheel file based on demo_bundle/src",
    packages=find_packages(where="./bundle_demo"),
    package_dir={"": "bundle_demo"},
    entry_points={
        "packages": [
            "main=demo_bundle.main:main",
        ],
    },
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools"
    ],
)
