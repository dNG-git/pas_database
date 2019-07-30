# -*- coding: utf-8 -*-

"""
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
https://www.direct-netware.de/redirect?pas;database

This Source Code Form is subject to the terms of the Mozilla Public License,
v. 2.0. If a copy of the MPL was not distributed with this file, You can
obtain one at http://mozilla.org/MPL/2.0/.
----------------------------------------------------------------------------
https://www.direct-netware.de/redirect?licenses;mpl2
----------------------------------------------------------------------------
setup.py
"""

from os import makedirs, path

try:
    from setuptools import setup
except ImportError:
    from distutils import setup
#

try:
    from dpt_builder_suite.distutils.build_py import BuildPy
    from dpt_builder_suite.distutils.install_data import InstallData
    from dpt_builder_suite.distutils.sdist import Sdist
    from dpt_builder_suite.distutils.temporary_directory import TemporaryDirectory
except ImportError:
    raise RuntimeError("'dpt-builder-suite' prerequisite not matched")
#

def get_version():
    """
Returns the version currently in development.

:return: (str) Version string
:since:  v0.1.2
    """

    return "v1.0.0"
#

with TemporaryDirectory(dir = ".") as build_directory:
    parameters = { "pasDatabaseVersion": get_version(), "plain_copy_extensions": "json,sql" }

    BuildPy.set_build_target_path(build_directory)
    BuildPy.set_build_target_parameters(parameters)

    InstallData.add_install_data_callback(InstallData.plain_copy, [ "data" ])
    InstallData.set_build_target_path(build_directory)
    InstallData.set_build_target_parameters(parameters)

    Sdist.set_build_target_path(build_directory)
    Sdist.set_build_target_parameters(parameters)

    package_dir = path.join(build_directory, "src")
    makedirs(package_dir)

    _setup = { "version": get_version()[1:],
               "package_dir": { "": package_dir },
               "packages": [ "pas_database" ],
               "data_files": [ ( "docs", [ "LICENSE", "README" ]) ],
               "entry_points": { "console_scripts": [ "pas-database-tool = pas_database.__main__:main" ] },
               "test_suite": "tests"
             }

    # Override build_py to first run builder.py
    _setup['cmdclass'] = { "build_py": BuildPy, "install_data": InstallData, "sdist": Sdist }

    setup(**_setup)
#
