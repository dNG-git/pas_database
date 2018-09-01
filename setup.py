# -*- coding: utf-8 -*-

"""
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
https://www.direct-netware.de/redirect?pas;core

This Source Code Form is subject to the terms of the Mozilla Public License,
v. 2.0. If a copy of the MPL was not distributed with this file, You can
obtain one at http://mozilla.org/MPL/2.0/.
----------------------------------------------------------------------------
https://www.direct-netware.de/redirect?licenses;mpl2
----------------------------------------------------------------------------
setup.py
"""

from os import makedirs, path

from distutils.core import setup

try:
    from dNG.distutils.command.build_py import BuildPy
    from dNG.distutils.command.install_data import InstallData
    from dNG.distutils.command.sdist import Sdist
    from dNG.distutils.temporary_directory import TemporaryDirectory
except ImportError:
    raise RuntimeError("'dng-builder-suite' prerequisite not matched")
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

    makedirs(path.join(build_directory, "src", "dNG"))

    _setup = { "name": "pas-database",
               "version": get_version(),
               "description": "Python Application Services",
               "long_description": """"pas_database" is an adapter and abstraction layer for SQLAlchemy.""",
               "author": "direct Netware Group et al.",
               "author_email": "web@direct-netware.de",
               "license": "MPL2",
               "url": "https://www.direct-netware.de/redirect?pas;database",

               "platforms": [ "any" ],

               "packages": [ "dNG" ],

               "data_files": [ ( "docs", [ "LICENSE", "README" ]) ],
               "scripts": [ path.join("src", "pas_db_tool.py") ]
             }

    # Override build_py to first run builder.py
    _setup['cmdclass'] = { "build_py": BuildPy, "install_data": InstallData, "sdist": Sdist }

    setup(**_setup)
#
