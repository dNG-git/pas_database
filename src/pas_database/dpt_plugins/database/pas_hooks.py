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
#echo(pasDatabaseVersion)#
#echo(__FILEPATH__)#
"""

# pylint: disable=import-error, no-name-in-module, unused-argument

from dpt_module_loader import NamedClassLoader
from dpt_plugins import Hook

from ...orm import Abstract
from ...schema import Schema

def after_apply_schema(params, last_return = None):
    """
Called for "pas.Database.applySchema.after"

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    key_store_class = NamedClassLoader.get_class("pas_database.orm.KeyStore")
    Schema.apply_version(key_store_class)

    schema_version_class = NamedClassLoader.get_class("pas_database.orm.SchemaVersion")
    Schema.apply_version(schema_version_class)

    return last_return
#

def load_all(params, last_return = None):
    """
Load and register all SQLAlchemy objects to generate database tables.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (mixed) Return value
:since:  v1.0.0
    """

    NamedClassLoader.get_class("pas_database.orm.KeyStore")
    NamedClassLoader.get_class("pas_database.orm.SchemaVersion")

    return last_return
#

def register_plugin():
    """
Register plugin hooks.

:since: v1.0.0
    """

    Hook.register("pas.Database.applySchema.after", after_apply_schema)
    Hook.register("pas.Database.loadAll", load_all)
#

def unregister_plugin():
    """
Unregister plugin hooks.

:since: v1.0.0
    """

    Hook.unregister("pas.Database.applySchema.after", after_apply_schema)
    Hook.unregister("pas.Database.loadAll", load_all)
#
