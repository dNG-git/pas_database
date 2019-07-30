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

# pylint: disable=import-error, no-name-in-module

from random import randrange
from time import time

try: from collections.abc import Mapping
except ImportError: from collections import Mapping

from dpt_json import JsonResource
from dpt_runtime.binary import Binary
from dpt_runtime.io_exception import IOException
from dpt_runtime.type_exception import TypeException
from dpt_runtime.value_exception import ValueException
from dpt_settings import Settings

from sqlalchemy.sql.expression import and_

from ..connection import Connection
from ..instance import Instance
from ..nothing_matched_exception import NothingMatchedException
from ..orm.key_store import KeyStore as _DbKeyStore

class KeyStore(Instance):
    """
Database based encoded key-value store.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    # pylint: disable=bad-staticmethod-argument

    _DB_INSTANCE_CLASS = _DbKeyStore
    """
SQLAlchemy database instance class to initialize for new instances.
    """

    def __init__(self, db_instance = None):
        """
Constructor __init__(KeyStore)

:since: v1.0.0
        """

        Instance.__init__(self, db_instance)

        self._id = (None if (db_instance is None) else self['id'])
        """
Database ID used for reloading
        """
    #

    @property
    def is_reloadable(self):
        """
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v1.0.0
        """

        return (self._id is not None)
    #

    @property
    def is_valid(self):
        """
Returns true if the KeyStore entry is active and valid.

:since: v1.0.0
        """

        with self:
            timestamp = time()

            _return = (self.local.db_instance.validity_start_time == 0 or self.local.db_instance.validity_start_time < timestamp)
            if (_return and self.local.db_instance.validity_end_time != 0 and self.local.db_instance.validity_end_time < timestamp): _return = False
        #

        return _return
    #

    @property
    def value_dict(self):
        """
Returns the values originally given as a dict to this KeyStore instance.

:return: (dict) Values from the KeyStore
:since:  v1.0.0
        """

        with self:
            _return = ({ }
                       if (self.local.db_instance.value is None) else
                       JsonResource.json_to_data(self.local.db_instance.value)
                      )
        #

        if (_return is None): raise ValueException("Value of the KeyStore does not contain the expected data format")
        return _return
    #

    @value_dict.setter
    def value_dict(self, data):
        """
Sets the values given as a dict as the value of this KeyStore instance.

:param data: Dict to be set as value

:since: v1.0.0
        """

        if (not isinstance(data, dict)): raise TypeException("Invalid data type given")
        with self: self._set_data_attribute("value", JsonResource().data_to_json(data))
    #

    def _reload(self):
        """
Implementation of the reloading SQLAlchemy database instance logic.

:since: v1.0.0
        """

        if (self.local.db_instance is None):
            if (self._id is None): raise IOException("Database instance is not reloadable.")
            self.local.db_instance = self.local.connection.query(_DbKeyStore).filter(_DbKeyStore.id == self._id).one()
        else: Instance._reload(self)
    #

    def _set_data_attribute(self, attribute, value):
        """
Sets data for the requested attribute.

:param attribute: Requested attribute
:param value: Value for the requested attribute

:since: v1.0.0
        """

        if (attribute == "value" and isinstance(value, Mapping)): self.value_dict = value
        else:
            if (attribute in ( "key", "value" )): value = Binary.utf8(value)
            Instance._set_data_attribute(self, attribute, value)
        #
    #

    @staticmethod
    def _load(cls, db_instance):
        """
Load KeyStore entry from database.

:param cls: Expected encapsulating database instance class
:param db_instance: SQLAlchemy database instance

:return: (object) KeyStore instance on success
:since:  v1.0.0
        """

        _return = None

        with Connection.get_instance() as connection:
            if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
                validity_ended_condition = and_(_DbKeyStore.validity_end_time > 0,
                                                _DbKeyStore.validity_end_time < int(time())
                                               )

                if (connection.query(_DbKeyStore).filter(validity_ended_condition).delete() > 0):
                    connection.optimize_random(_DbKeyStore)
                #
            #

            if (db_instance is not None):
                Instance._ensure_db_class(cls, db_instance)

                _return = KeyStore(db_instance)
                if (not _return.is_valid): _return = None
            #
        #

        return _return
    #

    @classmethod
    def load_id(cls, _id):
        """
Load KeyStore value by ID.

:param cls: Expected encapsulating database instance class
:param _id: KeyStore ID

:return: (object) KeyStore instance on success
:since:  v1.0.0
        """

        if (_id is None): raise NothingMatchedException("KeyStore ID is invalid")

        with Connection.get_instance(): _return = KeyStore._load(cls, Instance.get_db_class_query(cls).get(_id))

        if (_return is None): raise NothingMatchedException("KeyStore ID '{0}' not found".format(_id))
        return _return
    #

    @classmethod
    def load_key(cls, key):
        """
Load KeyStore value by key.

:param cls: Expected encapsulating database instance class
:param key: KeyStore key

:return: (object) KeyStore instance on success
:since:  v1.0.0
        """

        if (key is None): raise NothingMatchedException("KeyStore key is invalid")

        with Connection.get_instance():
            _return = KeyStore._load(cls,
                                     Instance.get_db_class_query(cls).filter(_DbKeyStore.key == key).first()
                                    )
        #

        if (_return is None): raise NothingMatchedException("KeyStore key '{0}' not found".format(key))
        return _return
    #
#
