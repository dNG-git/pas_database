# -*- coding: utf-8 -*-
##j## BOF

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

from random import randrange
from sqlalchemy.sql.expression import and_
from time import time

from dNG.data.json_resource import JsonResource
from dNG.pas.data.binary import Binary
from dNG.pas.data.settings import Settings
from dNG.pas.database.connection import Connection
from dNG.pas.database.instance import Instance
from dNG.pas.database.nothing_matched_exception import NothingMatchedException
from dNG.pas.database.instances.key_store import KeyStore as _DbKeyStore
from dNG.pas.runtime.io_exception import IOException
from dNG.pas.runtime.type_exception import TypeException
from dNG.pas.runtime.value_exception import ValueException

class KeyStore(Instance):
#
	"""
Database based encoded key-value store.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=maybe-no-member

	_DB_INSTANCE_CLASS = _DbKeyStore
	"""
SQLAlchemy database instance class to initialize for new instances.
	"""

	def __init__(self, db_instance = None):
	#
		"""
Constructor __init__(KeyStore)

:since: v0.1.00
		"""

		if (db_instance is None): db_instance = _DbKeyStore()
		Instance.__init__(self, db_instance)

		self.db_id = (None if (db_instance is None) else self.get_id())
		"""
Database ID used for reloading
		"""
	#

	get_id = Instance._wrap_getter("id")
	"""
Returns the ID of this instance.

:return: (str) KeyStore entry ID; None if undefined
:since:  v0.1.00
	"""

	get_key = Instance._wrap_getter("id")
	"""
Returns the key of this instance.

:return: (str) KeyStore entry key; None if undefined
:since:  v0.1.00
	"""

	def get_value_dict(self):
	#
		"""
Returns the values originally given as a dict to this KeyStore instance.

:return: (dict) Values from the KeyStore
:since:  v0.1.00
		"""

		with self: _return = JsonResource().json_to_data(self.local.db_instance.value)
		if (_return is None): raise ValueException("Value of the KeyStore does not contain the expected data format")
		return _return
	#

	def is_reloadable(self):
	#
		"""
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v0.1.00
		"""

		return (self.db_id is not None)
	#

	def is_valid(self):
	#
		"""
Returns true if the KeyStore entry is active and valid.

:since: v0.1.00
		"""

		with self:
		#
			timestamp = time()

			_return = (self.local.db_instance.validity_start_time == 0 or self.local.db_instance.validity_start_time < timestamp)
			if (_return and self.local.db_instance.validity_end_time != 0 and self.local.db_instance.validity_end_time < timestamp): _return = False
		#

		return _return
	#

	def _reload(self):
	#
		"""
Implementation of the reloading SQLAlchemy database instance logic.

:since: v0.1.00
		"""

		if (self.local.db_instance is None):
		#
			if (self.db_id is None): raise IOException("Database instance is not reloadable.")
			self.local.db_instance = self.local.connection.query(_DbKeyStore).filter(_DbKeyStore.id == self.db_id).one()
		#
		else: Instance._reload(self)
	#

	def set_data_attributes(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		with self:
		#
			if ("key" in kwargs): self.local.db_instance.key = Binary.utf8(kwargs['key'])
			if ("validity_start_time" in kwargs): self.local.db_instance.validity_start_time = kwargs['validity_start_time']
			if ("validity_end_time" in kwargs): self.local.db_instance.validity_end_time = kwargs['validity_end_time']
			if ("value" in kwargs): self.local.db_instance.value = Binary.utf8(kwargs['value'])
		#
	#

	def set_value_dict(self, data):
	#
		"""
Sets the values given as a dict as the value of this KeyStore instance.

:param data: Dict to be set as value

:since: v0.1.00
		"""

		if (not isinstance(data, dict)): raise TypeException("Invalid data type given")
		self.set_data_attributes(value = JsonResource().data_to_json(data))
	#

	@staticmethod
	def _load(db_instance):
	#
		"""
Load KeyStore entry from database.

:param db_instance: SQLAlchemy database instance

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		with Connection.get_instance() as connection:
		#
			if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
			#
				validity_ended_condition = and_(_DbKeyStore.validity_end_time > 0,
				                                _DbKeyStore.validity_end_time < int(time())
				                               )

				if (connection.query(_DbKeyStore).filter(validity_ended_condition).delete() > 0):
				#
					connection.optimize_random(_DbKeyStore)
				#
			#

			_return = (None if (db_instance is None) else KeyStore(db_instance))
			if (_return is not None and (not _return.is_valid())): _return = None
		#

		return _return
	#

	@staticmethod
	def load_id(_id):
	#
		"""
Load KeyStore value by ID.

:param _id: KeyStore ID

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		if (_id is None): raise NothingMatchedException("KeyStore ID is invalid")

		with Connection.get_instance() as connection: _return = KeyStore._load(connection.query(_DbKeyStore).get(_id))
		if (_return is None): raise NothingMatchedException("KeyStore ID '{0}' not found".format(_id))
		return _return
	#

	@staticmethod
	def load_key(key):
	#
		"""
Load KeyStore value by key.

:param key: KeyStore key

:return: (object) KeyStore instance on success
:since:  v0.1.00
		"""

		if (key is None): raise NothingMatchedException("KeyStore key is invalid")

		with Connection.get_instance() as connection: _return = KeyStore._load(connection.query(_DbKeyStore).filter(_DbKeyStore.key == key).first())
		if (_return is None): raise NothingMatchedException("KeyStore key '{0}' not found".format(key))
		return _return
	#
#

##j## EOF