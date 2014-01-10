# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.data.text.KeyStore
"""
"""n// NOTE
----------------------------------------------------------------------------
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
http://www.direct-netware.de/redirect.py?pas;database

This Source Code Form is subject to the terms of the Mozilla Public License,
v. 2.0. If a copy of the MPL was not distributed with this file, You can
obtain one at http://mozilla.org/MPL/2.0/.
----------------------------------------------------------------------------
http://www.direct-netware.de/redirect.py?licenses;mpl2
----------------------------------------------------------------------------
#echo(pasDatabaseVersion)#
#echo(__FILEPATH__)#
----------------------------------------------------------------------------
NOTE_END //n"""

from random import randrange
from time import time

from dNG.pas.data.binary import Binary
from dNG.pas.data.settings import Settings
from dNG.pas.database.connection import Connection
from dNG.pas.database.instance import Instance
from dNG.pas.database.instances.key_store import KeyStore as _DbKeyStore
from dNG.pas.runtime.io_exception import IOException

class KeyStore(Instance):
#
	"""
Database based encoded key-value store.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	def __init__(self, db_instance = None):
	#
		"""
Constructor __init__(KeyStore)

:since: v0.1.00
		"""

		if (db_instance == None): db_instance = _DbKeyStore()
		Instance.__init__(self, db_instance)

		self.db_id = (None if (db_instance == None) else db_instance.id)
		"""
Database ID used for reloading
		"""
	#

	def data_set(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		with self:
		#
			if ("key" in kwargs): self.local.db_instance.key = Binary.utf8(kwargs['key'])
			if ("validity_start_date" in kwargs): self.local.db_instance.validity_date = kwargs['validity_start_date']
			if ("validity_end_date" in kwargs): self.local.db_instance.validity_date = kwargs['validity_end_date']
			if ("value" in kwargs): self.local.db_instance.value = Binary.utf8(kwargs['value'])
		#
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

	def is_valid(self):
	#
		"""
Returns true if the KeyStore entry is active and valid.

:since: v0.1.00
		"""

		with self:
		#
			timestamp = time()

			_return = (self.local.db_instance.validity_start_date == 0 or self.local.db_instance.validity_start_date < timestamp)
			if (_return and self.local.db_instance.validity_end_date != 0 and self.local.db_instance.validity_end_date < timestamp): _return = False
		#

		return _return
	#

	def _reload(self):
	#
		"""
Implementation of the reloading SQLalchemy database instance logic.

:since: v0.1.00
		"""

		with self.lock:
		#
			if (self.local.db_instance == None):
			#
				if (self.db_id == None): raise IOException("Database instance is not reloadable.")
				self.local.db_instance = self._database.query(_DbKeyStore).filter(_DbKeyStore.id == self.db_id).one()
			#
			else: Instance._reload(self)
		#
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

		with Connection.get_instance() as database:
		#
			if ((not Settings.get("pas_database_auto_maintenance", False)) and randrange(0, 3) < 1):
			#
				if (database.query(_DbKeyStore).filter(_DbKeyStore.validity_end_date <= int(time())).delete() > 0): database.optimize_random(_DbKeyStore)
			#

			_return = (None if (db_instance == None) else KeyStore(db_instance))
			if (_return != None and (not _return.is_valid())): _return = None
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

		with Connection.get_instance() as database: return KeyStore._load(database.query(_DbKeyStore).filter(_DbKeyStore.id == _id).first())
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

		with Connection.get_instance() as database: return KeyStore._load(database.query(_DbKeyStore).filter(_DbKeyStore.key == key).first())
	#
#

##j## EOF