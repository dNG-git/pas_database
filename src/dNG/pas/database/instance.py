# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.Instance
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

from sqlalchemy import inspect
from sqlalchemy.engine.result import ResultProxy
from threading import local

from dNG.pas.data.traced_exception import TracedException
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.runtime.thread_lock import ThreadLock
from .connection import Connection
from .instance_iterator import InstanceIterator
from .instances.abstract import Abstract

class Instance(object):
#
	"""
"Instance" is an abstract object encapsulating an SQLAlchemy database
instance.

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
Constructor __init__(Instance)

:param db_instance: Encapsulated SQLAlchemy database instance

:since: v0.1.00
		"""

		self.context_depth = 0
		"""
Runtime context depth
		"""
		self._database = None
		"""
Database connection if bound
		"""
		self.local = local()
		"""
thread-local instance
		"""
		self.lock = ThreadLock()
		"""
Thread safety lock
		"""
		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)
		"""
The LogHandler is called whenever debug messages should be logged or errors
happened.
		"""
		self.wrapped_transaction = False
		"""
True if a wrapping transaction has been created automatically.
		"""

		self.local.db_instance = db_instance
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		self._database = Connection.get_instance()
		Connection._acquire()

		try:
		#
			if ((self.context_depth + self._database.get_transaction_depth()) < 1):
			#
				self._database.begin()
				self.context_depth = 1
			#
			elif (self.context_depth > 0): self.context_depth += 1

			if (self.local.db_instance == None): self.reload()
			else:
			#
				with self.lock:
				#
					if (inspect(self.local.db_instance).detached): self._database.add(self.local.db_instance)
				#
			#
		#
		except Exception:
		#
			Connection._release()
			raise
		#
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:since: v0.1.00
		"""

		if (self.context_depth > 0):
		#
			self.context_depth -= 1

			if (self.context_depth < 1):
			#
				try:
				#
					if (exc_type == None and exc_value == None): self._database.commit()
					else: self._database.rollback()
				#
				except Exception as handled_exception:
				#
					if (self.log_handler != None): self.log_handler.error(handled_exception)
					if (exc_type == None and exc_value == None): self._database.rollback()
				#

				with self.lock: self._database = None
			#
		#

		Connection._release()
	#

	def data_get(self, *args):
	#
		"""
Return the requested attributes.

:return: (dict) Values for the requested attributes
:since:  v0.1.00
		"""

		with self:
		#
			_return = { }
			for attribute in args: _return[attribute] = self._data_get(attribute)
		#

		return _return
	#

	def _data_get(self, attribute):
	#
		"""
Return the data for the requested attribute.

:param attribute: Requested attribute

:return: (dict) Value for the requested attribute
:since:  v0.1.00
		"""

		return (getattr(self.local.db_instance, attribute) if (hasattr(self.local.db_instance, attribute)) else self._data_get_unknown(attribute))
	#

	def _data_get_unknown(self, attribute):
	#
		"""
Return the data for the requested attribute not defined for this instance.

:param attribute: Requested attribute

:return: (dict) Value for the requested attribute
:since:  v0.1.00
		"""

		return None
	#

	def data_set(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		raise TracedException("Not implemented")
	#

	def delete(self):
	#
		"""
Deletes this entry from the database.

:return: (bool) True on success
:since:  v0.1.00
		"""

		_return = True

		if (self.is_known()):
		#
			with self:
			#
				self._database.delete(self.local.db_instance)
				self.local.db_instance = None
			#
		#
		else: _return = False

		return _return
	#

	def _get_db_instance(self):
	#
		"""
Returns the actual database entry instance.

:return: (object) Database entry instance
:since:  v0.1.00
		"""

		with self: return self.local.db_instance
	#

	def _insert(self):
	#
		"""
Insert the instance into the database.

:since: v0.1.00
		"""

		with self.lock, Connection.get_instance():
		#
			instance_state = inspect(self.local.db_instance)
			if (instance_state.transient): self._database.add(self.local.db_instance)
		#
	#

	def is_known(self):
	#
		"""
Returns true if the instance is already saved in the database.

:return: (bool) True if known
:since:  v0.1.00
		"""

		with self: return inspect(self.local.db_instance).has_identity
	#

	def reload(self):
	#
		"""
Reload instance data from the database.

:since: v0.1.00
		"""

		with self.lock:
		#
			if (self._database != None): self._reload()
		#
	#

	def _reload(self):
	#
		"""
Implementation of the reloading SQLalchemy database instance logic.

:since: v0.1.00
		"""

		if (self.local.db_instance == None): raise TracedException("Database instance is not reloadable.")
		self._database.refresh(self.local.db_instance)
	#

	def save(self):
	#
		"""
Saves changes of the instance into the database.

:since: v0.1.00
		"""

		if (self.is_known()): self._update()
		else: self._insert()
	#

	def _update(self):
	#
		"""
Updates the instance already saved to the database.

:since: v0.1.00
		"""

		pass
	#

	@staticmethod
	def buffered_iterator(entity, result, instance_class = None, *args, **kwargs):
	#
		"""
Returns an instance wrapping buffered iterator to encapsulate SQLalchemy
database instances with an given class.

:param entity: SQLalchemy database entity
:param cursor: SQLalchemy result cursor
:param instance_class: Encapsulating database instance class

:return: (Iterator) InstanceIterator object
:since:  v0.1.00
		"""

		if (not isinstance(result, ResultProxy)): raise TracedException("Invalid database result given")
		return InstanceIterator(entity, result, True, instance_class, *args, **kwargs)
	#

	@staticmethod
	def iterator(entity, result, instance_class = None, *args, **kwargs):
	#
		"""
Returns an instance wrapping unbuffered iterator to encapsulate SQLalchemy
database instances with an given class.

:param entity: SQLalchemy database entity
:param cursor: SQLalchemy result cursor
:param instance_class: Encapsulating database instance class

:return: (Iterator) InstanceIterator object
:since:  v0.1.00
		"""

		if (not isinstance(result, ResultProxy)): raise TracedException("Invalid database result given")
		return InstanceIterator(entity, result, False, instance_class, *args, **kwargs)
	#

	@staticmethod
	def __new__(cls, *args, **kwargs):
	#
		"""
python.org: Called to create a new instance of class cls..

:return: (object) Instance object
:since:  v0.1.00
		"""

		db_instance = (kwargs['db_instance'] if ("db_instance" in kwargs) else None)
		if (db_instance == None): db_instance = (args[0] if (len(args) > 0) else None)
		db_instance_class = (NamedLoader.get_class(db_instance.db_instance_class) if (isinstance(db_instance, Abstract) and db_instance.db_instance_class != None) else None)

		if (db_instance_class == None or cls == db_instance_class): _return = object.__new__(cls)
		else: _return = db_instance_class.__new__(db_instance_class, *args, **kwargs)

		return _return
	#
#

##j## EOF