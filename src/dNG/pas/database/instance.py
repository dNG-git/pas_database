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

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.result import ResultProxy
from threading import local

from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.runtime.io_exception import IOException
from dNG.pas.runtime.not_implemented_exception import NotImplementedException
from dNG.pas.runtime.thread_lock import ThreadLock
from dNG.pas.runtime.type_exception import TypeException
from dNG.pas.runtime.value_exception import ValueException
from .connection import Connection
from .instance_iterator import InstanceIterator
from .sort_definition import SortDefinition
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
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=unused-argument

	def __init__(self, db_instance = None):
	#
		"""
Constructor __init__(Instance)

:param db_instance: Encapsulated SQLAlchemy database instance

:since: v0.1.00
		"""

		self._db_sort_definition = None
		"""
Sort definition instance
		"""
		self.local = local()
		"""
thread-local instance
		"""
		self._lock = ThreadLock()
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

		# pylint: disable=broad-except,maybe-no-member,protected-access

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.__enter__()- (#echo(__LINE__)#)", self, context = "pas_database")

		try:
		#
			with self._lock:
			#
				if (not hasattr(self.local, "context_depth")): self.local.context_depth = 0

				if (self.local.context_depth < 1):
				#
					Connection._acquire()
					self.local.connection = Connection.get_instance()
				#

				self.local.context_depth += 1

				if (hasattr(self.local, "db_instance")
				    and self.local.db_instance != None
				   ): self._ensure_attached_instance()
				elif (self.is_reloadable()): self.reload()
			#
		#
		except Exception:
		#
			self._cleanup_enter()
			raise
		#
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v0.1.00
		"""

		# pylint: disable=broad-except,protected-access

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.__exit__()- (#echo(__LINE__)#)", self, context = "pas_database")

		if (self.local.context_depth > 0):
		# Thread safety
			with self._lock:
			#
				if (self.local.context_depth > 0):
				#
					self.local.context_depth -= 1

					if (self.local.context_depth < 1 and self.wrapped_transaction):
					#
						try:
						#
							if (exc_type == None and exc_value == None): self.local.connection.commit()
							else: self.local.connection.rollback()
						#
						except Exception as handled_exception:
						#
							if (self.log_handler != None): self.log_handler.error(handled_exception, context = "pas_database")
							if (exc_type == None and exc_value == None): self.local.connection.rollback()
						#

						self.wrapped_transaction = False
					#

					if (self.local.context_depth < 1): Connection._release()
				#
			#
		#

		return False
	#

	def __new__(cls, *args, **kwargs):
	#
		"""
python.org: Called to create a new instance of class cls.

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

	def _apply_db_sort_definition(self, query, context = None):
	#
		"""
Applies the sort order to the given SQLAlchemy query instance.

:param query: SQLAlchemy query instance
:param context: Sort definition context

:return: (object) Modified SQLAlchemy query instance
:since:  v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._apply_db_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")
		_return = query

		sort_definition = (self._get_default_sort_definition(context)
		                   if (self._db_sort_definition == None) else
		                   self._db_sort_definition
		                  )

		if (sort_definition != None): _return = sort_definition.apply(self, query)
		return _return
	#

	def _cleanup_enter(self):
	#
		"""
This method should be called for if exceptions in "__enter__" occur to
cleanup database connections held by this instance.

:since: v0.1.00
		"""

		# pylint: disable=protected-access

		if (self.local.context_depth < 1):
		#
			Connection._release()
			self.local.connection = None
		#
		else: self.local.context_depth -= 1
	#

	def delete(self):
	#
		"""
Deletes this entry from the database.

:return: (bool) True on success
:since:  v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.delete()- (#echo(__LINE__)#)", self, context = "pas_database")
		_return = True

		if (self.is_known()):
		#
			with self:
			#
				self._ensure_transaction_context()

				self.local.connection.delete(self.local.db_instance)
				self.local.db_instance = None
			#
		#
		else: _return = False

		return _return

	def _ensure_attached_instance(self):
	#
		"""
Checks the SQLAlchemy database instance to be attached to an session.

:since: v0.1.01
		"""

		with self._lock:
		#
			instance_state = inspect(self.local.db_instance)

			if (instance_state.detached):
			#
				if (instance_state.has_identity):
				#
					self.local.db_instance = self.local.connection.merge(self.local.db_instance,
					                                                     load = (not instance_state.persistent)
					                                                    )
				#
				else: self.local.connection.add(self.local.db_instance)
			#
		#
	#

	def _ensure_transaction_context(self):
	#
		"""
Checks for an active transaction or begins one.

:since: v0.1.01
		"""

		# pylint: disable=broad-except,maybe-no-member,protected-access

		if (self.local.connection.get_transaction_depth() < 1):
		#
			with self._lock:
			# Thread safety
				if (self.local.connection.get_transaction_depth() < 1):
				#
					self.local.connection.begin()
					self.wrapped_transaction = True
				#
			#
		#
	#

	def _ensure_thread_local_instance(self, cls):
	#
		"""
Checks for an initialized SQLAlchemy database instance or create one.

:since: v0.1.01
		"""

		if ((not hasattr(self.local, "db_instance")) or self.local.db_instance == None):
		#
			self.local.db_instance = (None
			                          if (self.is_reloadable()) else
			                          cls()
			                         )
		#
	#

	def _get_data_attribute(self, attribute):
	#
		"""
Returns the data for the requested attribute.

:param attribute: Requested attribute

:return: (dict) Value for the requested attribute; None if undefined
:since:  v0.1.00
		"""

		return (getattr(self.local.db_instance, attribute)
		        if (hasattr(self.local.db_instance, attribute)) else
		        self._get_unknown_data_attribute(attribute)
		       )
	#

	def get_data_attributes(self, *args):
	#
		"""
Returns the requested attributes.

:return: (dict) Values for the requested attributes; None for undefined ones
:since:  v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.get_data_attributes()- (#echo(__LINE__)#)", self, context = "pas_database")
		_return = { }

		with self:
		#
			for attribute in args: _return[attribute] = self._get_data_attribute(attribute)
		#

		return _return
	#

	def _get_db_column(self, attribute):
	#
		"""
Returns the SQLAlchemy column for the requested attribute.

:param attribute: Requested attribute

:return: (object) SQLAlchemy column; None if undefined
:since:  v0.1.00
		"""

		with self:
		#
			return (getattr(self.local.db_instance.__class__, attribute)
			        if (hasattr(self.local.db_instance.__class__, attribute)) else
			        self._get_unknown_db_column(attribute)
			       )
		#
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

	def _get_default_sort_definition(self, context = None):
	#
		"""
Returns the default sort definition list.

:param context: Sort definition context

:return: (object) Sort definition
:since:  v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._get_default_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")
		return SortDefinition()
	#

	def _get_unknown_data_attribute(self, attribute):
	#
		"""
Returns the data for the requested attribute not defined for this instance.

:param attribute: Requested attribute

:return: (dict) Value for the requested attribute; None if undefined
:since:  v0.1.00
		"""

		return None
	#

	def _get_unknown_db_column(self, attribute):
	#
		"""
Returns the SQLAlchemy column for the requested attribute not defined for
this instance main entity.

:param attribute: Requested attribute

:return: (object) SQLAlchemy column
:since:  v0.1.00
		"""

		raise ValueException("Given attribute '{0}' is not defined for this database instance".format(attribute))
	#

	def _insert(self):
	#
		"""
Insert the instance into the database.

:since: v0.1.00
		"""

		# pylint: disable=maybe-no-member

		if (inspect(self.local.db_instance).transient): self.local.connection.add(self.local.db_instance)
	#

	def is_data_attribute_none(self, *args):
	#
		"""
Returns true if at least one of the attributes is "None".

:return: (bool) True if at least one of the attributes is "None"
:since:  v0.1.00
		"""

		with self:
		#
			_return = False

			for attribute in args:
			#
				if (self._get_data_attribute(attribute) == None):
				#
					_return = True
					break
				#
			#
		#

		return _return
	#

	def is_known(self):
	#
		"""
Returns true if the instance is already saved in the database.

:return: (bool) True if known
:since:  v0.1.00
		"""

		# pylint: disable=maybe-no-member

		return inspect(self.local.db_instance).has_identity
	#

	def is_reloadable(self):
	#
		"""
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v0.1.00
		"""

		return False
	#

	def reload(self):
	#
		"""
Reload instance data from the database.

:since: v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.reload()- (#echo(__LINE__)#)", self, context = "pas_database")

		with self._lock:
		#
			if (hasattr(self.local, "connection") and self.local.connection != None): self._reload()
		#
	#

	def _reload(self):
	#
		"""
Implementation of the reloading SQLAlchemy database instance logic.

:since: v0.1.00
		"""

		if (self.local.db_instance == None): raise IOException("Database instance is not reloadable.")
		self.local.connection.refresh(self.local.db_instance)
	#

	def save(self):
	#
		"""
Saves changes of the instance into the database.

:since: v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.save()- (#echo(__LINE__)#)", self, context = "pas_database")

		with self:
		#
			self._ensure_transaction_context()

			if (self.is_known()): self._update()
			else: self._insert()
		#
	#

	def set_data_attributes(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		raise NotImplementedException()
	#

	def set_sort_definition(self, sort_definition):
	#
		"""
Sets the sort definition list.

:param sort_definition: Sort definition list

:since: v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.set_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")

		if (not isinstance(sort_definition, SortDefinition)): raise TypeException("Sort definition type given is not supported")
		self._db_sort_definition = sort_definition
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
Returns an instance wrapping buffered iterator to encapsulate SQLAlchemy
database instances with an given class.

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor
:param instance_class: Encapsulating database instance class

:return: (Iterator) InstanceIterator object
:since:  v0.1.00
		"""

		if (not isinstance(result, ResultProxy)): raise ValueException("Invalid database result given")
		return InstanceIterator(entity, result, True, instance_class, *args, **kwargs)
	#

	@staticmethod
	def iterator(entity, result, instance_class = None, *args, **kwargs):
	#
		"""
Returns an instance wrapping unbuffered iterator to encapsulate SQLAlchemy
database instances with an given class.

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor
:param instance_class: Encapsulating database instance class

:return: (Iterator) InstanceIterator object
:since:  v0.1.00
		"""

		if (not isinstance(result, ResultProxy)): raise ValueException("Invalid database result given")
		return InstanceIterator(entity, result, False, instance_class, *args, **kwargs)
	#

	@staticmethod
	def _wrap_getter(key):
	#
		"""
Wraps a "get*" method to return the given database entry value or
alternatively the given default one.

:param key: Key to create the "get*" method for

:return: (object) Proxy method
:since:  v0.1.00
		"""

		def proxymethod(self):
		#
			"""
Returns the value of the corresponding attribute.

:return: (mixed) Attribute value
:since:  v0.1.00
			"""

			return self.get_data_attributes(key)[key]
		#

		return proxymethod
	#

	@staticmethod
	def _wrap_setter(key):
	#
		"""
Wraps a "set*" method to set the given database entry value.

:param key: Key to create the "set*" method for

:return: (object) Proxy method
:since:  v0.1.00
		"""

		def proxymethod(self, value):
		#
			"""
Sets the value of the corresponding attribute.

:param value: Attribute value

:since: v0.1.00
			"""

			# pylint: disable=star-args

			attribute = { key: value }
			self.set_data_attributes(**attribute)
		#

		return proxymethod
	#
#

##j## EOF