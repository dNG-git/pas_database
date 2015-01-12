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

# pylint: disable=import-error,no-name-in-module

from os import path
from random import randrange
from sqlalchemy.engine import engine_from_config
from sqlalchemy.event import listen
from sqlalchemy.orm.interfaces import EXT_CONTINUE
from sqlalchemy.orm.mapper import configure_mappers, Mapper
from sqlalchemy.orm.scoping import scoped_session
from sqlalchemy.orm.session import sessionmaker
from threading import current_thread, local
from weakref import ref

try: from urllib.parse import urlsplit
except ImportError: from urlparse import urlsplit

from dNG.pas.data.settings import Settings
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.runtime.instance_lock import InstanceLock
from dNG.pas.runtime.thread_lock import ThreadLock
from dNG.pas.runtime.type_exception import TypeException
from dNG.pas.runtime.value_exception import ValueException

class Connection(object):
#
	"""
"Connection" is a proxy for a SQLAlchemy session.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=unused-argument

	_event_bound = False
	"""
True if the SQLAlchemy "translate_row" event has been bound.
	"""
	_instance_lock = InstanceLock()
	"""
Thread safety lock
	"""
	_sa_scoped_session = None
	"""
SQLAlchemy scoped session instance
	"""
	_settings_initialized = False
	"""
True after the database settings have been read from file
	"""
	_serialized = True
	"""
Serialize access to the underlying database if true
	"""
	_serialized_lock = ThreadLock()
	"""
Thread safety lock
	"""
	_weakref_instance = None
	"""
Cache weakref instance
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Connection)

:since: v0.1.00
		"""

		self.local = None
		"""
Local data handle
		"""
		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)
		"""
The LogHandler is called whenever debug messages should be logged or errors
happened.
		"""
		self.session = None
		"""
SQLAlchemy session
		"""

		if ((not Connection._event_bound) or Connection._sa_scoped_session is None):
		#
			with Connection._instance_lock:
			# Thread safety
				if (Connection._sa_scoped_session is None):
				#
					engine = engine_from_config(Settings.get_dict(),
					                            prefix = "pas_database_sqlalchemy_",
					                            strategy = "threadlocal"
					                           )

					Connection._sa_scoped_session = scoped_session(sessionmaker(engine))
				#

				if (not Connection._event_bound):
				#
					listen(Mapper, "translate_row", Connection._sa_on_translate_row)
					Connection._event_bound = True
				#
			#
		#

		self.session = Connection._sa_scoped_session
		if (self.log_handler is not None and Settings.get("pas_database_threaded_debug", False)): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}.__init__()- reporting: Thread ID {1:d}", self, current_thread().ident, context = "pas_database")
	#

	def __del__(self):
	#
		"""
Destructor __del__(Connection)

:since: v0.1.00
		"""

		if (self.session is not None):
		#
			if (self.log_handler is not None):
			#
				if (len(self.session.deleted) > 0): self.log_handler.warning("{0!r} has deleted instances to be rolled back", self, context = "pas_database")
				if (len(self.session.dirty) > 0): self.log_handler.warning("{0!r} has dirty instances to be rolled back", self, context = "pas_database")
				if (len(self.session.new) > 0): self.log_handler.warning("{0!r} has new instances to be ignored", self, context = "pas_database")
			#

			self.session.remove()
		#
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		self._enter_context()
		return self
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v0.1.00
		"""

		self._exit_context(exc_type, exc_value, traceback)
		return False
	#

	def __getattr__(self, name):
	#
		"""
python.org: Called when an attribute lookup has not found the attribute in
the usual places (i.e. it is not an instance attribute nor is it found in the
class tree for self).

:param name: Attribute name

:return: (mixed) Session attribute
:since:  v0.1.00
		"""

		if (self.session is None or (not hasattr(self.session, name))): raise TypeException("SQLAlchemy session does not implement '{0}'".format(name))
		return getattr(self.session, name)
	#

	def begin(self):
	#
		"""
sqlalchemy.org: Begin a transaction on this Session.

:since: v0.1.00
		"""

		self._ensure_thread_local()

		# SQLAlchemy starts the most outer transaction itself by default
		if (self.local.transactions < 0): self.local.transactions = 0
		elif (Settings.get("pas_database_transaction_use_native_nested", False)): self.session.begin_nested()
		else: self.session.begin(subtransactions = True)

		self.local.transactions += 1
		if (self.log_handler is not None): self.log_handler.debug("{0!r} transaction '{1:d}' started", self, self.local.transactions, context = "pas_database")
	#

	def commit(self):
	#
		"""
sqlalchemy.org: Flush pending changes and commit the current transaction.

:since: v0.1.00
		"""

		self._ensure_thread_local()

		if (self.local.transactions > 0):
		#
			self.session.commit()

			if (self.log_handler is not None): self.log_handler.debug("{0!r} transaction '{1:d}' committed", self, self.local.transactions, context = "pas_database")

			if (self.local.transactions < 2): self.local.transactions = -1
			else: self.local.transactions -= 1
		#
	#

	def _ensure_thread_local(self):
	#
		"""
For thread safety some variables are defined per thread. This method makes
sure that these variables are defined.

:since: v0.1.00
		"""

		if (self.local is None): self.local = local()

		if (not hasattr(self.local, "transactions")):
		#
			self.local.context_depth = 0
			self.local.transactions = -1
		#
	#

	def _enter_context(self):
	#
		"""
Enters the connection context.

:since: v0.1.02
		"""

		if (self.log_handler is not None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._enter_context()- (#echo(__LINE__)#)", self, context = "pas_database")

		Connection._acquire()

		self._ensure_thread_local()
		self.local.context_depth += 1

		return self
	#

	def escape_like_condition(self, value):
	#
		"""
Escapes the given value to be used in like conditions based on the default
backslash escape character.

@TODO: Add database specific layer.

:param value: LIKE condition value

:return: (str) Escaped condition value
:since:  v0.1.02
		"""

		_return = value.replace("%", "\\%")
		_return = _return.replace("_", "\\_")

		return _return
	#

	def _exit_context(self, exc_type, exc_value, traceback):
	#
		"""
Exits the connection context.

:since: v0.1.02
		"""

		if (self.log_handler is not None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._exit_context()- (#echo(__LINE__)#)", self, context = "pas_database")

		self.local.context_depth -= 1

		try:
		#
			if (self.local.context_depth < 1 and self.get_transaction_depth() < 1):
			#
				if (exc_type is None and exc_value is None): self.session.commit()
				else: self.session.rollback()

				self.session.expunge_all()
				if (self.log_handler is not None): self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._exit_context()- reporting: Cleared session instances", self, context = "pas_database")
			#
		#
		finally: Connection._release()

		return False
	#

	def get_session(self):
	#
		"""
Returns the active SQLAlchemy session.

:since: v0.1.00
		"""

		return self.session
	#

	def get_transaction_depth(self):
	#
		"""
Returns the current transaction depth.

:since: v0.1.00
		"""

		self._ensure_thread_local()
		return (0 if (self.local.transactions < 0) else self.local.transactions)
	#

	def optimize(self, table):
	#
		"""
Optimizes the given database table.

:param table: SQLAlchemy table definition

:since: v0.1.00
		"""

		pass
	#

	def optimize_random(self, table):
	#
		"""
Optimizes the given database table randomly (1/10 of all calls).

:param table: SQLAlchemy table definition

:since: v0.1.00
		"""

		if (randrange(0, 10) < 1): self.optimize(table)
	#

	def rollback(self):
	#
		"""
sqlalchemy.org: Rollback the current transaction in progress.

:since: v0.1.00
		"""

		self._ensure_thread_local()

		if (self.local.transactions > 0):
		#
			if ((not Settings.get("pas_database_transaction_use_native_nested", False)) and self.local.transactions > 1):
			#
				for _ in range(1, self.local.transactions): self.session.rollback()
				self.local.transactions = 1
			#

			self.session.rollback()

			if (self.log_handler is not None): self.log_handler.debug("{0!r} transaction '{1:d}' rolled back", self, self.local.transactions, context = "pas_database")

			if (self.local.transactions < 2): self.local.transactions = -1
			else: self.local.transactions -= 1
		#
	#

	@staticmethod
	def _acquire():
	#
		"""
Acquires a lock if the database should not be accessed from multiple
threads at once (serialized mode).

:since: v0.1.00
		"""

		if (Connection.is_serialized()): Connection._serialized_lock.acquire()
	#

	@staticmethod
	def _ensure_settings():
	#
		"""
Check and read settings if needed.

:since: v0.1.00
		"""

		if (not Connection._settings_initialized):
		#
			with Connection._serialized_lock:
			# Thread safety
				if (not Connection._settings_initialized):
				#
					Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)

					if (not Settings.is_defined("pas_database_url")): raise ValueException("Minimum database configuration missing")
					url = Settings.get("pas_database_url").replace("__path_base__", path.abspath(Settings.get("path_base")))

					if (not Settings.is_defined("pas_database_table_prefix")): Settings.set("pas_database_table_prefix", "pas")
					Connection._serialized = (not Settings.get("pas_database_threaded", True))
					if (Connection._serialized): Connection._serialized_lock.set_timeout(Settings.get("pas_database_lock_timeout", 30))

					url_elements = urlsplit(url)

					Settings.set("pas_database_backend_name", url_elements.scheme.split("+")[0])

					if (url_elements.username is None
					    and url_elements.password is None
					    and Settings.is_defined("pas_database_user")
					    and Settings.is_defined("pas_database_password")
					   ):
					#
						url = "{0}://{1}:{2}@{3}{4}".format(url_elements.scheme,
						                                    Settings.get("pas_database_user"),
						                                    Settings.get("pas_database_password"),
						                                    url_elements.hostname,
						                                    url_elements.path
						                                   )

						if (url_elements.query != ""): url += "?{0}".format(url_elements.query)
						if (url_elements.fragment != ""): url += "#{0}".format(url_elements.fragment)
					#

					Settings.set("pas_database_sqlalchemy_url", url)

					Connection._settings_initialized = True
				#
			#
		#
	#

	@staticmethod
	def get_backend_name():
	#
		"""
Returns the connection backend.

:return: (str) Database backend
:since:  v0.1.00
		"""

		if (not Connection._settings_initialized): Connection._ensure_settings()
		return Settings.get("pas_database_backend_name")
	#

	@staticmethod
	def get_instance():
	#
		"""
Get the Connection singleton.

:return: (Connection) Object on success
:since:  v0.1.00
		"""

		_return = None

		if (Connection._weakref_instance is not None): _return = Connection._weakref_instance()

		if (_return is None):
		#
			Connection._ensure_settings()

			_return = Connection()
			Connection._weakref_instance = ref(_return)
		#

		return _return
	#

	@staticmethod
	def get_table_prefix():
	#
		"""
Get the configured database table prefix.

:return: (str) Table prefix
:since:  v0.1.00
		"""

		if (not Connection._settings_initialized): Connection._ensure_settings()
		return Settings.get("pas_database_table_prefix")
	#

	@staticmethod
	def is_serialized():
	#
		"""
Returns true if access to a database instance is serialized.

:return: (bool) True for serialized access
:since:  v0.1.00
		"""

		if (not Connection._settings_initialized): Connection._ensure_settings()
		return Connection._serialized
	#

	@staticmethod
	def _release():
	#
		"""
Releases a previously acquired lock.

:since: v0.1.00
		"""

		if (Connection.is_serialized()): Connection._serialized_lock.release()
	#

	@staticmethod
	def _sa_on_translate_row(mapper, context, row):
	#
		"""
sqlalchemy.org: Perform pre-processing on the given result row and return a
new row instance.

:param mapper: The Mapper which is the target of this event.
:param context: The QueryContext, which includes a handle to the current
                Query in progress as well as additional state information.
:param mapper: The result row being handled.

:since:  v0.1.00
		"""

		if (mapper.polymorphic_map is not None and mapper.polymorphic_on in row):
		#
			common_name = "dNG.pas.database.instances.{0}".format(row[mapper.polymorphic_on])
			if ((not NamedLoader.is_defined(common_name, False)) and NamedLoader.is_defined(common_name)): configure_mappers()
		#

		return EXT_CONTINUE
	#
#

##j## EOF