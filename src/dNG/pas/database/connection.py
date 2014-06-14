# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.Connection
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

# pylint: disable=import-error,no-name-in-module

from os import path
from random import randrange
from sqlalchemy.engine import engine_from_config
from sqlalchemy.event import listen
from sqlalchemy.orm.interfaces import EXT_CONTINUE
from sqlalchemy.orm.mapper import configure_mappers, Mapper
from sqlalchemy.orm.session import sessionmaker
from threading import local
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
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=unused-argument

	_event_bound = False
	"""
True if the SQLAlchemy "translate_row" event has been bound.
	"""
	_event_lock = InstanceLock()
	"""
Thread safety lock
	"""
	_local = local()
	"""
thread-local instance
	"""
	_sa_sessionmaker = None
	"""
Scoped database session
	"""
	_serialized_lock = ThreadLock()
	"""
Thread safety lock
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Connection)

:since: v0.1.00
		"""

		self.context_depth = 0
		"""
Runtime context depth
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
		self.session = None
		"""
SQLAlchemy session
		"""
		self.transactions = -1
		"""
Number of active transactions
		"""

		Connection._ensure_thread_local()

		if (Connection._sa_sessionmaker == None):
		#
			engine = engine_from_config(Connection._local.settings, prefix = "pas_database_sqlalchemy_", strategy = "threadlocal")
			Connection._sa_sessionmaker = (sessionmaker(engine, autoflush = False) if (Connection._local.serialized) else sessionmaker(engine))
		#

		self.session = Connection._sa_sessionmaker()

		if (not Connection._event_bound):
		# Thread safety
			with Connection._event_lock:
			#
				if (not Connection._event_bound):
				#
					listen(Mapper, "translate_row", Connection._sa_on_translate_row)
					Connection._event_bound = True
				#
			#
		#
	#

	def __del__(self):
	#
		"""
Destructor __del__(Connection)

:since: v0.1.00
		"""

		# pylint: disable=broad-except

		if (self.session != None):
		# SQLAlchemy starts the most outer transaction itself by default
			if (self.transactions < 0):
			#
				Connection._ensure_thread_local()
				Connection._acquire()

				try: self.session.commit()
				except Exception as handled_exception:
				#
					if (self.log_handler != None): self.log_handler.error(handled_exception)
				#
				finally: Connection._release()
			#

			self.session.close()
		#
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		Connection._acquire()

		try:
		#
			with self._lock:
			#
				if ((self.context_depth + self.transactions) < 0):
				#
					self.begin()
					self.context_depth = 1
				#
				elif (self.context_depth > 0): self.context_depth += 1
			#
		#
		except Exception:
		#
			Connection._release()
			raise
		#

		return self
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:since: v0.1.00
		"""

		# pylint: disable=broad-except

		if (self.context_depth > 0):
		# Thread safety
			with self._lock:
			#
				if (self.context_depth > 0):
				#
					self.context_depth -= 1

					if (self.context_depth < 1):
					#
						try:
						#
							if (exc_type == None and exc_value == None): self.commit()
							else: self.rollback()
						#
						except Exception as handled_exception:
						#
							if (self.log_handler != None): self.log_handler.error(handled_exception)
							if (exc_type == None and exc_value == None): self.rollback()
						#
					#
				#
			#
		#

		Connection._release()
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

		if (self.session == None or (not hasattr(self.session, name))): raise TypeException("SQLAlchemy session does not implement '{0}'".format(name))
		return getattr(self.session, name)
	#

	def begin(self):
	#
		"""
sqlalchemy.org: Begin a transaction on this Session.

:since: v0.1.00
		"""

		with self._lock:
		# SQLAlchemy starts the most outer transaction itself by default
			if (self.transactions < 0): self.transactions = 0
			elif (Connection._local.settings.get("pas_database_transaction_use_native_nested", False)): self.session.begin_nested()
			else: self.session.begin(subtransactions = True)

			self.transactions += 1
			if (self.log_handler != None): self.log_handler.debug("pas.database.Connection transaction '{0:d}' started".format(self.transactions))
		#
	#

	def commit(self):
	#
		"""
sqlalchemy.org: Flush pending changes and commit the current transaction.

:since: v0.1.00
		"""

		if (self.transactions > 0):
		# Thread safety
			with self._lock:
			#
				if (self.transactions > 0):
				#
					self.session.commit()

					if (self.log_handler != None): self.log_handler.debug("pas.database.Connection transaction '{0:d}' committed".format(self.transactions))

					if (self.transactions < 2): self.transactions = -1
					else: self.transactions -= 1
				#
			#
		#
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

		return (0 if (self.transactions < 0) else self.transactions)
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

		if (self.transactions > 0):
		# Thread safety
			with self._lock:
			#
				if (self.transactions > 0):
				#
					if ((not Connection._local.settings.get("pas_database_transaction_use_native_nested", False)) and self.transactions > 1):
					#
						for _ in range(1, self.transactions): self.session.rollback()
						self.transactions = 1
					#

					self.session.rollback()

					if (self.log_handler != None): self.log_handler.debug("pas.database.Connection transaction '{0:d}' rolled back".format(self.transactions))

					if (self.transactions < 2): self.transactions = -1
					else: self.transactions -= 1
				#
			#
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

		if (Connection._local.serialized): Connection._serialized_lock.acquire()
	#

	@staticmethod
	def _ensure_thread_local():
	#
		"""
For thread safety some variables are defined per thread. This method makes
sure that these variables are defined.

:since: v0.1.00
		"""

		if (not hasattr(Connection._local, "settings")):
		#
			Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)
			Connection._local.settings = Settings.get_dict()

			if ("pas_database_url" not in Connection._local.settings): raise ValueException("Minimum database configuration missing")
			Connection._local.settings['pas_database_url'] = Connection._local.settings['pas_database_url'].replace("[rewrite]path_base[/rewrite]", path.abspath(Connection._local.settings.get("path_base")))

			if ("pas_database_table_prefix" not in Connection._local.settings): Connection._local.settings['pas_database_table_prefix'] = "pas"
			Connection._local.serialized = ("pas_database_threaded" in Connection._local.settings and (not Connection._local.settings['pas_database_threaded']))
			if (Connection._local.serialized): Connection._serialized_lock.set_timeout(Connection._local.settings.get("pas_database_lock_timeout", 30))

			url_elements = urlsplit(Connection._local.settings['pas_database_url'])

			Connection._local.settings['pas_database_backend_name'] = url_elements.scheme.split("+")[0]

			if (url_elements.username == None
			    and url_elements.password == None
			    and "pas_database_user" in Connection._local.settings
			    and "pas_database_password" in Connection._local.settings
			   ):
			#
				url = "{0}://{1}:{2}@{3}{4}".format(url_elements.scheme,
				                                    Connection._local.settings['pas_database_user'],
				                                    Connection._local.settings['pas_database_password'],
				                                    url_elements.hostname,
				                                    url_elements.path
				                                   )

				Connection._local.settings['pas_database_sqlalchemy_url'] = url

				if (url_elements.query != ""): Connection._local.settings['pas_database_sqlalchemy_url'] += "?{0}".format(url_elements.query)
				if (url_elements.fragment != ""): Connection._local.settings['pas_database_sqlalchemy_url'] += "#{0}".format(url_elements.fragment)
			#
			else: Connection._local.settings['pas_database_sqlalchemy_url'] = Connection._local.settings['pas_database_url']
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

		Connection._ensure_thread_local()
		return Connection._local.settings['pas_database_backend_name']
	#

	@staticmethod
	def get_instance():
	#
		"""
Get the Connection thread-local singleton.

:return: (Connection) Object on success
:since:  v0.1.00
		"""

		_return = None

		if (hasattr(Connection._local, "weakref_instance")): _return = Connection._local.weakref_instance()

		if (_return == None):
		#
			_return = Connection()
			Connection._local.weakref_instance = ref(_return)
		#

		return _return
	#

	@staticmethod
	def get_settings():
	#
		"""
Returns the settings used for the database instance.

:return: (dict) Database settings
:since:  v0.1.00
		"""

		Connection._ensure_thread_local()
		return Connection._local.settings
	#

	@staticmethod
	def is_serialized():
	#
		"""
Returns true if access to a database instance is serialized.

:return: (bool) True for serialized access
:since:  v0.1.00
		"""

		Connection._ensure_thread_local()
		return Connection._local.serialized
	#

	@staticmethod
	def _release():
	#
		"""
Releases a previously acquired lock.

:since: v0.1.00
		"""

		if (Connection._local.serialized): Connection._serialized_lock.release()
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

		if (mapper.polymorphic_map != None and mapper.polymorphic_on in row):
		#
			common_name = "dNG.pas.database.instances.{0}".format(row[mapper.polymorphic_on])
			if ((not NamedLoader.is_defined(common_name, False)) and NamedLoader.is_defined(common_name)): configure_mappers()
		#

		return EXT_CONTINUE
	#
#

##j## EOF