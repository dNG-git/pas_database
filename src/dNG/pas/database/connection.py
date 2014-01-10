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

from os import path
from random import randrange
from sqlalchemy import engine_from_config
from sqlalchemy.orm import sessionmaker
from threading import local
from weakref import ref
import re

from dNG.pas.data.settings import Settings
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hooks import Hooks
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

	local = local()
	"""
thread-local instance
	"""
	session = None
	"""
Scoped database session
	"""
	serialized_lock = ThreadLock()
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
		self.lock = ThreadLock()
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

		Connection._thread_local_check()

		if (Connection.session == None):
		#
			engine = engine_from_config(Connection.local.settings, prefix = "pas_database_sqlalchemy_", strategy = "threadlocal")
			Connection.session = (sessionmaker(engine, autoflush = False) if (Connection.local.serialized) else sessionmaker(engine))
		#

		self.session = Connection.session()
	#

	def __del__(self):
	#
		"""
Destructor __del__(Connection)

:since: v0.1.00
		"""

		if (self.session != None):
		# SQLAlchemy starts the most outer transaction itself by default
			if (self.transactions < 0):
			#
				Connection._thread_local_check()
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
			with self.lock:
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

		with self.lock:
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

		if (self.session == None or (not hasattr(self.session, name))): raise TypeException("SQLalchemy session does not implement '{0}'".format(name))
		return getattr(self.session, name)
	#

	def begin(self):
	#
		"""
sqlalchemy.org: Begin a transaction on this Session.

:since: v0.1.00
		"""

		with self.lock:
		# SQLAlchemy starts the most outer transaction itself by default
			if (self.transactions < 0): self.transactions = 0
			elif (Connection.local.settings.get("pas_database_transaction_use_native_nested", False)): self.session.begin_nested()
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

		with self.lock:
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

		with self.lock:
		#
			if (self.transactions > 0):
			#
				if ((not Connection.local.settings.get("pas_database_transaction_use_native_nested", False)) and self.transactions > 1):
				#
					for i in range(1, self.transactions): self.session.rollback()
					self.transactions = 1
				#

				self.session.rollback()

				if (self.log_handler != None): self.log_handler.debug("pas.database.Connection transaction '{0:d}' rolled back".format(self.transactions))

				if (self.transactions < 2): self.transactions = -1
				else: self.transactions -= 1
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

		if (Connection.local.serialized): Connection.serialized_lock.acquire()
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

		if (hasattr(Connection.local, "weakref_instance")): _return = Connection.local.weakref_instance()
		else:
		#
			with Connection.serialized_lock:
			#
				if (Connection.session == None): Hooks.load("database")
			#
		#

		if (_return == None):
		#
			_return = Connection()
			Connection.local.weakref_instance = ref(_return)
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

		Connection._thread_local_check()
		return Connection.local.settings
	#

	@staticmethod
	def is_serialized():
	#
		"""
Returns true if access to a database instance is serialized.

:return: (bool) True for serialized access
:since:  v0.1.00
		"""

		Connection._thread_local_check()
		return Connection.local.serialized
	#

	@staticmethod
	def _release():
	#
		"""
Releases a previously acquired lock.

:since: v0.1.00
		"""

		if (Connection.local.serialized): Connection.serialized_lock.release()
	#

	@staticmethod
	def _thread_local_check():
	#
		"""
For thread safety some variables are defined per thread. This method makes
sure that these variables are defined.

:since: v0.1.00
		"""

		if (not hasattr(Connection.local, "settings")):
		#
			Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)
			Connection.local.settings = Settings.get_instance()

			if ("pas_database_url" not in Connection.local.settings): raise ValueException("Minimum database configuration missing")
			Connection.local.settings['pas_database_url'] = Connection.local.settings['pas_database_url'].replace("[rewrite]path_base[/rewrite]", path.abspath(Connection.local.settings.get("path_base")))

			if ("pas_database_table_prefix" not in Connection.local.settings): Connection.local.settings['pas_database_table_prefix'] = "pas"
			Connection.local.serialized = ("pas_database_threaded" in Connection.local.settings and (not Connection.local.settings['pas_database_threaded']))
			if (Connection.local.serialized): Connection.serialized_lock.set_timeout(Connection.local.settings.get("pas_database_lock_timeout", 30))

			if ("@" in Connection.local.settings['pas_database_url'] or "password" not in Connection.local.settings or "user" not in Connection.local.settings): Connection.local.settings['pas_database_sqlalchemy_url'] = Connection.local.settings['pas_database_url']
			else: Connection.local.settings['pas_database_sqlalchemy_url'] = re.sub("^(.+)\\://(.*)$", "\\1://{0}:{1}@\\2".format(Connection.local.settings['pas_database_user'], Connection.local.settings['pas_database_password']), Connection.local.settings['pas_database_url'])
		#
	#
#

##j## EOF