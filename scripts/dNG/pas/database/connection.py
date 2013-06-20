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
http://www.direct-netware.de/redirect.py?pas;db

This Source Code Form is subject to the terms of the Mozilla Public License,
v. 2.0. If a copy of the MPL was not distributed with this file, You can
obtain one at http://mozilla.org/MPL/2.0/.
----------------------------------------------------------------------------
http://www.direct-netware.de/redirect.py?licenses;mpl2
----------------------------------------------------------------------------
#echo(pasDbVersion)#
#echo(__FILEPATH__)#
----------------------------------------------------------------------------
NOTE_END //n"""

from os import path
from random import randrange
from sqlalchemy import engine_from_config
from sqlalchemy.orm import scoped_session, sessionmaker
from threading import RLock
import re

from dNG.pas.data.settings import Settings
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hooks import Hooks

class Connection(object):
#
	"""
"Connection" is a proxy for the SQLAlchemy Engine.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: db
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	instance = None
	"""
Class instance
	"""
	ref_count = 0
	"""
Instances used
	"""
	settings = { }
	"""
SQLAlchemy configuration
	"""
	synchronized = RLock()
	"""
Lock used in multi thread environments.
	"""

	def __init__(self):
	#
		"""
Constructor __init__(Instance)

:since: v0.1.00
		"""

		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)
		"""
The log_handler is called whenever debug messages should be logged or errors
happened.
		"""
		self.session = None
		"""
SQLAlchemy session
		"""
		self.scoped_session_factory = scoped_session(sessionmaker(engine_from_config(Connection.settings, prefix = "sqlalchemy_", echo = "debug")))
		"""
Thread local session factory
		"""
		self.transactions = 0
		"""
SQLAlchemy configuration
		"""

		self.session = self.scoped_session_factory()
	#

	def __del__(self):
	#
		"""
Destructor __del__(Instance)

:since: v0.1.00
		"""

		if (self.log_handler != None): self.log_handler.return_instance()

		self.session.close()
		if (self.scoped_session_factory != None): self.scoped_session_factory.remove()
	#

	def __getattr__(self, name):
	#
		"""
python.org: Called when an attribute lookup has not found the attribute in
the usual places (i.e. it is not an instance attribute nor is it found in the
class tree for self).

:param name: Attribute name

:return: (callable) Static adapter method
:since:  v0.1.00
		"""

		if (self.session != None and hasattr(self.session, name)): return getattr(self.session, name)
		else: raise AttributeError("Session does not implement '{0}'".format(name))
	#

	def begin(self):
	#
		"""
sqlalchemy.org: Begin a transaction on this Session.

:since: v0.1.00
		"""

		with Connection.synchronized:
		#
			if (self.transactions > 0):
			# SQLAlchemy starts the most outer transaction itself by default
				if (Connection.settings.get("pas_db_transaction_use_native_nested", True)): self.session.begin_nested()
				else: self.session.begin(subtransactions = True)
			#

			self.transactions += 1
			if (self.log_handler != None): self.log_handler.debug("pas.db transaction '{0:d}' started".format(self.transactions))
		#
	#

	def commit(self):
	#
		"""
sqlalchemy.org: Flush pending changes and commit the current transaction.

:since: v0.1.00
		"""

		with Connection.synchronized:
		#
			if (self.transactions > 0):
			#
				self.session.commit()
				if (self.log_handler != None): self.log_handler.debug("pas.db transaction '{0:d}' committed".format(self.transactions))
				self.transactions -= 1
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

		return self.transactions
	#

	def return_instance(self):
	#
		"""
The last "return_instance()" call will free the singleton reference.

:since: v0.1.00
		"""

		with Connection.synchronized:
		#
			if (Connection.instance != None):
			#
				if (Connection.ref_count > 0): Connection.ref_count -= 1
				if (Connection.ref_count == 0): Connection.instance = None
			#
		#
	#

	def optimize(self, table):
	#
		"""
Optimizes the given database table.

:since: v0.1.00
		"""

		pass
	#

	def optimize_random(self, table):
	#
		"""
Optimizes the given database table randomly (1/10 of all calls).

:since: v0.1.00
		"""

		if (randrange(0, 10) < 1): pass
	#

	def rollback(self):
	#
		"""
sqlalchemy.org: Rollback the current transaction in progress.

:since: v0.1.00
		"""

		with Connection.synchronized:
		#
			if (self.transactions > 0):
			#
				self.session.rollback()
				if (self.log_handler != None): self.log_handler.debug("pas.db transaction '{0:d}' rolled back".format(self.transactions))
				self.transactions -= 1
			#
		#
	#

	@staticmethod
	def get_instance(count = True):
	#
		"""
Get the database singleton.

:param count: Count "get()" request

:return: (Instance) Object on success
:since:  v0.1.00
		"""

		with Connection.synchronized:
		#
			if (Connection.instance == None):
			#
				Connection.get_settings()
				Connection.instance = Connection()
			#

			if (count): Connection.ref_count += 1
		#

		return Connection.instance
	#

	@staticmethod
	def get_settings(refresh = False):
	#
		"""
Get the configured database table prefix.

:param refresh: True to refresh settings

:return: (dict) Configured database settings
:since:  v0.1.00
		"""

		with Connection.synchronized:
		#
			if (refresh): Connection.settings = { }

			if (len(Connection.settings) < 1):
			#
				Hooks.load("db")

				Settings.read_file("{0}/settings/pas_db.json".format(Settings.get("path_data")), True)
				settings = Settings.get_instance()

				for key in settings:
				#
					if (key == "pas_db_url"): Connection.settings['url'] = settings['pas_db_url'].replace("[path_base]", path.abspath(Settings.get("path_base")))
					if (key.startswith("pas_db_")): Connection.settings[key[7:]] = settings[key]
				#

				if ("url" not in Connection.settings): raise RuntimeError("Minimum database configuration missing", 38)

				if ("table_prefix" not in Connection.settings): Connection.settings['table_prefix'] = "pas"

				if ("@" in Connection.settings['url'] or "password" not in Connection.settings or "user" not in Connection.settings): Connection.settings['sqlalchemy_url'] = Connection.settings['url']
				else: Connection.settings['sqlalchemy_url'] = re.sub("^(.+)\\://(.*)$", "\\1://{0}:{1}@\\2".format(Connection.settings['user'], Connection.settings['password']), Connection.settings['url'])
			#
		#

		return Connection.settings
	#
#

##j## EOF