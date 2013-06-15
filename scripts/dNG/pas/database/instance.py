# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.instance
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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from threading import RLock
import re

from dNG.pas.data.settings import Settings
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hooks import Hooks

class Instance(object):
#
	"""
"Instance" is a proxy for an SQLAlchemy Engine.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: db
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	base_instance = None
	"""
SQLAlchemy base instance
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
	transactions = 0
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
		self.scoped_session_factory = scoped_session(sessionmaker(engine_from_config(Instance.settings, prefix = "sqlalchemy_")))
		"""
Thread local session factory
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

		with Instance.synchronized:
		#
			if (self.scoped_session_factory != None): self.scoped_session_factory.remove()
		#
	#

	def begin(self):
	#
		"""
sqlalchemy.org: Begin a transaction on this Session.

:since: v0.1.00
		"""

		with Instance.synchronized:
		# SQLAlchemy starts the most outer transaction itself by default
			if (self.transactions == 0 or (not Instance.settings.get("pas_db_transaction_use_native_nested", False))): self.session.begin(subtransactions = True)
			else: self.session.begin_nested()

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

		with Instance.synchronized:
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

		with Instance.synchronized:
		#
			if (Instance.instance != None):
			#
				if (Instance.ref_count > 0): Instance.ref_count -= 1
				if (Instance.ref_count == 0): Instance.instance = None
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

		with Instance.synchronized:
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
	def get_base_layout():
	#
		"""
Get the base database table layout class.

:return: (object) Python class
:since:  v0.1.00
		"""

		if (Instance.base_instance == None): Instance.base_instance = declarative_base()
		return Instance.base_instance
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

		with Instance.synchronized:
		#
			if (Instance.instance == None):
			#
				Instance.get_settings()
				Instance.instance = Instance()
			#

			if (count): Instance.ref_count += 1
		#

		return Instance.instance
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

		with Instance.synchronized:
		#
			if (refresh): Instance.settings = { }

			if (len(Instance.settings) < 1):
			#
				Hooks.load("db")

				Settings.read_file("{0}/settings/pas_db.json".format(Settings.get("path_data")), True)
				settings = Settings.get_instance()

				for key in settings:
				#
					if (key == "pas_db_url"): Instance.settings['url'] = settings['pas_db_url'].replace("[path_base]", path.abspath(Settings.get("path_base")))
					if (key.startswith("pas_db_")): Instance.settings[key[7:]] = settings[key]
				#

				if ("url" not in Instance.settings): raise RuntimeError("Minimum database configuration missing", 38)

				if ("table_prefix" not in Instance.settings): Instance.settings['table_prefix'] = "pas"

				if ("@" in Instance.settings['url'] or "password" not in Instance.settings or "user" not in Instance.settings): Instance.settings['sqlalchemy_url'] = Instance.settings['url']
				else: Instance.settings['sqlalchemy_url'] = re.sub("^(.+)\\://(.*)$", "\\1://{0}:{1}@\\2".format(Instance.settings['user'], Instance.settings['password']), Instance.settings['url'])
			#
		#

		return Instance.settings
	#

	@staticmethod
	def get_table_prefix():
	#
		"""
Get the configured database table prefix.

:return: (str) Table prefix
:since:  v0.1.00
		"""

		settings = Instance.get_settings()
		return settings['table_prefix']
	#
#

##j## EOF