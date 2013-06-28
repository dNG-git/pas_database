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
http://www.direct-netware.de/redirect.py?pas;db

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
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import reconstructor

from dNG.pas.module.named_loader import NamedLoader
from .connection import Connection

class Instance(declarative_base()):
#
	"""
"Instance" is an abstract SQLAlchemy object.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: db
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	__abstract__ = True
	"""
sqlalchemy.org: "__abstract__" causes declarative to skip the production
of a table or mapper for the class entirely.
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
	#

	def insert(self):
	#
		"""
Insert the instance into the database.

:param count: Count "get()" request

:access: protected
:since:  v0.1.00
		"""

		database = Connection.get_instance()
		database.add(self)
	#

	def is_known(self):
	#
		"""
Returns true if the instance is already saved in the database.

:return: (bool) True if known
:since:  v0.1.00
		"""

		return inspect(self).has_identity
	#

	def reload(self):
	#
		"""
Reload instance data from the database.

:since: v0.1.00
		"""

		database = Connection.get_instance()
		if (not inspect(self).detached): database.refresh(self)
	#

	def save(self):
	#
		"""
Saves changes of the instance into the database.

:since: v0.1.00
		"""

		if (self.is_known()): self.update()
		else: self.insert()
	#

	def update(self):
	#
		"""
Updates the instance already saved to the database.

:access: protected
:since:  v0.1.00
		"""

		if (inspect(self).detached):
		#
			database = Connection.get_instance()
			database.add(self)
		#
	#

	@reconstructor
	def sa_reconstructor(self):
	#
		"""
sqlalchemy.org: Designates a method as the "reconstructor", an __init__-like
method that will be called by the ORM after the instance has been loaded
from the database or otherwise reconstituted.

:since: v0.1.00
		"""

		self.__init__()
	#

	@staticmethod
	def get_table_prefix():
	#
		"""
Get the configured database table prefix.

:return: (str) Table prefix
:since:  v0.1.00
		"""

		settings = Connection.get_settings()
		return settings['table_prefix']
	#
#

##j## EOF