# -*- coding: utf-8 -*-
##j## BOF

"""
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
"""

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import reconstructor

from dNG.pas.database.connection import Connection

class Abstract(declarative_base()):
#
	"""
This class provides abstract SQLAlchemy database instances.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=unused-argument

	__abstract__ = True
	"""
sqlalchemy.org: "__abstract__" causes declarative to skip the production
of a table or mapper for the class entirely.
	"""
	db_instance_class = None
	"""
Encapsulating SQLAlchemy database instance class name
	"""

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

		return Connection.get_settings().get("pas_database_table_prefix")
	#
#

##j## EOF