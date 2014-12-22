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

from time import time
from uuid import uuid4 as uuid

from sqlalchemy.schema import Column
from sqlalchemy.types import BIGINT, VARCHAR

from dNG.pas.database.types.date_time import DateTime
from .abstract import Abstract

class SchemaVersion(Abstract):
#
	"""
Database table listing the current schema version.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=invalid-name

	__tablename__ = "{0}_schema_version".format(Abstract.get_table_prefix())
	"""
SQLAlchemy table name
	"""
	db_instance_class = "dNG.pas.database.Schema"
	"""
Encapsulating SQLAlchemy database instance class name
	"""
	db_schema_version = 1
	"""
Database schema version
	"""

	id = Column(VARCHAR(32), primary_key = True)
	"""
schema_version.id
	"""
	name = Column(VARCHAR(255), index = True, nullable = False)
	"""
schema_version.name
	"""
	version = Column(BIGINT, nullable = False)
	"""
schema_version.version
	"""
	applied = Column(DateTime, default = 0, nullable = False)
	"""
schema_version.applied
	"""

	def __init__(self, *args, **kwargs):
	#
		"""
Constructor __init__(SchemaVersion)

:since: v0.1.00
		"""

		Abstract.__init__(self, *args, **kwargs)
		if (self.id is None): self.id = uuid().hex
		if (self.applied is None): self.applied = int(time())
	#
#

##j## EOF