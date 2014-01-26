# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.instances.Abstract
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

from sqlalchemy.event import listen
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import configure_mappers, reconstructor
from sqlalchemy.orm.interfaces import EXT_CONTINUE
from sqlalchemy.orm.mapper import Mapper

from dNG.pas.database.connection import Connection
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.runtime.instance_lock import InstanceLock

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

	__abstract__ = True
	"""
sqlalchemy.org: "__abstract__" causes declarative to skip the production
of a table or mapper for the class entirely.
	"""
	db_instance_class = None
	"""
Encapsulating SQLAlchemy database instance class name
	"""
	event_bound = False
	"""
True if the SQLAlchemy "translate_row" event has been bound.
	"""
	lock = InstanceLock()
	"""
Thread safety lock
	"""

	def __init__(self, *args, **kwargs):
	#
		"""
Constructor __init__(Abstract)

:since: v0.1.00
		"""

		super(Abstract, self).__init__(*args, **kwargs)

		if (not Abstract.event_bound):
		#
			# Event could be bound in another thread so check again
			with Abstract.lock:
			#
				if (not Abstract.event_bound):
				#
					listen(Mapper, "translate_row", Abstract.on_translate_row)
					Abstract.event_bound = True
				#
			#
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

		return Connection.get_settings().get("pas_database_table_prefix")
	#

	@staticmethod
	def on_translate_row(mapper, context, row):
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