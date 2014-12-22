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

from dNG.pas.database.connection import Connection
from dNG.pas.runtime.iterator import Iterator

class InstanceIterator(Iterator):
#
	"""
"InstanceIterator" provides an instance wrapping iterator to encapsulate
SQLAlchemy database instances with an given class.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	def __init__(self, entity, cursor, buffered = False, instance_class = None, *args, **kwargs):
	#
		"""
Constructor __init__(InstanceIterator)

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor
:param buffered: True to buffer the result
:param instance_class: Encapsulating database instance class

:since: v0.1.00
		"""

		self.args = args
		"""
Arguments given to the contructor of the encapsulating database instance
		"""
		self.buffered = buffered
		"""
True if the results are buffered in a list.
		"""
		self.instance_class = instance_class
		"""
Instance class encapsulating the database instance
		"""
		self.kwargs = kwargs
		"""
Keyword arguments given to the contructor of the encapsulating database
instance
		"""
		self.result = None
		"""
Instance class encapsulating the database instance
		"""

		with Connection.get_instance() as connection:
		#
			self.result = (list(connection.query(entity).instances(cursor))
			               if (buffered) else
			               connection.query(entity).instances(cursor)
			              )
		#
	#

	def __iter__(self):
	#
		"""
python.org: Return an iterator object.

:return: (object) Iterator object
:since:  v0.1.00
		"""

		return (iter(self.result) if (self.instance_class is None) else self)
	#

	def __next__(self):
	#
		"""
python.org: Return the next item from the container.

:return: (object) Result object
:since:  v0.1.00
		"""

		if (self.buffered):
		#
			if (len(self.result) < 1): raise StopIteration()
			_return = (self.result.pop(0) if (self.instance_class is None) else self.instance_class(self.result.pop(0), *self.args, **self.kwargs))
		#
		else: _return = (next(self.result) if (self.instance_class is None) else self.instance_class(next(self.result), *self.args, **self.kwargs))

		return _return
	#
#

##j## EOF