# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.SortDefinition
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

from dNG.pas.runtime.type_exception import TypeException
from dNG.pas.runtime.value_exception import ValueException

class SortDefinition(object):
#
	"""
"SortDefinition" is an abstracted .

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	ASCENDING = 1
	"""
Ascending sort direction
	"""
	DESCENDING = 2
	"""
Descending sort direction
	"""

	def __init__(self, sort_tuples = None):
	#
		"""
Constructor __init__(SortDefinition)

:since: v0.1.00
		"""

		self.sort_tuples = [ ]
		"""
List of tuples defining the attribute and sort direction
		"""

		if (sort_tuples == None): sort_tuples = [ ]

		for sort_tuple in sort_tuples:
		#
			if (type(sort_tuple) != tuple): raise TypeException("Sort definition data type given is not supported")
			if (len(sort_tuple) != 2): raise ValueException("Sort definition given is not supported")

			self.append(sort_tuple[0], sort_tuple[1])
		#
	#

	def get_list(self):
	#
		"""
Returns the current sort definition list.

:return: (list) Sort definition list
:since:  v0.1.00
		"""

		return self.sort_tuples
	#

	def append(self, attribute, direction):
	#
		"""
Appends a sort definition to the current list.

:param attribute: Database entity attribute
:param direction: Sort direction

:since: v0.1.00
		"""

		SortDefinition.validate_sort_direction(direction)
		self.sort_tuples.append(( attribute, direction ))
	#

	def clear(self):
	#
		"""
Clears the current list.

:since: v0.1.00
		"""

		self.sort_tuples = [ ]
	#

	def prepend(self, attribute, direction):
	#
		"""
Prepends a sort definition to the current list.

:param attribute: Database entity attribute
:param direction: Sort direction

:since: v0.1.00
		"""

		SortDefinition.validate_sort_direction(direction)
		self.sort_tuples.insert(0, ( attribute, direction ))
	#

	@staticmethod
	def validate_sort_direction(direction):
	#
		"""
Validates the given sort direction.
		"""

		if (direction != SortDefinition.ASCENDING
		    and direction != SortDefinition.DESCENDING
		   ): raise ValueException("Sort definition given is not supported")
	#
#

##j## EOF