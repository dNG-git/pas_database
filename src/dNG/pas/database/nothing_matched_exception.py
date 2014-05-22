# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.NothingMatchedException
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

from dNG.pas.runtime.value_exception import ValueException

class NothingMatchedException(ValueException):
#
	"""
"NothingMatchedException" is raised when no entry matched the given
query condition.

:author:     direct Netware Group
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.01
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	def __init__(self, value = "Nothing matched", _exception = None):
	#
		"""
Constructor __init__(NothingMatchedException)

:param value: Exception message value
:param _exception: Inner exception

:since: v0.1.01
		"""

		ValueException.__init__(self, value, _exception)
	#
#

##j## EOF