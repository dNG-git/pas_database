# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.TransactionContext
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

from dNG.pas.data.logging.log_line import LogLine
from .connection import Connection

class TransactionContext(object):
#
	"""
"TransactionContext" provides an SQLAlchemy based ContextManager for
transactions.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	def __init__(self):
	#
		"""
Constructor __init__(TransactionContext)

:since: v0.1.00
		"""

		self.context_depth = 0
		"""
Runtime context depth
		"""
		self._database = None
		"""
Database connection if bound
		"""
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		# pylint: disable=broad-except,protected-access

		self._database = Connection.get_instance()
		Connection._acquire()

		try:
		#
			self.context_depth += 1
			self._database.begin()
		#
		except Exception:
		#
			Connection._release()
			raise
		#
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:since: v0.1.00
		"""

		# pylint: disable=broad-except,protected-access

		if (self.context_depth > 0):
		#
			try:
			#
				if (exc_type == None and exc_value == None): self._database.commit()
				else: self._database.rollback()

				self.context_depth -= 1
				if (self.context_depth < 1): self._database = None
			#
			except Exception as handled_exception:
			#
				if (LogLine != None): LogLine.error(handled_exception)
				if (exc_type == None and exc_value == None): self._database.rollback()
			#
		#

		Connection._release()
	#
#

##j## EOF