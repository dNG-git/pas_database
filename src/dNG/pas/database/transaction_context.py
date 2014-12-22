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
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	def __init__(self):
	#
		"""
Constructor __init__(TransactionContext)

:since: v0.1.00
		"""

		self.connection = None
		"""
Database connection if bound
		"""
		self.context_depth = 0
		"""
Runtime context depth
		"""
	#

	def __enter__(self):
	#
		"""
python.org: Enter the runtime context related to this object.

:since: v0.1.00
		"""

		# pylint: disable=broad-except,protected-access

		try:
		#
			if (self.context_depth < 1):
			#
				Connection._acquire()
				self.connection = Connection.get_instance()
				self.connection.begin()
			#

			self.context_depth += 1
		#
		except Exception:
		#
			if (self.context_depth < 1):
			#
				self.connection = None
				Connection._release()
			#

			raise
		#
	#

	def __exit__(self, exc_type, exc_value, traceback):
	#
		"""
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v0.1.00
		"""

		# pylint: disable=broad-except,protected-access

		if (self.context_depth > 0):
		#
			try:
			#
				self.context_depth -= 1

				if (exc_type is None and exc_value is None): self.connection.commit()
				else: self.connection.rollback()
			#
			except Exception as handled_exception:
			#
				if (LogLine is not None): LogLine.error(handled_exception, context = "pas_database")
				if (exc_type is None and exc_value is None): self.connection.rollback()
			#
			finally:
			#
				if (self.context_depth < 1):
				#
					self.connection = None
					Connection._release()
				#
			#
		#

		return False
	#
#

##j## EOF