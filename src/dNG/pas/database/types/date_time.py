# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.types.DateTime
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

from datetime import datetime
from sqlalchemy.types import DateTime as _DateTime
from sqlalchemy.types import TypeDecorator
from time import mktime

class DateTime(TypeDecorator):
#
	"""
This class provides an SQLAlchemy DateTime type represented as UNIX
timestamp in Python.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	impl = _DateTime
	"""
sqlalchemy.org: The class-level "impl" attribute is required, and can
reference any TypeEngine class.
	"""
	python_type = float
	"""
sqlalchemy.org: Return the Python type object expected to be returned by
instances of this type, if known.
	"""

	def process_bind_param(self, value, dialect):
	#
		"""
sqlalchemy.org: Receive a bound parameter value to be converted.

:param value: Data to operate upon, of any type expected by this method in
              the subclass. Can be None.
:param dialect: The Dialect in use

:return: (mixed) Subclasses override this method to return the value that
         should be passed along to the underlying TypeEngine object, and
         from there to the DBAPI execute() method.
		"""

		return (None if (value == None) else datetime.fromtimestamp(value))
	#

	def process_result_value(self, value, dialect):
	#
		"""
sqlalchemy.org: Receive a result-row column value to be converted.

:param value: Data to operate upon, of any type expected by this method in
              the subclass. Can be None.
:param dialect: The Dialect in use

:return: (mixed) Subclasses override this method to return the value that
         should be passed back to the application, given a value that is
         already processed by the underlying TypeEngine object, originally
         from the DBAPI cursor method fetchone() or similar.
		"""

		_return = None

		if (value != None):
		#
			_return = (value.timestamp()
			           if (hasattr(value, "timestamp")) else
			           mktime(value.timetuple())
			          )
		#

		return _return
	#
#

##j## EOF