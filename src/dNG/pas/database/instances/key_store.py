# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.database.instances.KeyStore
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

from sqlalchemy import BIGINT, Column, TEXT, VARCHAR
from uuid import uuid4 as uuid

from .abstract import Abstract

class KeyStore(Abstract):
#
	"""
Database based key-value store.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	__tablename__ = "{0}_key_store".format(Abstract.get_table_prefix())
	"""
SQLAlchemy table name
	"""
	db_instance_class = "dNG.pas.data.text.KeyStore"
	"""
Encapsulating SQLAlchemy database instance class name
	"""

	id = Column(VARCHAR(32), primary_key = True)
	"""
keystore.id
	"""
	key = Column(VARCHAR(32), nullable = False, unique = True)
	"""
keystore.key
	"""
	validity_start_date = Column(BIGINT, server_default = "0", nullable = False)
	"""
keystore.validity_start_date
	"""
	validity_end_date = Column(BIGINT, server_default = "0", nullable = False)
	"""
keystore.validity_end_date
	"""
	value = Column(TEXT)
	"""
keystore.value
	"""

	def __init__(self, *args, **kwargs):
	#
		"""
Constructor __init__(KeyStore)

:since: v0.1.00
		"""

		Abstract.__init__(self, *args, **kwargs)
		if (self.id == None): self.id = uuid().hex
	#
#

##j## EOF