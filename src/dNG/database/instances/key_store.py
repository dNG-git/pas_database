# -*- coding: utf-8 -*-

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

from uuid import uuid4 as uuid

from dNG.database.types.date_time import DateTime

from sqlalchemy.schema import Column
from sqlalchemy.types import TEXT, VARCHAR

from .abstract import Abstract

class KeyStore(Abstract):
    """
Database based key-value store.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    # pylint: disable=invalid-name

    __tablename__ = "{0}_key_store".format(Abstract.get_table_prefix())
    """
SQLAlchemy table name
    """
    db_instance_class = "dNG.data.text.KeyStore"
    """
Encapsulating SQLAlchemy database instance class name
    """
    db_schema_version = 2
    """
Database schema version
    """

    id = Column(VARCHAR(32), primary_key = True)
    """
keystore.id
    """
    key = Column(VARCHAR(255), nullable = False, unique = True)
    """
keystore.key
    """
    validity_start_time = Column(DateTime, default = 0, nullable = False)
    """
keystore.validity_start_time
    """
    validity_end_time = Column(DateTime, default = 0, nullable = False)
    """
keystore.validity_end_time
    """
    value = Column(TEXT)
    """
keystore.value
    """

    def __init__(self, *args, **kwargs):
        """
Constructor __init__(KeyStore)

:since: v0.2.00
        """

        Abstract.__init__(self, *args, **kwargs)
        if (self.id is None): self.id = uuid().hex
    #
#
