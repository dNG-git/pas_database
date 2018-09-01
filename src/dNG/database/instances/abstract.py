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

# pylint: disable=import-error,no-name-in-module

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import reconstructor

from dNG.database.connection import Connection
from dNG.runtime.value_exception import ValueException

class Abstract(declarative_base()):
    """
This class provides abstract SQLAlchemy database instances.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    # pylint: disable=bad-staticmethod-argument, unused-argument

    __abstract__ = True
    """
sqlalchemy.org: "__abstract__" causes declarative to skip the production
of a table or mapper for the class entirely.
    """
    db_instance_class = None
    """
Encapsulating SQLAlchemy database instance class name
    """
    db_schema_version = None
    """
Database schema version
    """

    @reconstructor
    def sa_reconstructor(self):
        """
sqlalchemy.org: Designates a method as the "reconstructor", an __init__-like
method that will be called by the ORM after the instance has been loaded
from the database or otherwise reconstituted.

:since: v1.0.0
        """

        self.__init__()
    #

    @classmethod
    def get_db_column(cls, attribute):
        """
Returns the SQLAlchemy column for the requested attribute.

:param cls: Python class
:param attribute: Requested attribute

:return: (object) SQLAlchemy column
:since:  v1.0.0
        """

        return cls._get_db_column(cls, attribute)
    #

    @staticmethod
    def _get_db_column(cls, attribute):
        """
Returns the SQLAlchemy column for the requested attribute of the given
class.

:param cls: Python class
:param attribute: Requested attribute

:return: (object) SQLAlchemy column
:since:  v1.0.0
        """

        return (getattr(cls, attribute)
                if (hasattr(cls, attribute)) else
                cls.get_unknown_db_column(attribute)
               )
    #

    @classmethod
    def get_unknown_db_column(cls, attribute):
        """
Returns the SQLAlchemy column for the requested attribute not defined for
this entity class.

:param cls: Python class
:param attribute: Requested attribute

:return: (object) SQLAlchemy column
:since:  v1.0.0
        """

        return cls._get_unknown_db_column(cls, attribute)
    #

    @staticmethod
    def _get_unknown_db_column(cls, attribute):
        """
Returns the SQLAlchemy column for the requested attribute not defined for
the given entity class.

:param cls: Python class
:param attribute: Requested attribute

:return: (object) SQLAlchemy column
:since:  v1.0.0
        """

        raise ValueException("Given attribute '{0}' is not defined for '{1}".format(attribute, cls.__name__))
    #

    @staticmethod
    def get_table_prefix():
        """
Get the configured database table prefix.

:return: (str) Table prefix
:since:  v1.0.0
        """

        return Connection.get_table_prefix()
    #
#
