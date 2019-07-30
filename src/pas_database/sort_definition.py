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

from dpt_runtime.type_exception import TypeException
from dpt_runtime.value_exception import ValueException

class SortDefinition(object):
    """
"SortDefinition" is an abstracted class to define instances of database sort
instructions.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
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
        """
Constructor __init__(SortDefinition)

:since: v1.0.0
        """

        self.sort_tuples = [ ]
        """
List of tuples defining the attribute and sort direction
        """

        if (sort_tuples is None): sort_tuples = [ ]

        for sort_tuple in sort_tuples:
            if (type(sort_tuple) is not tuple): raise TypeException("Sort definition data type given is not supported")
            if (len(sort_tuple) != 2): raise ValueException("Sort definition given is not supported")

            self.append(sort_tuple[0], sort_tuple[1])
        #
    #

    def apply(self, db_column_definition, query):
        """
Applies the sort order to the given SQLAlchemy query instance.

:param db_column_definition: Database class or column definition
:param query: SQLAlchemy query instance

:return: (object) Modified SQLAlchemy query instance
:since:  v1.0.0
        """

        _return = query

        is_get_db_column_available = hasattr(db_column_definition, "get_db_column")

        for sort_definition in self.sort_tuples:
            column = (db_column_definition.get_db_column(sort_definition[0])
                      if (is_get_db_column_available) else
                      getattr(db_column_definition, sort_definition[0])
                     )

            _return = _return.order_by(column.asc()
                                       if (sort_definition[1] == SortDefinition.ASCENDING) else
                                       column.desc()
                                      )
        #

        return _return
    #

    def append(self, attribute, direction):
        """
Appends a sort definition to the current list.

:param attribute: Database entity attribute
:param direction: Sort direction

:return: (object) SortDefinition instance
:since:  v1.0.0
        """

        SortDefinition.validate_sort_direction(direction)
        self.sort_tuples.append(( attribute, direction ))

        return self
    #

    def clear(self):
        """
Clears the current list.

:since: v1.0.0
        """

        self.sort_tuples = [ ]
    #

    def prepend(self, attribute, direction):
        """
Prepends a sort definition to the current list.

:param attribute: Database entity attribute
:param direction: Sort direction

:return: (object) SortDefinition instance
:since:  v1.0.0
        """

        SortDefinition.validate_sort_direction(direction)
        self.sort_tuples.insert(0, ( attribute, direction ))

        return self
    #

    @staticmethod
    def validate_sort_direction(direction):
        """
Validates the given sort direction.

:since: v1.0.0
        """

        if (direction not in ( SortDefinition.ASCENDING, SortDefinition.DESCENDING )): raise ValueException("Sort definition given is not supported")
    #
#
