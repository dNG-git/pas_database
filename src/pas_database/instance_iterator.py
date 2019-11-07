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

from dpt_runtime.iterator import Iterator

from sqlalchemy.inspection import inspect

from .connection import Connection

class InstanceIterator(Iterator):
    """
"InstanceIterator" provides an instance wrapping iterator to encapsulate
SQLAlchemy database instances with an given class.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    __slots__ = [ "args", "buffered", "instance_class", "kwargs", "result" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self, entity, cursor, buffered = False, instance_class = None, *args, **kwargs):
        """
Constructor __init__(InstanceIterator)

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor
:param buffered: True to buffer the result
:param instance_class: Encapsulating database instance class

:since: v1.0.0
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
Results being interated
        """

        with Connection.get_instance() as connection:
            if (buffered): self._init_buffered_results(connection, entity, cursor)
            else: self.result = connection.query(entity).instances(cursor)
        #
    #

    def __iter__(self):
        """
python.org: Return an iterator object.

:return: (object) Iterator object
:since:  v1.0.0
        """

        return (iter(self.result) if (self.instance_class is None) else self)
    #

    def __next__(self):
        """
python.org: Return the next item from the container.

:return: (object) Result object
:since:  v1.0.0
        """

        db_instance = None

        if (self.buffered):
            if (len(self.result) < 1): raise StopIteration()
            return self.result.pop(0)
        else:
            db_instance = next(self.result)

            return (db_instance
                    if (self.instance_class is None) else
                    self.instance_class(db_instance, *self.args, **self.kwargs)
                   )
        #
    #

    def _ensure_populated_db_instance(self, db_instance):
        """
Loads and initializes instances for the buffer.

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor

:since: v1.0.0
        """

        _return = db_instance

        if (self.instance_class is None
            or (not issubclass(self.instance_class.get_db_class(self.instance_class),
                               db_instance.__class__
                              )
               )
           ):
            instance_state = inspect(db_instance)

            if (len(instance_state.expired_attributes) > 0):
                _return = (Connection.get_instance()
                           .query(db_instance.__class__)
                           .populate_existing()
                           .get(instance_state.identity)
                          )
            #
        #

        return _return
    #

    def _init_buffered_results(self, connection, entity, cursor):
        """
Loads and initializes instances for the buffer.

:param entity: SQLAlchemy database entity
:param cursor: SQLAlchemy result cursor

:since: v1.0.0
        """

        self.result = [ ]

        for db_instance in connection.query(entity).instances(cursor):
            db_instance = self._ensure_populated_db_instance(db_instance)

            self.result.append(db_instance
                               if (self.instance_class is None) else
                               self.instance_class(db_instance, *self.args, **self.kwargs)
                              )
        #
    #
#
