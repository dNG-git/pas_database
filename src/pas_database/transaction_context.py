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

from functools import wraps
from threading import local

from dpt_logging import LogLine

from .connection import Connection

class TransactionContext(object):
    """
"TransactionContext" provides an SQLAlchemy based ContextManager for
transactions.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    __slots__ = [ "__weakref__", "local" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self):
        """
Constructor __init__(TransactionContext)

:since: v1.0.0
        """

        self.local = local()
        """
thread-local instance
        """
    #

    def __enter__(self):
        """
python.org: Enter the runtime context related to this object.

:since: v1.0.0
        """

        # pylint: disable=broad-except,protected-access

        is_connection_context_entered = False

        try:
            if (not hasattr(self.local, "context_depth")): self.local.context_depth = 0

            if (self.local.context_depth < 1):
                self.local.connection = Connection.get_instance()
                self.local.connection._enter_context()

                is_connection_context_entered = True
            #

            self.local.connection.begin()
            self.local.context_depth += 1
        except Exception:
            if (is_connection_context_entered): self.local.connection._exit_context(None, None, None)

            raise
        #
    #

    def __exit__(self, exc_type, exc_value, traceback):
        """
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v1.0.0
        """

        # pylint: disable=broad-except,protected-access

        try:
            if (exc_type is None and exc_value is None): self.local.connection.commit()
            else: self.local.connection.rollback()
        except Exception as handled_exception:
            if (LogLine is not None): LogLine.error(handled_exception, context = "pas_database")
            if (exc_type is None and exc_value is None): self.local.connection.rollback()
        finally:
            self.local.context_depth -= 1
            if (self.local.context_depth < 1): self.local.connection._exit_context(exc_type, exc_value, traceback)
        #

        return False
    #

    @staticmethod
    def wrap_callable(_callable):
        """
Wraps a callable to be executed within an transaction context.

:param callable: Wrapped code

:return: (object) Proxy method
:since:  v1.0.0
        """

        @wraps(_callable)
        def proxymethod(*args, **kwargs):
            with TransactionContext(): return _callable(*args, **kwargs)
        #

        return proxymethod
    #
#
