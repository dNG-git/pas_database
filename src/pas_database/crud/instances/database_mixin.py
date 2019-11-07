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

from pas_crud_engine import NothingMatchedException, OperationFailedException
from pas_crud_engine.instances import Abstract
from sqlalchemy.exc import SQLAlchemyError

from ...connection import Connection
from ...nothing_matched_exception import NothingMatchedException as _DbNothingMatchedException
from ...transaction_context import TransactionContext

class DatabaseMixin(object):
    """
"DatabaseMixin" provides a wrapper method to map database exceptions to CRUD ones.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    _mixin_slots_ = [ ]
    """
Additional __slots__ used for inherited classes.
    """
    __slots__ = [ ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    @staticmethod
    def catch_and_wrap_matching_exception(_callable):
        """
Catch certain exceptions and wrap them in CRUD defined ones.

:param _callable: Wrapped code

:return: (object) Proxy method
:since:  v1.0.0
    """

        @wraps(_callable)
        def proxymethod(self, *args, **kwargs):
            try: return _callable(self, *args, **kwargs)
            except _DbNothingMatchedException as handled_exception: raise NothingMatchedException(_exception = handled_exception)
            except SQLAlchemyError as handled_exception: raise OperationFailedException(_exception = handled_exception)
        #

        return proxymethod
    #

    @staticmethod
    def wrap(_callable):
        """
Catch certain exceptions and wrap them in CRUD defined ones.

:param callable: Wrapped code

:return: (object) Proxy method
:since:  v1.0.0
    """

        @wraps(_callable)
        def proxymethod(self, *args, **kwargs):
            with Connection.get_instance(): return _callable(self, *args, **kwargs)
        #

        return DatabaseMixin.catch_and_wrap_matching_exception(proxymethod)
    #

    @staticmethod
    def wrap_transaction(_callable):
        """
Catch certain exceptions and wrap them in CRUD defined ones. This method
additionally applies a transaction context for the callable.

:param callable: Wrapped code

:return: (object) Proxy method
:since:  v1.0.0
    """

        @wraps(_callable)
        def proxymethod(self, *args, **kwargs):
            with TransactionContext(): return _callable(self, *args, **kwargs)
        #

        return DatabaseMixin.catch_and_wrap_matching_exception(proxymethod)
    #
#
