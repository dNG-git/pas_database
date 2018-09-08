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

# pylint: disable=import-error

from dNG.data.crud.nothing_matched_exception import NothingMatchedException
from dNG.database.nothing_matched_exception import NothingMatchedException as _DbNothingMatchedException
from dNG.database.transaction_context import TransactionContext

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

    @staticmethod
    def wrap(_callable):
        """
Catch certain exceptions and wrap them in CRUD defined ones.

:param callable: Wrapped code

:return: (object) Proxy method
:since:  v1.0.0
    """

        def proxymethod(self, *args, **kwargs):
            try: return _callable(self, *args, **kwargs)
            except _DbNothingMatchedException as handled_exception: raise NothingMatchedException(_exception = handled_exception)
        #

        return proxymethod
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

        def proxymethod(self, *args, **kwargs):
            try:
                with TransactionContext(): return _callable(self, *args, **kwargs)
            except _DbNothingMatchedException as handled_exception: raise NothingMatchedException(_exception = handled_exception)
        #

        return proxymethod
    #
#
