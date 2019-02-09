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

try: from collections.abc import Mapping
except ImportError: from collections import Mapping

from dNG.data.crud.instances.abstract import Abstract
from dNG.data.crud.instances.database_mixin import DatabaseMixin
from dNG.data.crud.operation_not_supported_exception import OperationNotSupportedException
from dNG.data.text.input_filter import InputFilter
from dNG.data.text.key_store import KeyStore

class Instance(DatabaseMixin, Abstract):
    """
CRUD entity instance for "KeyStore".

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap_transaction
    def create(self, **kwargs):
        """
"create" operation for "key_store/instance"

:return: (object) KeyStore instance created
:since:  v1.0.0
        """

        is_access_control_supported = self.is_supported("access_control_validation")

        if (kwargs['_select_id'] is not None): raise OperationNotSupportedException()

        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.create", kwargs = kwargs)

        key_store = KeyStore()

        with key_store:
            key_store.set_data_attributes(**Instance._get_filtered_kwargs(kwargs))
            key_store.save()
        #

        return key_store
    #

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap
    def get(self, **kwargs):
        """
"get" operation for "key_store/instance/:id"

:return: (object) KeyStore instance
:since:  v1.0.0
        """

        is_access_control_supported = self.is_supported("access_control_validation")

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.get", _id = kwargs['_select_id'])
        key_store = KeyStore.load_id(kwargs['_select_id'])
        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.get", key_store = key_store)

        return key_store
    #

    def get_data(self, **kwargs):
        """
"get" operation for "key_store/instance/:id/data"

:return: (object) KeyStore instance data
:since:  v1.0.0
        """

        if (not isinstance(kwargs['_selected_value'], KeyStore)): raise OperationNotSupportedException()
        return kwargs['_selected_value'].value_dict
    #

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap
    def select(self, **kwargs):
        """
Internal select operation for "key_store/instance/:id"

:return: (object) KeyStore instance
:since:  v1.0.0
        """

        return self.get(**kwargs)
    #

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap_transaction
    def update(self, **kwargs):
        """
"update" operation for "key_store/instance/:id"

:return: (bool) True on success
:since:  v1.0.0
        """

        is_access_control_supported = self.is_supported("access_control_validation")

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.update", _id = kwargs['_select_id'])
        key_store = KeyStore.load_id(kwargs['_select_id'])
        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.update", key_store = key_store)

        with key_store:
            key_store.set_data_attributes(**Instance._get_filtered_kwargs(kwargs))
            key_store.save()
        #

        return True
    #

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap_transaction
    def update_data(self, **kwargs):
        """
"update" operation for "key_store/instance/:id/data"

:return: (bool) True on success
:since:  v1.0.0
        """

        is_access_control_supported = self.is_supported("access_control_validation")

        if (not isinstance(kwargs['_selected_value'], KeyStore)): raise OperationNotSupportedException()
        key_store = kwargs['_selected_value']

        if (is_access_control_supported): self.access_control.validate(self, "key_store.Instance.update", key_store = key_store)

        with key_store:
            key_store.value_dict = Instance._get_filtered_kwargs(kwargs)
            key_store.save()
        #

        return True
    #

    @classmethod
    def _get_filtered_kwargs(cls, kwargs):
        """
Returns all kwargs after filtering keys and their values.

:return: (dict) Filtered kwargs
:since:  v1.0.0
        """

        _return = { }

        if ("key" in kwargs): _return['key'] = InputFilter.filter_control_chars(kwargs['key'])[:255]
        if ("validity_start_time" in kwargs): _return['validity_start_time'] = float(kwargs['validity_start_time'])
        if ("validity_end_time" in kwargs): _return['validity_end_time'] = float(kwargs['validity_end_time'])

        if ("value" in kwargs):
            _return['value'] = (kwargs['value']
                                if (isinstance(kwargs['value'], Mapping)) else
                                InputFilter.filter_control_chars(kwargs['value'])
                               )
        #

        return _return
    #
#
