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

from dpt_runtime.input_filter import InputFilter
from pas_crud_engine import OperationNotSupportedException
from pas_crud_engine.instances import Abstract

from ..database_mixin import DatabaseMixin
from ....instances import KeyStore

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

    __slots__ = DatabaseMixin._mixin_slots_
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
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

        if (kwargs['_select_id'] is not None): raise OperationNotSupportedException()

        self.access_control.validate(self, "key_store.Instance.create", kwargs = kwargs)
        _return = KeyStore()

        with _return:
            _return.set_data_attributes(**Instance._get_filtered_kwargs(kwargs))
            _return.save()
        #

        return _return
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

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        self.access_control.validate(self, "key_store.Instance.get", _id = kwargs['_select_id'])
        _return = KeyStore.load_id(kwargs['_select_id'])
        self.access_control.validate(self, "key_store.Instance.get", key_store = _return)

        return _return
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
    def get_key(self, **kwargs):
        """
"get" operation for "key_store/instance/key/:key"

:return: (object) KeyStore instance
:since:  v1.0.0
        """

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        self.access_control.validate(self, "key_store.Instance.get_key", _id = kwargs['_select_id'])
        _return = KeyStore.load_key(kwargs['_select_id'])
        self.access_control.validate(self, "key_store.Instance.get_key", key_store = _return)

        return _return
    #

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

        if (kwargs['_select_id'] is None): raise OperationNotSupportedException()

        self.access_control.validate(self, "key_store.Instance.update", _id = kwargs['_select_id'])
        key_store = KeyStore.load_id(kwargs['_select_id'])
        self.access_control.validate(self, "key_store.Instance.update", key_store = key_store)

        with key_store:
            key_store.set_data_attributes(**Instance._get_filtered_kwargs(kwargs))
            key_store.save()
        #

        return True
    #

    @Abstract.catch_and_wrap_matching_exception
    @Abstract.restrict_to_access_control_validated_execution
    @DatabaseMixin.wrap_transaction
    def update_data(self, _select_id = None, _selected_value = None, **kwargs):
        """
"update" operation for "key_store/instance/:id/data"

:param _select_id: CRUD resource selection ID
:param _selected_value: CRUD resource selected value

:return: (bool) True on success
:since:  v1.0.0
        """

        if (not isinstance(_selected_value, KeyStore)): raise OperationNotSupportedException()
        key_store = _selected_value

        self.access_control.validate(self, "key_store.Instance.update", key_store = key_store)

        with key_store:
            key_store.value_dict = kwargs
            key_store.save()
        #

        return True
    #

    @staticmethod
    def _get_filtered_kwargs(kwargs):
        """
Returns all kwargs after filtering keys and their values.

:param kwargs: Keyword arguments to filter

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
