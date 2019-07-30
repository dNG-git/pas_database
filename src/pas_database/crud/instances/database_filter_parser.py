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

import re

from pas_crud_engine import InputValidationException, OperationNotSupportedException
from pas_crud_engine.instances import AbstractFilterParser

from ...condition_definition import ConditionDefinition

class DatabaseFilterParser(AbstractFilterParser):
    """
"DatabaseFilterParser" provides a condition definition used for database
requests.

:author:     direct Netware Group et al.
:copyright:  direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    RE_KEY_CONDITION = re.compile("^(!?)(\\w+)([<>]=?|~=)?$")
    """
RegExp for key conditions
    """

    @AbstractFilterParser.filter.setter
    def filter(self, condition_definition):
        """
Sets an already initialized database condition definition instance for this
instance.

:param condition_definition: Condition definition instance

:return: (mixed) Parser specific filter representation
:since:  v1.0.0
        """

        if (not isinstance(condition_definition, ConditionDefinition)): raise OperationNotSupportedException()
        self._filter = condition_definition
    #

    def _add_condition(self, condition_definition, key_definition, value):
        """
Adds a condition to the given definition for the given key and value.

:param condition_definition: Condition definition instance
:param key_definition: Key definition
:param value: Value to handle

:since: v1.0.0
        """

        key = key_definition['key']
        condition_type = key_definition['type']

        if (condition_type == ConditionDefinition.TYPE_EXACT_MATCH): condition_definition.add_exact_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_EXACT_NO_MATCH): condition_definition.add_exact_no_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_LESS_THAN_MATCH): condition_definition.add_less_than_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH): condition_definition.add_less_than_or_equal_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_GREATER_THAN_MATCH): condition_definition.add_greater_than_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH): condition_definition.add_greater_than_or_equal_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_CASE_INSENSITIVE_MATCH): condition_definition.add_case_insensitive_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_CASE_INSENSITIVE_NO_MATCH): condition_definition.add_case_insensitive_no_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_IN_LIST_MATCH): condition_definition.add_in_list_match_condition(key, value)
        elif (condition_type == ConditionDefinition.TYPE_NOT_IN_LIST_MATCH): condition_definition.add_not_in_list_match_condition(key, value)
    #

    def _parse_and_concatenation(self, filter_data):
        """
Parses the given dictionary representing an "and" concatenated filter
definition.

:param filter_data: "and" concatenated dictionary

:return: (mixed) Parser specific filter representation
:since:  v1.0.0
        """

        _return = ConditionDefinition(ConditionDefinition.AND)

        for key in filter_data:
            value = self._parse(key, filter_data[key])

            if (type(value) is ConditionDefinition): _return.add_sub_condition(value)
            else: self._parse_and_add_condition(_return, key, value)
        #

        return _return
    #

    def _parse_or_concatenation(self, key, filter_list):
        """
Parses the given list representing an "or" concatenated filter definition.

:param key: Key of filter level being parsed
:param filter_list: "or" concatenated list

:return: (mixed) Parser specific filter representation
:since:  v1.0.0
        """

        _return = ConditionDefinition(ConditionDefinition.OR)

        list_key_definition = self._parse_key_condition(key)
        value_list = [ ]

        for filter_data in filter_list:
            value = self._parse(key, filter_data)

            if (type(value) is ConditionDefinition): _return.add_sub_condition(value)
            elif (list_key_definition is None): raise InputValidationException("Only sub-conditions are allowed for the top-level filter list")
            else:
                key_definition = (self._parse_key_condition(key, value)
                                  if (isinstance(value, list)) else
                                  list_key_definition
                                 )

                if (key_definition['type'] == ConditionDefinition.TYPE_EXACT_MATCH): value_list.append(value)
                else: self._add_condition(_return, key_definition, value)
        #

        value_list_length = len(value_list)

        if (value_list_length > 1):
            self._add_condition(_return, key, value_list)
        elif (value_list_length == 1):
            self._add_condition(_return, key, value_list[0])
        #

        return _return
    #

    def _parse_and_add_condition(self, condition_definition, key, value):
        """
Parses the given key definition and adds the result as a new condition to
the given definition.

:param condition_definition: Condition definition instance
:param key: Key definition string
:param value: Value to handle

:since: v1.0.0
        """

        key_definition = self._parse_key_condition(key, value)
        self._add_condition(condition_definition, key_definition, value)
    #

    def _parse_key_condition(self, key, value = None):
        """
Parses the given key definition string containing the relevant property name
and operation.

:param key: Key definition string
:param value: Value to handle

:return: (dict) Key definition
:since:  v1.0.0
        """

        _return = (None if (key is None) else { })

        if (key is not None):
            re_result = DatabaseFilterParser.RE_KEY_CONDITION.match(key)
            if (re_result is None): raise InputValidationException()

            _return['key'] = key

            if (isinstance(value, list)):
                if (re_result.group(1) == "" and re_result.group(3) is None): _return['type'] = ConditionDefinition.TYPE_IN_LIST_MATCH
                elif (re_result.group(1) == "!" and re_result.group(3) is None): _return['type'] = ConditionDefinition.TYPE_NOT_IN_LIST_MATCH
                else: raise InputValidationException()
            elif (re_result.group(1) == "" and re_result.group(3) is None): _return['type'] = ConditionDefinition.TYPE_EXACT_MATCH
            elif (re_result.group(1) == "!" and re_result.group(3) is None): _return['type'] = ConditionDefinition.TYPE_EXACT_NO_MATCH
            elif (re_result.group(1) == "" and re_result.group(3) == "<"): _return['type'] = ConditionDefinition.TYPE_LESS_THAN_MATCH
            elif (re_result.group(1) == "" and re_result.group(3) == "<="): _return['type'] = ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH
            elif (re_result.group(1) == "" and re_result.group(3) == ">"): _return['type'] = ConditionDefinition.TYPE_GREATER_THAN_MATCH
            elif (re_result.group(1) == "" and re_result.group(3) == ">="): _return['type'] = ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH
            elif (re_result.group(1) == "" and re_result.group(3) == "~="): _return['type'] = ConditionDefinition.TYPE_CASE_INSENSITIVE_MATCH
            elif (re_result.group(1) == "!" and re_result.group(3) == "~="): _return['type'] = ConditionDefinition.TYPE_CASE_INSENSITIVE_NO_MATCH
            else: raise InputValidationException()
        #

        return _return
    #

    def _set_empty_filter(self):
        """
Sets an empty parser specific filter representation for an empty filter
string.

:since: v1.0.0
        """

        self._filter = ConditionDefinition()
    #
#
