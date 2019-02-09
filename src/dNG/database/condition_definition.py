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

from sqlalchemy.sql.expression import and_, or_

from dNG.runtime.type_exception import TypeException
from dNG.runtime.value_exception import ValueException

class ConditionDefinition(object):
    """
"ConditionDefinition" is an abstracted definition of conditions applied to
a SQLalchemy database query before execution.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    AND = 1
    """
AND condition concatenation
    """
    OR = 2
    """
OR condition concatenation
    """
    TYPE_CASE_INSENSITIVE_MATCH = 7
    """
Matches if the attribute value matches the case insensitive condition.
    """
    TYPE_CASE_INSENSITIVE_NO_MATCH = 8
    """
Matches if the attribute value does not match the case insensitive
condition.
    """
    TYPE_CASE_SENSITIVE_MATCH = 5
    """
Matches if the attribute value matches the case sensitive condition.
    """
    TYPE_CASE_SENSITIVE_NO_MATCH = 6
    """
Matches if the attribute value does not match the case sensitive condition.
    """
    TYPE_EXACT_MATCH = 1
    """
Matches if the attribute value matches the condition exactly.
    """
    TYPE_EXACT_NO_MATCH = 2
    """
Matches if the attribute value does not match the condition exactly.
    """
    TYPE_GREATER_THAN_MATCH = 11
    """
Matches if the attribute value is greater than the condition.
    """
    TYPE_GREATER_THAN_OR_EQUAL_MATCH = 12
    """
Matches if the attribute value is greater than or equals the condition.
    """
    TYPE_IN_LIST_MATCH = 3
    """
Matches if the property is a exact match of a value in the list.
    """
    TYPE_LESS_THAN_MATCH = 9
    """
Matches if the attribute value is less than the condition.
    """
    TYPE_LESS_THAN_OR_EQUAL_MATCH = 10
    """
Matches if the attribute value is less than or equals the condition.
    """
    TYPE_NOT_IN_LIST_MATCH = 4
    """
Matches if the property is not a exact match of a value in the list.
    """
    TYPE_SUB_CONDITION = 13
    """
Condition contains a sub condition definition.
    """

    def __init__(self, concatenation = OR):
        """
Constructor __init__(ConditionDefinition)

:since: v1.0.0
        """

        self.concatenation = None
        """
Type of condition concatenation
        """
        self.conditions = [ ]
        """
List of conditions
        """

        self.set_concatenation(concatenation)
    #

    @property
    def conditions_count(self):
        """
Returns the number of defined conditions.

:return: (int) Conditions count
:since:  v1.0.0
        """

        return len(self.conditions)
    #

    def add_case_insensitive_match_condition(self, attribute, value):
        """
Adds a case insensitive condition to match the given value.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_CASE_INSENSITIVE_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_case_insensitive_no_match_condition(self, attribute, value):
        """
Adds a case insensitive condition to not match the given value.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_CASE_INSENSITIVE_NO_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_case_sensitive_match_condition(self, attribute, value):
        """
Adds a case sensitive condition to match the given value.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_CASE_SENSITIVE_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_case_sensitive_no_match_condition(self, attribute, value):
        """
Adds a case sensitive condition to not match the given value.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_CASE_SENSITIVE_NO_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_exact_match_condition(self, attribute, value):
        """
Adds a condition to match the given value exactly.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_EXACT_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_exact_no_match_condition(self, attribute, value):
        """
Adds a condition to not match the given value exactly.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_EXACT_NO_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_greater_than_match_condition(self, attribute, value):
        """
Adds a condition to match values greater than the given one.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_GREATER_THAN_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_greater_than_or_equal_match_condition(self, attribute, value):
        """
Adds a condition to match values greater than or equal the given one.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_in_list_match_condition(self, attribute, value):
        """
Adds a condition to match a value exactly in the list given.

:param attribute: Database entity attribute
:param value: List of condition values

:since: v1.0.0
        """

        if (isinstance(value, list) and len(value) > 0):
            self.conditions.append({ "type": ConditionDefinition.TYPE_IN_LIST_MATCH,
                                     "attribute": attribute,
                                     "value": value
                                   })
        #
    #

    def add_less_than_match_condition(self, attribute, value):
        """
Adds a condition to match values less than the given one.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_LESS_THAN_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_less_than_or_equal_match_condition(self, attribute, value):
        """
Adds a condition to match values less than or equal the given one.

:param attribute: Database entity attribute
:param value: Condition value

:since: v1.0.0
        """

        self.conditions.append({ "type": ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH,
                                 "attribute": attribute,
                                 "value": value
                               })
    #

    def add_not_in_list_match_condition(self, attribute, value):
        """
Adds a condition to match a value exactly in the list given.

:param attribute: Database entity attribute
:param value: List of condition values

:since: v1.0.0
        """

        if (isinstance(value, list) and len(value) > 0):
            self.conditions.append({ "type": ConditionDefinition.TYPE_IN_LIST_MATCH,
                                     "attribute": attribute,
                                     "value": value
                                   })
        #
    #

    def add_sub_condition(self, condition_definition):
        """
Adds the given condition definition as a sub condition.

:param condition_definition: ConditionDefinition instance

:since: v1.0.0
        """

        if (not isinstance(condition_definition, ConditionDefinition)): raise TypeException("Given condition definition type is invalid")

        if (condition_definition.conditions_count > 0):
            self.conditions.append({ "type": ConditionDefinition.TYPE_SUB_CONDITION,
                                     "condition_definition": condition_definition
                                   })
        #
    #

    def apply(self, db_column_definition, query):
        """
Applies the conditions to the given SQLAlchemy query instance.

:param db_column_definition: Database class or column definition
:param query: SQLAlchemy query instance

:return: (object) Modified SQLAlchemy query instance
:since:  v1.0.0
        """

        conditions = self._get_conditions(db_column_definition)

        return (query
                if (conditions is None) else
                query.filter(conditions)
               )
    #

    def clear(self):
        """
Clears the current condition list.

:since: v1.0.0
        """

        self.conditions = [ ]
    #

    def get_concatenation(self):
        """
Returns the concatenation used for this condition definition.

:retrun: (int) Concatenation type
:since:  v1.0.0
        """

        return self.concatenation
    #

    def _get_condition(self, db_column_definition, condition):
        """
Returns a SQLalchemy condition.

:param db_column_definition: Database class or column definition
:param condition: Condition

:return: (object) SQLalchemy condition; None if unknown type or empty
:since:  v1.0.0
        """

        # pylint: disable=protected-access

        _return = None

        if (condition['type'] == ConditionDefinition.TYPE_SUB_CONDITION):
            _return = condition['condition_definition']._get_conditions(db_column_definition)
        else:
            column = ConditionDefinition._get_db_column(db_column_definition, condition['attribute'])

            if (condition['type'] == ConditionDefinition.TYPE_CASE_INSENSITIVE_MATCH):
                _return = column.ilike(condition['value'], "\\")
            elif (condition['type'] == ConditionDefinition.TYPE_CASE_INSENSITIVE_NO_MATCH):
                _return = column.notilike(condition['value'], "\\")
            elif (condition['type'] == ConditionDefinition.TYPE_CASE_SENSITIVE_MATCH):
                _return = column.like(condition['value'], "\\")
            elif (condition['type'] == ConditionDefinition.TYPE_CASE_SENSITIVE_NO_MATCH):
                _return = column.notlike(condition['value'], "\\")
            elif (condition['type'] == ConditionDefinition.TYPE_EXACT_MATCH):
                _return = (column == condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_EXACT_NO_MATCH):
                _return = (column != condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_GREATER_THAN_MATCH):
                _return = (column > condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH):
                _return = (column >= condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_IN_LIST_MATCH):
                _return = column.in_(condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_LESS_THAN_MATCH):
                _return = (column < condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH):
                _return = (column <= condition['value'])
            elif (condition['type'] == ConditionDefinition.TYPE_NOT_IN_LIST_MATCH):
                _return = column.notin_(condition['value'])
            #
        #

        return _return
    #

    def _get_conditions(self, db_column_definition):
        """
Returns the SQLalchemy condition clause corresponding to this definition
instance.

:param db_column_definition: Database class or column definition

:return: (object) SQLalchemy condition clause; None if empty
:since:  v1.0.0
        """

        _return = None

        condition_clauses = [ ]

        for condition in self.conditions:
            condition_clause = self._get_condition(db_column_definition, condition)
            if (condition_clause is not None): condition_clauses.append(condition_clause)
        #

        condition_clauses_count = len(condition_clauses)

        if (condition_clauses_count == 1): _return = condition_clauses[0]
        elif (condition_clauses_count > 1):
            _return = (and_(*condition_clauses)
                       if (self.concatenation == ConditionDefinition.AND) else
                       or_(*condition_clauses)
                      )
        #

        return _return
    #

    def set_concatenation(self, concatenation):
        """
Sets the concatenation used for this condition definition.

:param concatenation: Concatenation

:since: v1.0.0
        """

        if (concatenation not in ( ConditionDefinition.AND, ConditionDefinition.OR )): raise ValueException("Given condition concatenation is invalid")
        self.concatenation = concatenation
    #

    @staticmethod
    def _get_db_column(db_column_definition, name):
        """
Returns a column from the given definition.

:param db_column_definition: Database class or column definition
:param name: Column name

:return: (object) Database instance column; None if not defined
:since:  v1.0.0
        """

        return (db_column_definition.get_db_column(name)
                if (hasattr(db_column_definition, "get_db_column")) else
                getattr(db_column_definition, name)
               )
    #
#
