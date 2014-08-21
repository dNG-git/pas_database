# -*- coding: utf-8 -*-
##j## BOF

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

from sqlalchemy.sql.expression import and_, or_

from dNG.pas.runtime.type_exception import TypeException
from dNG.pas.runtime.value_exception import ValueException

class ConditionDefinition(object):
#
	"""
"ConditionDefinition" is an abstracted definition of conditions applied to
a SQLalchemy database query before execution.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	AND = 1
	"""
AND condition concatenation
	"""
	NONETYPE = type(None)
	"""
NoneType used to validate a SQLalchemy condition is returned by
"_get_condition()".
	"""
	OR = 2
	"""
OR condition concatenation
	"""
	TYPE_CASE_INSENSITIVE_MATCH = 3
	"""
Matches if the attribute value matches the case insensitive condition. "*"
is used as a placeholder.
	"""
	TYPE_CASE_SENSITIVE_MATCH = 2
	"""
Matches if the attribute value matches the case sensitive condition. "*"
is used as a placeholder.
	"""
	TYPE_EXACT_MATCH = 1
	"""
Matches if the attribute value is matches the condition exactly
	"""
	TYPE_GREATER_THAN_MATCH = 6
	"""
Matches if the attribute value is greater than the condition
	"""
	TYPE_GREATER_THAN_OR_EQUAL_MATCH = 7
	"""
Matches if the attribute value is greater than or equals the condition
	"""
	TYPE_LESS_THAN_MATCH = 4
	"""
Matches if the attribute value is less than the condition
	"""
	TYPE_LESS_THAN_OR_EQUAL_MATCH = 5
	"""
Matches if the attribute value is less than or equals the condition
	"""
	TYPE_SUB_CONDITION = 8
	"""
Condition contains a sub condition definition
	"""

	def __init__(self, concatenation = OR):
	#
		"""
Constructor __init__(ConditionDefinition)

:since: v0.1.00
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

	def apply(self, instance, query):
	#
		"""
Applies the sort order to the given SQLAlchemy query instance.

:param instance: Database instance
:param query: SQLAlchemy query instance

:return: (object) Modified SQLAlchemy query instance
:since:  v0.1.00
		"""

		conditions = self._get_conditions(instance)

		return (query
		        if (conditions == None) else
		        query.filter(conditions)
		       )
	#

	def _get_condition(self, instance, condition):
	#
		"""
Returns a SQLalchemy condition.

:param instance: Database instance
:param condition: Condition

:return: (object) SQLalchemy condition; None if unknown type or empty
		"""

		_return = None

		if (condition['type'] == ConditionDefinition.TYPE_EXACT_MATCH):
		#
			column = ConditionDefinition._get_instance_column(instance, condition['attribute'])
			_return = (column == condition['value'])
		#
		elif (condition['type'] == ConditionDefinition.TYPE_GREATER_THAN_MATCH):
		#
			column = ConditionDefinition._get_instance_column(instance, condition['attribute'])
			_return = (column > condition['value'])
		#
		elif (condition['type'] == ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH):
		#
			column = ConditionDefinition._get_instance_column(instance, condition['attribute'])
			_return = (column >= condition['value'])
		#
		elif (condition['type'] == ConditionDefinition.TYPE_LESS_THAN_MATCH):
		#
			column = ConditionDefinition._get_instance_column(instance, condition['attribute'])
			_return = (column < condition['value'])
		#
		elif (condition['type'] == ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH):
		#
			column = ConditionDefinition._get_instance_column(instance, condition['attribute'])
			_return = (column <= condition['value'])
		#
		elif (condition['type'] == ConditionDefinition.TYPE_SUB_CONDITION): _return = condition['condition_definition']._get_conditions(instance)

		return _return
	#

	def _get_conditions(self, instance):
	#
		"""
Returns the SQLalchemy condition clause corresponding to this definition
instance.

:param instance: Database instance

:return: (object) SQLalchemy condition clause; None if empty
		"""

		_return = None

		condition_clauses = [ ]

		for condition in self.conditions:
		#
			condition_clause = self._get_condition(instance, condition)
			if (type(condition_clause) == ConditionDefinition.NONETYPE): condition_clauses.append(condition_clause)
		#

		condition_clauses_count = len(condition_clauses)

		if (condition_clauses_count == 1): _return = condition_clauses[0]
		elif (condition_clauses_count > 1):
		#
			_return = (and_(*condition_clauses)
			           if (self.concatenation == ConditionDefinition.AND) else
			           or_(*condition_clauses)
			          )
		#

		return _return
	#

	def add_exact_match_condition(self, attribute, value):
	#
		"""
Adds a condition to match the given value exactly.

:param attribute: Database entity attribute
:param value: Condition value

:return: (object) ConditionDefinition instance
:since:  v0.1.00
		"""

		self.conditions.append({ "type": ConditionDefinition.TYPE_EXACT_MATCH,
		                         "attribute": attribute,
		                         "value": value
		                       })
	#

	def add_greater_than_match_condition(self, attribute, value):
	#
		"""
Adds a condition to match values greater than the given one.

:param attribute: Database entity attribute
:param value: Condition value

:return: (object) ConditionDefinition instance
:since:  v0.1.00
		"""

		self.conditions.append({ "type": ConditionDefinition.TYPE_GREATER_THAN_MATCH,
		                         "attribute": attribute,
		                         "value": value
		                       })
	#

	def add_greater_than_or_equal_match_condition(self, attribute, value):
	#
		"""
Adds a condition to match values greater than or equal the given one.

:param attribute: Database entity attribute
:param value: Condition value

:return: (object) ConditionDefinition instance
:since:  v0.1.00
		"""

		self.conditions.append({ "type": ConditionDefinition.TYPE_GREATER_THAN_OR_EQUAL_MATCH,
		                         "attribute": attribute,
		                         "value": value
		                       })
	#

	def add_less_than_match_condition(self, attribute, value):
	#
		"""
Adds a condition to match values less than the given one.

:param attribute: Database entity attribute
:param value: Condition value

:return: (object) ConditionDefinition instance
:since:  v0.1.00
		"""

		self.conditions.append({ "type": ConditionDefinition.TYPE_LESS_THAN_MATCH,
		                         "attribute": attribute,
		                         "value": value
		                       })
	#

	def add_less_than_or_equal_match_condition(self, attribute, value):
	#
		"""
Adds a condition to match values less than or equal the given one.

:param attribute: Database entity attribute
:param value: Condition value

:return: (object) ConditionDefinition instance
:since:  v0.1.00
		"""

		self.conditions.append({ "type": ConditionDefinition.TYPE_LESS_THAN_OR_EQUAL_MATCH,
		                         "attribute": attribute,
		                         "value": value
		                       })
	#

	def add_sub_condition(self, condition_definition):
	#
		"""
Adds the given condition definition as a sub condition.

:param condition_definition: ConditionDefinition instance

:since: v0.1.00
		"""

		if (not isinstance(condition_definition, ConditionDefinition)): raise TypeException("Given condition definition type is invalid")

		self.conditions.append({ "type": ConditionDefinition.TYPE_SUB_CONDITION,
		                         "condition_definition": condition_definition
		                       })
	#

	def clear(self):
	#
		"""
Clears the current list.

:since: v0.1.00
		"""

		self.conditions = [ ]
	#

	def set_concatenation(self, concatenation):
	#
		"""
Sets the concatenation used for this condition definition.

:param concatenation: Concatenation

:since: v0.1.00
		"""

		if (concatenation not in ( ConditionDefinition.AND, ConditionDefinition.OR )): raise ValueException("Given condition concatenation is invalid")
		self.concatenation = concatenation
	#

	@staticmethod
	def _get_instance_column(instance, name):
	#
		"""
Returns a column from the given instance.

:param instance: Database instance
:param name: Column name

:return: (object) Database instance column; None if not defined
		"""

		return (instance._get_db_column(name)
			    if (hasattr(instance, "_get_db_column")) else
			    getattr(instance, name)
			   )
	#
#

##j## EOF