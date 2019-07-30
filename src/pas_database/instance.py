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

from threading import local

try: from collections.abc import MutableMapping
except ImportError: from collections import MutableMapping

from dpt_module_loader import NamedClassLoader
from dpt_runtime.io_exception import IOException
from dpt_runtime.type_exception import TypeException
from dpt_runtime.value_exception import ValueException
from dpt_threading.thread_lock import ThreadLock

from sqlalchemy.inspection import inspect
from sqlalchemy.engine.result import ResultProxy

from .connection import Connection
from .instance_iterator import InstanceIterator
from .nothing_matched_exception import NothingMatchedException
from .orm.abstract import Abstract
from .sort_definition import SortDefinition

class Instance(MutableMapping):
    """
"Instance" is an abstract object encapsulating an SQLAlchemy database
instance.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    # pylint: disable=bad-staticmethod-argument, unused-argument

    _DB_INSTANCE_CLASS = None
    """
SQLAlchemy database instance class to initialize for new instances.
    """

    def __init__(self, db_instance = None):
        """
Constructor __init__(Instance)

:param db_instance: Encapsulated SQLAlchemy database instance

:since: v1.0.0
        """

        MutableMapping.__init__(self)

        self._db_context_sort_definition = { }
        """
Context specific sort definition instances
        """
        self._db_sort_definition = None
        """
Sort definition instance
        """
        self.local = local()
        """
thread-local instance
        """
        self._lock = ThreadLock()
        """
Thread safety lock
        """
        self._log_handler = NamedClassLoader.get_singleton("dpt_logging.LogHandler", False)
        """
The LogHandler is called whenever debug messages should be logged or errors
happened.
        """

        self.local.db_instance = db_instance
    #

    def __delitem__(self, key):
        """
python.org: Called to implement deletion of self[key].

:param key: Database instance attribute

:since: v1.0.0
        """

        if (self.is_data_attribute_defined(key)): raise IOException("Database instance attributes can not be deleted")
        super(Instance, self).__delitem__(self, key)
    #

    def __enter__(self):
        """
python.org: Enter the runtime context related to this object.

:since: v1.0.0
        """

        self._enter_context()
        return self
    #

    def __exit__(self, exc_type, exc_value, traceback):
        """
python.org: Exit the runtime context related to this object.

:return: (bool) True to suppress exceptions
:since:  v1.0.0
        """

        self._exit_context(exc_type, exc_value, traceback)
        return False
    #

    def __getitem__(self, key):
        """
python.org: Called to implement evaluation of self[key].

:param key: Database instance to access

:return: (mixed) Database attribute value
:since:  v1.0.0
        """

        with self: return self._get_data_attribute(key)
    #

    def __iter__(self):
        """
python.org: Return an iterator object.

:return: (object) Iterator object
:since:  v1.0.0
        """

        db_instance_class = Instance.get_db_class(self.__class__)
        if (db_instance_class is None): raise IOException("Database instance attributes can not be accessed")

        for column in db_instance_class.__table__.columns: yield column.key
        for key in self._computed_column_keys: yield key
    #

    def __len__(self):
        """
python.org: Called to implement the built-in function len().

:return: (int) Number of database instance attributes
:since: v1.0.0
        """

        return len(Instance.get_db_class(self.__class__).__table__.columns)
    #

    def __new__(cls, *args, **kwargs):
        """
python.org: Called to create a new instance of class cls.

:param cls: Python class

:return: (object) Instance object
:since:  v1.0.0
        """

        db_instance = kwargs.get("db_instance")
        if (db_instance is None): db_instance = (args[0] if (len(args) > 0) else None)

        db_instance_class = (NamedClassLoader.get_class(db_instance.db_instance_class)
                             if (isinstance(db_instance, Abstract) and db_instance.db_instance_class is not None) else
                             None
                            )

        if (db_instance_class is None or cls == db_instance_class): _return = object.__new__(cls)
        else:
            _return = db_instance_class.__new__(db_instance_class, *args, **kwargs)
            _return.__init__(*args, **kwargs)
        #

        return _return
    #

    def __setitem__(self, key, value):
        """
python.org: Called to implement assignment to self[key].

:param key: Database instance attribute
:param value: Database instance attribute value

:since: v1.0.0
        """

        with self: self._set_data_attribute(key, value)
    #

    @property
    def _computed_column_keys(self):
        """
Returns a list of computed column keys.

:return: (list) List of computed column keys
:since:  v1.0.0
        """

        return [ ]
    #

    @property
    def _db_instance(self):
        """
Returns the actual database entry instance.

:return: (object) Database entry instance
:since:  v1.0.0
        """

        with self: return self.local.db_instance
    #

    @property
    def is_known(self):
        """
Returns true if the instance is already saved in the database.

:return: (bool) True if known
:since:  v1.0.0
        """

        return (self.local.db_instance is not None and inspect(self.local.db_instance).has_identity)
    #

    @property
    def is_reloadable(self):
        """
Returns true if the instance can be reloaded automatically in another
thread.

:return: (bool) True if reloadable
:since:  v1.0.0
        """

        return False
    #

    def _apply_db_sort_definition(self, query, context = None):
        """
Applies the sort order to the given SQLAlchemy query instance.

:param query: SQLAlchemy query instance
:param context: Sort definition context

:return: (object) Modified SQLAlchemy query instance
:since:  v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}._apply_db_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")
        _return = query

        sort_definition = self._get_db_sort_definition(context)

        if (sort_definition is not None):
            with self: _return = sort_definition.apply(self.local.db_instance.__class__, query)
        #

        return _return
    #

    def delete(self):
        """
Deletes this entry from the database.

:return: (bool) True on success
:since:  v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.delete()- (#echo(__LINE__)#)", self, context = "pas_database")
        _return = True

        with self:
            if (self.is_known):
                self._ensure_transaction_context()

                self.local.connection.delete(self.local.db_instance)
                self.local.db_instance = None
            else: _return = False
        #

        return _return
    #

    def _ensure_attached_instance(self):
        """
Checks the SQLAlchemy database instance to be attached to an session.

:since: v1.0.0
        """

        with self._lock:
            instance_state = inspect(self.local.db_instance)

            if (instance_state.detached):
                if (instance_state.has_identity):
                    self.local.db_instance = self.local.connection.merge(self.local.db_instance,
                                                                         load = (not instance_state.persistent)
                                                                        )
                else: self.local.connection.add(self.local.db_instance)
            #
        #
    #

    def _ensure_transaction_context(self):
        """
Checks for an active transaction or begins one.

:since: v1.0.0
        """

        # pylint: disable=broad-except, protected-access

        if (self.local.connection.get_transaction_depth() < 1):
            self.local.connection.begin()
            self.local.wrapped_transaction = True
        #
    #

    def _ensure_thread_local_instance(self):
        """
Checks for an initialized SQLAlchemy database instance or create one.

:since: v1.0.0
        """

        if (not hasattr(self.local, "db_instance")): self.local.db_instance = None

        if (self.local.db_instance is None):
            if (self.is_reloadable): self.reload()
            else:
                db_class = Instance.get_db_class(self.__class__)
                if (db_class is not None): self.local.db_instance = db_class()
            #
        #
    #

    def _enter_context(self):
        """
Enters the connection context.

:since: v1.0.0
        """

        # pylint: disable=broad-except, protected-access

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.__enter__()- (#echo(__LINE__)#)", self, context = "pas_database")

        is_connection_context_entered = False

        try:
            if (not hasattr(self.local, "context_depth")): self.local.context_depth = 0

            if (self.local.context_depth < 1):
                self.local.connection = Connection.get_instance()
                self.local.connection._enter_context()

                is_connection_context_entered = True
            #

            self.local.context_depth += 1

            if (getattr(self.local, "db_instance", None) is None): self._ensure_thread_local_instance()
            else: self._ensure_attached_instance()
        except Exception:
            self._enter_context_cleanup()
            if (is_connection_context_entered): self.local.connection._exit_context(None, None, None)

            raise
        #
    #

    def _enter_context_cleanup(self):
        """
This method should be called for if exceptions in "__enter__" occur to
cleanup database connections held by this instance.

:since: v1.0.0
        """

        if (self.local.context_depth > 0): self.local.context_depth -= 1
    #

    def _exit_context(self, exc_type, exc_value, traceback):
        """
Exits the connection context.

:since: v1.0.0
        """

        # pylint: disable=broad-except,protected-access

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.__exit__()- (#echo(__LINE__)#)", self, context = "pas_database")

        self.local.context_depth -= 1

        if (self.local.context_depth < 1 and getattr(self.local, "wrapped_transaction", False)):
            try:
                if (exc_type is None and exc_value is None): self.local.connection.commit()
                else: self.local.connection.rollback()

                self.local.wrapped_transaction = False
            except Exception as handled_exception:
                if (self._log_handler is not None): self._log_handler.error(handled_exception, context = "pas_database")
                if (exc_type is None and exc_value is None): self.local.connection.rollback()
            #
        #

        if (self.local.context_depth < 1): self.local.connection._exit_context(exc_type, exc_value, traceback)
    #

    def _get_data_attribute(self, attribute):
        """
Returns the data for the requested attribute.

:param attribute: Requested attribute

:return: (mixed) Value for the requested attribute; None if undefined
:since:  v1.0.0
        """

        return (getattr(self.local.db_instance, attribute)
                if (hasattr(self.local.db_instance, attribute)) else
                self._get_unknown_data_attribute(attribute)
               )
    #

    def get_data_attributes(self, *args):
        """
Returns the requested attributes.

:return: (dict) Values for the requested attributes; None for undefined ones
:since:  v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.get_data_attributes()- (#echo(__LINE__)#)", self, context = "pas_database")
        _return = { }

        with self:
            for attribute in args: _return[attribute] = self._get_data_attribute(attribute)
        #

        return _return
    #

    def _get_db_sort_definition(self, context = None):
        """
Returns the sort definition based in the given context.

:param context: Sort definition context

:return: (object) SortDefinition instance if specified; None otherwise
:since:  v1.0.0
        """

        _return = None

        if (context is None): _return = self._db_sort_definition
        elif (context in self._db_context_sort_definition): _return = self._db_context_sort_definition[context]

        if (_return is None): _return = self._get_default_sort_definition(context)

        return _return
    #

    def _get_default_sort_definition(self, context = None):
        """
Returns the default sort definition list.

:param context: Sort definition context

:return: (object) Sort definition
:since:  v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}._get_default_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")
        return SortDefinition()
    #

    def _get_unknown_data_attribute(self, attribute):
        """
Returns the data for the requested attribute not defined for this instance.

:param attribute: Requested attribute

:return: (dict) Value for the requested attribute; None if undefined
:since:  v1.0.0
        """

        return None
    #

    def _insert(self):
        """
Insert the instance into the database.

:since: v1.0.0
        """

        if (inspect(self.local.db_instance).transient): self.local.connection.add(self.local.db_instance)
    #

    def is_data_attribute_defined(self, attribute):
        """
Checks the given attribute if it is defined for the entity.

:param attribute: Requested attribute

:return: (bool) Returns true if the attribute is defined
:since:  v1.0.0
        """

        _return = False

        db_class = Instance.get_db_class(self.__class__)

        if (issubclass(db_class, Abstract)):
            try:
                db_class.get_db_column(attribute)
                _return = True
            except ValueException: pass
        #

        return _return
    #

    def is_data_attribute_none(self, *args):
        """
Returns true if at least one of the attributes is "None".

:return: (bool) True if at least one of the attributes is "None"
:since:  v1.0.0
        """

        with self:
            _return = False

            for attribute in args:
                if (self._get_data_attribute(attribute) is None):
                    _return = True
                    break
                #
            #
        #

        return _return
    #

    def reload(self):
        """
Reload instance data from the database.

:since: v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.reload()- (#echo(__LINE__)#)", self, context = "pas_database")

        with self._lock:
            if (not hasattr(self.local, "db_instance")): self._ensure_thread_local_instance()
            if (hasattr(self.local, "connection") and self.local.connection is not None): self._reload()
        #
    #

    def _reload(self):
        """
Implementation of the reloading SQLAlchemy database instance logic.

:since: v1.0.0
        """

        if (self.local.db_instance is None): raise IOException("Database instance is not reloadable.")
        self.local.connection.refresh(self.local.db_instance)
    #

    def save(self):
        """
Saves changes of the instance into the database.

:since: v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.save()- (#echo(__LINE__)#)", self, context = "pas_database")

        with self:
            self._ensure_transaction_context()

            if (self.is_known): self._update()
            else: self._insert()
        #
    #

    def _set_data_attribute(self, attribute, value):
        """
Sets data for the requested attribute.

:param attribute: Requested attribute
:param value: Value for the requested attribute

:since: v1.0.0
        """

        if (hasattr(self.local.db_instance, attribute)): setattr(self.local.db_instance, attribute, value)
        else: self._set_unknown_data_attribute(attribute, value)
    #

    def set_data_attributes(self, **kwargs):
        """
Sets values given as keyword arguments to this method.

:since: v1.0.0
        """

        with self:
            for key in kwargs: self._set_data_attribute(key, kwargs[key])
        #
    #

    def set_sort_definition(self, sort_definition, context = None):
        """
Sets the sort definition list.

:param sort_definition: Sort definition list
:param context: Sort definition context

:since: v1.0.0
        """

        if (self._log_handler is not None): self._log_handler.debug("#echo(__FILEPATH__)# -{0!r}.set_sort_definition()- (#echo(__LINE__)#)", self, context = "pas_database")

        if (not isinstance(sort_definition, SortDefinition)): raise TypeException("Sort definition type given is not supported")

        if (context is None): self._db_sort_definition = sort_definition
        else: self._db_context_sort_definition[context] = sort_definition
    #

    def _set_unknown_data_attribute(self, attribute, value):
        """
Sets data for the requested attribute not defined for this instance.

:param attribute: Requested attribute
:param value: Value for the requested attribute

:since: v1.0.0
        """

        raise ValueException("Attribute '{0}' is not defined on database instance".format(attribute))
    #

    def _update(self):
        """
Updates the instance already saved to the database.

:since: v1.0.0
        """

        pass
    #

    @classmethod
    def buffered_iterator(cls, entity, result, *args, **kwargs):
        """
Returns an instance wrapping buffered iterator to encapsulate SQLAlchemy
database instances with an given class.

:param cls: Encapsulating database instance class
:param entity: SQLAlchemy database entity
:param result: SQLAlchemy result cursor

:return: (Iterator) InstanceIterator object
:since:  v1.0.0
        """

        if (not isinstance(result, ResultProxy)): raise ValueException("Invalid database result given")
        return InstanceIterator(entity, result, True, cls, *args, **kwargs)
    #

    @staticmethod
    def _data_attribute_property(key):
        """
Creates a Python instance attribute for the given database entry value key.

:param key: Database entry value key

:return: (object) Proxy method
:since:  v1.0.0
        """

        @property
        def proxymethod(self):
            """
Returns the value of the corresponding attribute.

:return: (mixed) Attribute value
:since:  v1.0.0
            """

            with self: return self._get_data_attribute(key)
        #

        @proxymethod.setter
        def proxymethod(self, value):
            """
Sets the value of the corresponding attribute.

:param value: Attribute value

:since: v1.0.0
            """

            with self: self._set_data_attribute(key, value)
        #

        return proxymethod
    #

    @staticmethod
    def _data_attribute_readonly_property(key):
        """
Creates a read-only Python instance attribute for the given database entry
value key.

:param key: Database entry value key

:return: (object) Proxy method
:since:  v1.0.0
        """

        @property
        def proxymethod(self):
            """
Returns the value of the corresponding attribute.

:return: (mixed) Attribute value
:since:  v1.0.0
            """

            with self: return self._get_data_attribute(key)
        #

        return proxymethod
    #

    @staticmethod
    def _ensure_db_class(cls, db_instance):
        """
Checks the SQLAlchemy database instance to be an expected one while
auto-loading it.

:param cls: Expected encapsulating database instance class
:param db_instance: Encapsulated database instance

:since: v1.0.0
        """

        if (db_instance is not None
            and getattr(cls, "_DB_INSTANCE_CLASS") is not None
            and (not isinstance(db_instance, cls._DB_INSTANCE_CLASS))
           ): raise ValueException("Given encapsulated database instance is not valid for this encapsulating one")
    #

    @staticmethod
    def get_class(class_name):
        """
Returns the encapsulating database class of the given name.

:param class_name: Encapsulating database class name

:return: (object) Encapsulating database class
:since:  v1.0.0
        """

        _return = NamedClassLoader.get_class_in_namespace("instances", class_name)
        if (not issubclass(_return, Instance)): raise NothingMatchedException("Database class '{0}' not supported".format(class_name))

        return _return
    #

    @staticmethod
    def get_db_class(cls):
        """
Returns the database class for the encapsulating database class given.

:param cls: Encapsulating database class

:return: (object) Database class; None if not defined
:since:  v1.0.0
        """

        return getattr(cls, "_DB_INSTANCE_CLASS", None)
    #

    @staticmethod
    def get_db_class_query(cls):
        """
Returns the query instance for the encapsulating database class given.

:param cls: Encapsulating database class

:since: v1.0.0
        """

        db_class = Instance.get_db_class(cls)
        if (db_class is None): raise ValueException("Encapsulating database class is not valid")

        with Connection.get_instance() as connection:
            return connection.query(db_class)
        #
    #

    @classmethod
    def iterator(cls, entity, result, *args, **kwargs):
        """
Returns an instance wrapping unbuffered iterator to encapsulate SQLAlchemy
database instances with an given class.

:param cls: Encapsulating database instance class
:param entity: SQLAlchemy database entity
:param result: SQLAlchemy result cursor

:return: (Iterator) InstanceIterator object
:since:  v1.0.0
        """

        if (not isinstance(result, ResultProxy)): raise ValueException("Invalid database result given")
        return InstanceIterator(entity, result, False, cls, *args, **kwargs)
    #
#
