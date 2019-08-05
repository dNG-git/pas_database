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

from os import path
import os
import re

from dpt_file import File
from dpt_interactive_cli import InteractiveCli
from dpt_json import JsonResource
from dpt_logging import LogLine
from dpt_plugins import HookContext
from dpt_runtime.binary import Binary
from dpt_runtime.io_exception import IOException
from dpt_runtime.type_exception import TypeException
from dpt_settings import Settings

from .connection import Connection
from .instance import Instance
from .nothing_matched_exception import NothingMatchedException
from .orm import Abstract as _DbAbstract
from .orm.schema_version import SchemaVersion as _DbSchemaVersion
from .transaction_context import TransactionContext

class Schema(Instance):
    """
The "Schema" class provides methods to handle versions and upgrades.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    _DB_INSTANCE_CLASS = _DbSchemaVersion
    """
SQLAlchemy database instance class to initialize for new instances.
    """
    RE_ESCAPED = re.compile("(\\\\+)$")
    """
RegExp to find escape characters
    """
    RE_SQL_COMMENT_LINE = re.compile("^\\s*\\-\\-.*$", re.M)
    """
Comments in (invalid) JSON setting files are replaced before getting parsed.
    """

    def __init__(self, db_instance = None):
        """
Constructor __init__(Schema)

:since: v1.0.0
        """

        Instance.__init__(self, db_instance)
    #

    def _set_data_attribute(self, attribute, value):
        """
Sets data for the requested attribute.

:param attribute: Requested attribute
:param value: Value for the requested attribute

:since: v1.0.0
        """

        if (attribute == "name"): value = Binary.utf8(value)
        Instance._set_data_attribute(self, attribute, value)
    #

    @staticmethod
    def apply_version(instance_class):
        """
Applies all changed schema version files required for the given instance
class.

:param instance_class: Database instance class

:since: v1.0.0
        """

        # pylint: disable=broad-except

        if (instance_class is None
            or (not issubclass(instance_class, _DbAbstract))
            or instance_class.db_schema_version is None
           ): raise TypeException("Given instance class is invalid")

        current_version = 0
        instance_class_name = instance_class.__name__

        with Connection.get_instance():
            try:
                schema_version = Schema.load_latest_name_entry(instance_class_name)
                current_version = schema_version['version']
            except NothingMatchedException: pass

            target_version = instance_class.db_schema_version

            with HookContext("pas.database.{0}.applySchema".format(instance_class_name),
                             current_version = current_version,
                             target_version = target_version
                            ):
                schema_directory_path = path.join(Settings.get("path_data"),
                                                "database",
                                                "{0}_schema".format(Connection.get_backend_name()),
                                                instance_class_name
                                                )

                schema_version_files = { }

                if (os.access(schema_directory_path, os.R_OK | os.X_OK)):
                    re_object = re.compile("schema\\_\\d+\\.(json|sql)$", re.I)

                    schema_version_files = { file_name: path.join(schema_directory_path, file_name)
                                            for file_name in os.listdir(schema_directory_path) if (re_object.match(file_name) is not None)
                                        }
                #

                if (current_version < 1):
                    LogLine.info("pas.Database schema '{0}' is at version {1:d}".format(instance_class_name, target_version))

                    schema = Schema()
                    schema.set_data_attributes(name = instance_class_name, version = target_version)
                    schema.save()
                elif (len(schema_version_files) > 0
                    and current_version < target_version
                    ): Schema._upgrade(instance_class_name, schema_version_files, current_version, target_version)
            #
        #
    #

    @staticmethod
    def _apply_sql_command(sql_command):
        """
Applies the given SQL command to the database connection.

:param sql_command: Database specific SQL command

:since: v1.0.0
        """

        Connection.get_instance().get_bind().execute(sql_command)
    #

    @staticmethod
    def _apply_sql_file(file_path_name):
        """
Applies the given SQL file.

:param file_path_name: Database specific SQL file

:since: v1.0.0
        """

        file_content = None
        file_object = File()

        if (file_object.open(file_path_name, True, "r")):
            file_content = Binary.str(file_object.read())
            file_object.close()
        #

        if (file_content is None): raise IOException("Schema file given is invalid")

        file_content = file_content.replace("__db_prefix__", _DbAbstract.get_table_prefix())
        file_content = Schema.RE_SQL_COMMENT_LINE.sub("", file_content)

        current_sql_command = ""
        sql_commands = file_content.split(";")

        for sql_command in sql_commands:
            re_result = Schema.RE_ESCAPED.search(sql_command)

            if (re_result is not None
                and (len(re_result.group(1)) % 2) == 1
               ): current_sql_command += sql_command
            else:
                current_sql_command = (sql_command
                                       if (current_sql_command == "") else
                                       current_sql_command + sql_command
                                      )

                current_sql_command = current_sql_command.strip()

                if (current_sql_command != ""):
                    Schema._apply_sql_command(current_sql_command)
                    current_sql_command = ""
                #
            #
        #
    #

    @staticmethod
    def _check_upgrade_dependencies(dependencies):
        """
Checks that all dependencies are matched.

:param dependencies: List of dependencies to be checked

:return: (bool) True if all dependencies are matched
:since:  v1.0.0
        """

        _return = True

        for dependency in dependencies:
            if ("name" in dependency and "version_required" in dependency):
                try:
                    schema_version = Schema.load_latest_name_entry(dependency['name'])
                    current_version = schema_version['version']
                except NothingMatchedException: current_version = 0

                if (current_version < dependency['version_required']):
                    _return = False
                    break
                #
            #
        #

        return _return
    #

    @classmethod
    def load_latest_name_entry(cls, name):
        """
Load the schema entry with the highest version for the given name.

:param cls: Expected encapsulating database instance class
:param name: Schema name

:return: (object) Schema instance on success
:since:  v1.0.0
        """

        with Connection.get_instance():
            db_query = Instance.get_db_class_query(cls).filter(_DbSchemaVersion.name == name)
            db_query = db_query.order_by(_DbSchemaVersion.version.desc()).limit(1)
            db_instance = db_query.first()

            if (db_instance is None): raise NothingMatchedException("Schema name '{0}' is invalid".format(name))
            Instance._ensure_db_class(cls, db_instance)

            return Schema(db_instance)
        #
    #

    @staticmethod
    def _upgrade(instance_class_name, schema_version_files, current_version, target_version):
        """
Upgrades the given database schema.

:param instance_class_name: SQLAlchemy database instance name the schema is
       used for
:param schema_version_files: List of database schema version files
:param current_version: Current version of the SQLAlchemy database instance
:param target_version: Target version of the SQLAlchemy database instance

:since: v1.0.0
        """

        LogLine.info("pas.Database will upgrade schema '{0}' from version {1:d} to {2:d}".format(instance_class_name, current_version, target_version))
        schema_versions = (version for version in range(current_version + 1, target_version + 1) if "schema_{0:d}.sql".format(version) in schema_version_files)

        try:
            with TransactionContext():
                for schema_version in schema_versions:
                    if ("schema_{0:d}.json".format(schema_version) in schema_version_files):
                        schema_data_file = File()
                        schema_data_path_file_name = schema_version_files["schema_{0:d}.json".format(schema_version)]

                        try:
                            if (not schema_data_file.open(schema_data_path_file_name, True, "r")):
                                raise IOException("An error occurred while reading database schema settings of '{0}' at version {1:d}".format(instance_class_name, schema_version))
                            #

                            schema_raw_data = schema_data_file.read()

                            schema_data = JsonResource.json_to_data(schema_raw_data)

                            if (schema_data is None):
                                raise IOException("'{0}' is not a valid JSON encoded file".format(schema_data_path_file_name))
                            #

                            if (type(schema_data.get("dependencies")) is list
                                and (not Schema._check_upgrade_dependencies(schema_data['dependencies']))
                               ):
                                LogLine.warning("pas.Database stopped upgrade of schema '{0}' at version {1:d} because of missing dependencies".format(instance_class_name, schema_version, target_version))

                                cli = InteractiveCli.get_instance()
                                if (isinstance(cli, InteractiveCli)): cli.output_info("Database schema '{0}' not completely upgraded because of missing dependencies. Execute again after dependencies are matched.".format(instance_class_name))

                                break
                            #
                        finally: schema_data_file.close()
                    #

                    Schema._apply_sql_file(schema_version_files["schema_{0:d}.sql".format(schema_version)])
                    LogLine.info("pas.Database schema '{0}' is at version {1:d}".format(instance_class_name, schema_version))

                    schema = Schema()
                    schema.set_data_attributes(name = instance_class_name, version = schema_version)
                    schema.save()
                #
            #
        except Exception:
            cli = InteractiveCli.get_instance()
            if (isinstance(cli, InteractiveCli)): cli.output_error("An error occurred updating the database schema '{0}'".format(instance_class_name))

            raise
        #
    #
#
