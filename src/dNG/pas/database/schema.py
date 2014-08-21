# -*- coding: utf-8 -*-
##j## BOF

"""
direct PAS
Python Application Services
----------------------------------------------------------------------------
(C) direct Netware Group - All rights reserved
http://www.direct-netware.de/redirect.py?pas;database

This Source Code Form is subject to the terms of the Mozilla Public License,
v. 2.0. If a copy of the MPL was not distributed with this file, You can
obtain one at http://mozilla.org/MPL/2.0/.
----------------------------------------------------------------------------
http://www.direct-netware.de/redirect.py?licenses;mpl2
----------------------------------------------------------------------------
#echo(pasDatabaseVersion)#
#echo(__FILEPATH__)#
"""

from os import path
import os
import re

from dNG.data.file import File
from dNG.pas.data.binary import Binary
from dNG.pas.data.settings import Settings
from dNG.pas.data.logging.log_line import LogLine
from dNG.pas.database.connection import Connection
from dNG.pas.database.instances.abstract import Abstract as _DbAbstract
from dNG.pas.database.instances.schema_version import SchemaVersion as _DbSchemaVersion
from dNG.pas.loader.interactive_cli import InteractiveCli
from dNG.pas.plugins.hook_context import HookContext
from dNG.pas.runtime.io_exception import IOException
from dNG.pas.runtime.type_exception import TypeException
from .instance import Instance
from .nothing_matched_exception import NothingMatchedException
from .transaction_context import TransactionContext

class Schema(Instance):
#
	"""
The "Schema" class provides methods to handle versions and upgrades.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
             Mozilla Public License, v. 2.0
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
	#
		"""
Constructor __init__(Schema)

:since: v0.1.00
		"""

		Instance.__init__(self, db_instance)
	#

	get_version = Instance._wrap_getter("version")
	"""
Returns the version for this schema entry.

:return: (int) Version
:since:  v0.1.00
	"""

	def set_data_attributes(self, **kwargs):
	#
		"""
Sets values given as keyword arguments to this method.

:since: v0.1.00
		"""

		self._ensure_thread_local_instance(_DbSchemaVersion)

		with self:
		#
			if ("name" in kwargs): self.local.db_instance.name = Binary.utf8(kwargs['name'])
			if ("version" in kwargs): self.local.db_instance.version = kwargs['version']
			if ("applied" in kwargs): self.local.db_instance.applied = kwargs['applied']
		#
	#

	@staticmethod
	def apply_version(instance_class):
	#
		"""
Applies all changed schema version files required for the given instance
class.

:param instance_class: Database instance class

:since: v0.1.00
		"""

		# pylint: disable=broad-except

		if (instance_class == None
		    or (not issubclass(instance_class, _DbAbstract))
		    or instance_class.db_schema_version == None
		   ): raise TypeException("Given instance class is invalid")

		instance_class_name = instance_class.__name__

		with Connection.get_instance() as connection, HookContext("dNG.pas.database.{0}.applySchema".format(instance_class_name)):
		#
			schema_directory_path = path.join(Settings.get("path_data"),
			                                  "database",
			                                  "{0}_schema".format(Connection.get_backend_name()),
			                                  instance_class_name
			                                 )

			schema_version_files = { }

			if (os.access(schema_directory_path, os.R_OK | os.X_OK)):
			#
				re_object = re.compile("schema\\_\\d+\\.sql$", re.I)

				schema_version_files = { filename: path.join(schema_directory_path, filename)
				                         for filename in os.listdir(schema_directory_path) if (re_object.match(filename) != None)
				                       }
			#

			target_version = instance_class.db_schema_version

			try:
			#
				schema_version = Schema.load_latest_name_entry(instance_class_name)
				current_version = schema_version.get_version()
			#
			except NothingMatchedException: current_version = 0

			if (current_version < 1):
			#
				LogLine.info("pas.Database schema '{0}' is at version {1:d}".format(instance_class_name, target_version))

				schema = Schema()
				schema.set_data_attributes(name = instance_class_name, version = target_version)
				schema.save()
			#
			elif (len(schema_version_files) > 0):
			#
				if (current_version < target_version):
				#
					LogLine.info("pas.Database will upgrade schema '{0}' from version {1:d} to {2:d}".format(instance_class_name, current_version, target_version))
					schema_versions = (version for version in range(current_version + 1, target_version + 1) if "schema_{0:d}.sql".format(version) in schema_version_files)

					try:
					#
						with TransactionContext():
						#
							for schema_version in schema_versions:
							#
								Schema._apply_sql_file(connection, schema_version_files["schema_{0:d}.sql".format(schema_version)])
								LogLine.info("pas.Database schema '{0}' is at version {1:d}".format(instance_class_name, target_version))

								schema = Schema()
								schema.set_data_attributes(name = instance_class_name, version = schema_version)
								schema.save()
							#
						#
					#
					except Exception:
					#
						cli = InteractiveCli.get_instance()
						if (isinstance(cli, InteractiveCli)): cli.output_error("An error occurred updating the database schema '{0}'".format(instance_class_name))

						raise
					#
				#
			#
		#
	#

	@staticmethod
	def _apply_sql_command(connection, sql_command):
	#
		"""
Applies the given SQL command to the database connection.

:param connection: Database connection
:param sql_command: Database specific SQL command

:since: v0.1.01
		"""

		connection.get_bind().execute(sql_command)
	#

	@staticmethod
	def _apply_sql_file(connection, file_pathname):
	#
		"""
Applies the given SQL file.

:param connection: Database connection
:param file_pathname: Database specific SQL file

:since: v0.1.00
		"""

		file_object = File()

		if (file_object.open(file_pathname, True, "r")):
		#
			file_content = Binary.str(file_object.read())
			file_object.close()
		#
		else: raise IOException("Schema file given is invalid")

		file_content = file_content.replace("__db_prefix__", _DbAbstract.get_table_prefix())
		file_content = Schema.RE_SQL_COMMENT_LINE.sub("", file_content)

		current_sql_command = ""
		sql_commands = file_content.split(";")

		for sql_command in sql_commands:
		#
			re_result = Schema.RE_ESCAPED.search(sql_command)

			if (re_result != None
			    and (len(re_result.group(1)) % 2) == 1
			   ): current_sql_command += sql_command
			else:
			#
				current_sql_command = (sql_command
				                       if (current_sql_command == "") else
				                       current_sql_command + sql_command
				                      )

				current_sql_command = current_sql_command.strip()

				if (current_sql_command != ""):
				#
					Schema._apply_sql_command(connection, current_sql_command)
					current_sql_command = ""
				#
			#
		#
	#

	@staticmethod
	def load_latest_name_entry(name):
	#
		"""
Load the schema entry with the highest version for the given name.

:param name: Schema name

:return: (object) Schema instance on success
:since:  v0.1.00
		"""

		_return = None

		with Connection.get_instance() as connection:
		#
			db_query = connection.query(_DbSchemaVersion).filter(_DbSchemaVersion.name == name)
			db_query = db_query.order_by(_DbSchemaVersion.version.desc()).limit(1)
			db_instance = db_query.first()

			if (db_instance == None): raise NothingMatchedException("Schema name '{0}' is invalid".format(name))
			_return = Schema(db_instance)
		#

		return _return
	#
#

##j## EOF