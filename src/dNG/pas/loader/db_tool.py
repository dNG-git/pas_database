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

from argparse import ArgumentParser

from dNG.pas.data.settings import Settings
from dNG.pas.database.connection import Connection
from dNG.pas.database.instances.abstract import Abstract
from dNG.pas.database.transaction_context import TransactionContext
from dNG.pas.loader.interactive_cli import InteractiveCli
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hook import Hook
from dNG.pas.plugins.hook_context import HookContext

class DbTool(InteractiveCli):
#
	"""
Tool to work with the configured database and its tables.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
	"""

	# pylint: disable=unused-argument

	def __init__(self):
	#
		"""
Constructor __init__(DbTool)

:param args: Parsed command line arguments

:since: v0.1.00
		"""

		InteractiveCli.__init__(self)

		self.cli_setup = False
		"""
True if this tool should handle the initial configuration.
		"""

		self.arg_parser = ArgumentParser()
		self.arg_parser.add_argument("--applySchema", action = "store_true", dest = "apply_schema")
		self.arg_parser.add_argument("-s", action = "store_true", dest = "cli_setup")

		InteractiveCli.register_run_callback(self._on_run)
		InteractiveCli.register_shutdown_callback(self._on_shutdown)
	#

	def is_cli_setup(self):
	#
		"""
Returns true if this tool should handle the initial configuration.
		"""

		return self.cli_setup
	#

	def _on_run(self, args):
	#
		"""
Callback for execution.

:since: v0.1.00
		"""

		Settings.read_file("{0}/settings/pas_core.json".format(Settings.get("path_data")), True)
		Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)

		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)

		if (self.log_handler != None):
		#
			Hook.set_log_handler(self.log_handler)
			NamedLoader.set_log_handler(self.log_handler)

			self.log_handler.debug("#echo(__FILEPATH__)# -{0!r}._on_run()- (#echo(__LINE__)#)", self, context = "pas_database")
		#

		self.cli_setup = args.cli_setup

		Hook.load("database")
		Hook.register("dNG.pas.Status.stop", self.stop)

		if (args.apply_schema): self.run_apply_schema(args)
	#

	def _on_shutdown(self):
	#
		"""
Callback for shutdown.

:since: v0.1.00
		"""

		Hook.call("dNG.pas.Status.onShutdown")

		Hook.free()
	#

	def run_apply_schema(self, args):
	#
		"""
Callback for execution.

:since: v0.1.00
		"""

		self.output_info("Loading database entities ...")

		with Connection.get_instance() as connection:
		#
			Hook.call("dNG.pas.Database.loadAll")

			self.output_info("Applying schema ...")

			with TransactionContext(), HookContext("dNG.pas.Database.applySchema"):
			#
				Abstract().metadata.create_all(connection.get_bind())
			#
		#

		self.output_info("Process completed")
	#

	def stop(self, params = None, last_return = None):
	#
		"""
Stops running instances.

:param params: Parameter specified
:param last_return: The return value from the last hook called.

:return: (None) None to stop communication after this call
:since:  v0.1.00
		"""

		return last_return
	#
#

##j## EOF