# -*- coding: utf-8 -*-
##j## BOF

"""
dNG.pas.loader.DbTool
"""
"""n// NOTE
----------------------------------------------------------------------------
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
----------------------------------------------------------------------------
NOTE_END //n"""

from argparse import ArgumentParser

from dNG.pas.data.settings import Settings
from dNG.pas.database.connection import Connection
from dNG.pas.database.instances.abstract import Abstract
from dNG.pas.loader.cli import Cli
from dNG.pas.module.named_loader import NamedLoader
from dNG.pas.plugins.hook import Hook

class DbTool(Cli):
#
	"""
Tool to work with the configured database and its tables.

:author:     direct Netware Group
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.1.00
:license:    http://www.direct-netware.de/redirect.py?licenses;mpl2
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

		Cli.__init__(self)

		self.arg_parser = ArgumentParser()

		Cli.register_run_callback(self._on_run)
		Cli.register_shutdown_callback(self._on_shutdown)
	#

	def _on_run(self, args):
	#
		"""
Callback for execution.

:since: v1.0.0
		"""

		# pylint: disable=no-member

		Settings.read_file("{0}/settings/pas_core.json".format(Settings.get("path_data")), True)
		Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)

		self.log_handler = NamedLoader.get_singleton("dNG.pas.data.logging.LogHandler", False)

		if (self.log_handler != None):
		#
			Hook.set_log_handler(self.log_handler)
			NamedLoader.set_log_handler(self.log_handler)

			self.log_handler.debug("#echo(__FILEPATH__)# -DbTool._on_run(args)- (#echo(__LINE__)#)")
		#

		Hook.load("database")
		Hook.register("dNG.pas.Status.stop", self.stop)
		Hook.call("dNG.pas.Database.loadAll")

		database = Connection.get_instance()
		Abstract().metadata.create_all(database.get_bind())
	#

	def _on_shutdown(self):
	#
		"""
Callback for shutdown.

:since: v0.1.00
		"""

		Hook.call("dNG.pas.Status.onShutdown")
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