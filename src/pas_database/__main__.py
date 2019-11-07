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

from argparse import ArgumentParser

from dpt_interactive_cli import InteractiveCli
from dpt_module_loader import NamedClassLoader
from dpt_plugins import Hook
from dpt_plugins import HookContext
from dpt_runtime.environment import Environment
from dpt_settings import Settings

from .connection import Connection
from .orm import Abstract
from .transaction_context import TransactionContext

class Application(InteractiveCli):
    """
Tool to work with the configured database and its tables.

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v1.0.0
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    # pylint: disable=unused-argument

    __slots__ = [ "_cli_setup" ]
    """
python.org: __slots__ reserves space for the declared variables and prevents
the automatic creation of __dict__ and __weakref__ for each instance.
    """

    def __init__(self):
        """
Constructor __init__(Application)

:param args: Parsed command line arguments

:since: v1.0.0
        """

        InteractiveCli.__init__(self)

        self._cli_setup = False
        """
True if this tool should handle the initial configuration.
        """

        self.arg_parser = ArgumentParser()
        self.arg_parser.add_argument("--applySchema", action = "store_true", dest = "apply_schema")
        self.arg_parser.add_argument("-s", action = "store_true", dest = "cli_setup")

        InteractiveCli.register_run_callback(self._on_run)
        InteractiveCli.register_shutdown_callback(self._on_shutdown)
    #

    @property
    def is_cli_setup(self):
        """
Returns true if this tool should handle the initial configuration.

:return: (bool) True if CLI should handle the initial configuration.
:since:  v1.0.0
        """

        return self._cli_setup
    #

    @InteractiveCli.log_handler.setter
    def log_handler(self, log_handler):
        """
Sets the LogHandler.

:param log_handler: LogHandler to use

:since: v1.0.0
        """

        InteractiveCli.log_handler.fset(self, log_handler)

        Hook.set_log_handler(log_handler)
        NamedClassLoader.set_log_handler(log_handler)
    #

    def _on_run(self, args):
        """
Callback for execution.

:since: v1.0.0
        """

        # pylint: disable=attribute-defined-outside-init

        Settings.read_file("{0}/settings/core.json".format(Settings.get("path_data")), True)
        Settings.read_file("{0}/settings/pas_database.json".format(Settings.get("path_data")), True)

        log_handler = NamedClassLoader.get_singleton("dpt_logging.LogHandler", False)

        if (log_handler is not None):
            self.log_handler = log_handler
            log_handler.debug("#echo(__FILEPATH__)# -{0!r}._on_run()- (#echo(__LINE__)#)", self, context = "pas_database")
        #

        self._cli_setup = args.cli_setup

        Hook.load("database")

        if (args.apply_schema): self.run_apply_schema(args)
    #

    def _on_shutdown(self):
        """
Callback for shutdown.

:since: v1.0.0
        """

        Hook.free()
    #

    def run_apply_schema(self, args):
        """
Callback for execution.

:since: v1.0.0
        """

        self.output_info("Loading database entities ...")

        with Connection.get_instance() as connection:
            Hook.call("pas.Database.loadAll")

            self.output_info("Applying schema ...")

            with HookContext("pas.Database.applySchema"), TransactionContext():
                Abstract().metadata.create_all(connection.get_bind())
            #
        #

        self.output_info("Process completed")
    #
#

def main():
    """
Application main entry point.

:since: v1.0.0
    """

    application = None
    if (not Environment.is_application_short_name_defined()): Environment.set_application_short_name("pas.database")

    try:
        application = Application()
        application.run()
    except Exception as handled_exception:
        if (application is not None):
            application.error(handled_exception)
            application.stop()
        else: sys.stderr.write("{0!r}".format(sys.exc_info()))
    #
#

if (__name__ == "__main__"): main()
