#!/usr/bin/env python
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

from dNG.loader.db_tool import DbTool
import sys

db_tool = None

try:
#
	db_tool = DbTool()
	db_tool.run()
#
except KeyboardInterrupt: pass
except Exception as handled_exception:
#
	if (db_tool is not None):
	#
		db_tool.error(handled_exception)
		db_tool.stop()
	#
	else: sys.stderr.write("{0!r}".format(sys.exc_info()))
#

##j## EOF