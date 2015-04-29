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

from sqlalchemy.orm.mapper import configure_mappers

from dNG.pas.module.named_loader import NamedLoader

class AutoloadingPolymorphicMap(dict):
#
	def __missing__(self, key):
	#
		"""
python.org: If a subclass of dict defines a method __missing__(), if the key
is not present, the d[key] operation calls that method with the key as
argument.

:param key: Polymorphic name we want to load

:return: (object) Polymorphic instance if found
:since:  v0.1.00
		"""

		common_name = "dNG.pas.database.instances.{0}".format(key)

		if ((not NamedLoader.is_defined(common_name, False))
		    and NamedLoader.is_defined(common_name)
		   ): configure_mappers()

		return dict.__getitem__(self, key)
	#
#

##j## EOF