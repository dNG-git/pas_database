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

from sqlalchemy.orm.mapper import configure_mappers

from dNG.module.named_loader import NamedLoader

class AutoloadingPolymorphicMap(dict):
    """
"AutoloadingPolymorphicMap" is used to re-trigger the "configure_mappers()"
after loading SQLAlchemy instances by "dNG.module.NamedLoader".

:author:     direct Netware Group et al.
:copyright:  (C) direct Netware Group - All rights reserved
:package:    pas
:subpackage: database
:since:      v0.2.00
:license:    https://www.direct-netware.de/redirect?licenses;mpl2
             Mozilla Public License, v. 2.0
    """

    def __missing__(self, key):
        """
python.org: If a subclass of dict defines a method __missing__(), if the key
is not present, the d[key] operation calls that method with the key as
argument.

:param key: Polymorphic name we want to load

:return: (object) Polymorphic instance if found
:since:  v0.2.00
        """

        common_name = "dNG.database.instances.{0}".format(key)

        if ((not NamedLoader.is_defined(common_name, False))
            and NamedLoader.is_defined(common_name)
           ): configure_mappers()

        return dict.__getitem__(self, key)
    #
#
