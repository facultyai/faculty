"""
Shell helper functions.

Vendored from https://github.com/python/cpython/blob/3.6/Lib/shlex.py
"""

# Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010,
# 2011, 2012, 2013, 2014, 2015, 2016, 2017 Python Software Foundation; All
# Rights Reserved
#
# This file is distributed under the terms of the Python Software Foundation
# License Version 2: see https://github.com/python/cpython/blob/3.6/LICENSE

from __future__ import unicode_literals

import six


if six.PY3:
    from shlex import quote

else:
    import re

    _find_unsafe = re.compile(r"[^\w@%+=:,./-]").search

    def quote(s):
        """Return a shell-escaped version of the string *s*."""
        if not s:
            return "''"
        if _find_unsafe(s) is None:
            return s

        # use single quotes, and put single quotes into double quotes
        # the string $'b is then quoted as '$'"'"'b'
        return "'" + s.replace("'", "'\"'\"'") + "'"
