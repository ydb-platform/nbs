/*
  File: acl_to_any_text.c

  Copyright (C) 1999, 2000
  Andreas Gruenbacher, <andreas.gruenbacher@gmail.com>

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU Lesser General Public
  License as published by the Free Software Foundation; either
  version 2.1 of the License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
  Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public
  License along with this library; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
*/

#include "config.h"
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
#include "libacl.h"

char *
acl_to_any_text(acl_t acl, const char *prefix, char separator, int options)
{
	return __acl_to_any_text(acl, NULL, prefix, separator, NULL, options);
}
