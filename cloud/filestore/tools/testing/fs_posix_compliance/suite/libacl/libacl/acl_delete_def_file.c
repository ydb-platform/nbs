/*
  File: acl_delete_def_file.c

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
#include <sys/types.h>
#include <sys/xattr.h>
#include "byteorder.h"
#include "acl_ea.h"
#include "libacl.h"

/* 23.4.8 */
int
acl_delete_def_file(const char *path_p)
{
	int error;
	
	error = removexattr(path_p, ACL_EA_DEFAULT);
	if (error < 0 && errno != ENOATTR && errno != ENODATA)
		return -1;
	return 0;
}

