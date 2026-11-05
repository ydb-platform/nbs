/*
  File: acl_clear_perms.c

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
#include "libacl.h"


/* 23.4.3 */
int
acl_clear_perms(acl_permset_t permset_d)
{
	acl_permset_obj *acl_permset_obj_p = ext2int(acl_permset, permset_d);
	if (!acl_permset_obj_p)
		return -1;
	acl_permset_obj_p->sperm = ACL_PERM_NONE;
	return 0;
}

