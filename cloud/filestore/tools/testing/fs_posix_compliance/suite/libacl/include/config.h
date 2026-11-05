#pragma once

/* Static Linux configuration for the vendored acl 2.3.1 sources. */
#define EXPORT extern
#define HAVE_VISIBILITY_ATTRIBUTE 1

#define HAVE_ACL_DELETE_DEF_FILE 1
#define HAVE_ACL_ENTRIES 1
#define HAVE_ACL_FREE 1
#define HAVE_ACL_FROM_MODE 1
#define HAVE_ACL_FROM_TEXT 1
#define HAVE_ACL_GET_ENTRY 1
#define HAVE_ACL_GET_FD 1
#define HAVE_ACL_GET_FILE 1
#define HAVE_ACL_GET_PERM 1
#define HAVE_ACL_GET_PERMSET 1
#define HAVE_ACL_GET_TAG_TYPE 1
#define HAVE_ACL_SET_FD 1
#define HAVE_ACL_SET_FILE 1

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

