/*-------------------------------------------------------------------------
 *
 * auth.h
 *	  Definitions for network authentication routines
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/libpq/auth.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AUTH_H
#define AUTH_H

#include "libpq/libpq-be.h"

extern __thread char *pg_krb_server_keyfile;
extern __thread bool pg_krb_caseins_users;
extern char *pg_krb_realm;

extern void ClientAuthentication(Port *port);

/* Hook for plugins to get control in ClientAuthentication() */
typedef void (*ClientAuthentication_hook_type) (Port *, int);
extern __thread PGDLLIMPORT ClientAuthentication_hook_type ClientAuthentication_hook;

#endif							/* AUTH_H */
