/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015, 2018-2021 Free Software Foundation,
   Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

#ifndef YY_EMATCH_LIB_ROUTE_CLS_EMATCH_SYNTAX_H_INCLUDED
# define YY_EMATCH_LIB_ROUTE_CLS_EMATCH_SYNTAX_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int ematch_debug;
#endif
/* "%code requires" blocks.  */
#line 28 "lib/route/cls/ematch_syntax.y"


struct ematch_quoted {
	char *	data;
	size_t	len;
	int	index;
};


#line 59 "lib/route/cls/ematch_syntax.h"

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    ERROR = 258,                   /* ERROR  */
    LOGIC = 259,                   /* LOGIC  */
    NOT = 260,                     /* NOT  */
    OPERAND = 261,                 /* OPERAND  */
    NUMBER = 262,                  /* NUMBER  */
    ALIGN = 263,                   /* ALIGN  */
    LAYER = 264,                   /* LAYER  */
    KW_OPEN = 265,                 /* "("  */
    KW_CLOSE = 266,                /* ")"  */
    KW_PLUS = 267,                 /* "+"  */
    KW_MASK = 268,                 /* "mask"  */
    KW_SHIFT = 269,                /* ">>"  */
    KW_AT = 270,                   /* "at"  */
    EMATCH_CMP = 271,              /* "cmp"  */
    EMATCH_NBYTE = 272,            /* "pattern"  */
    EMATCH_TEXT = 273,             /* "text"  */
    EMATCH_META = 274,             /* "meta"  */
    KW_EQ = 275,                   /* "="  */
    KW_GT = 276,                   /* ">"  */
    KW_LT = 277,                   /* "<"  */
    KW_FROM = 278,                 /* "from"  */
    KW_TO = 279,                   /* "to"  */
    META_RANDOM = 280,             /* "random"  */
    META_LOADAVG_0 = 281,          /* "loadavg_0"  */
    META_LOADAVG_1 = 282,          /* "loadavg_1"  */
    META_LOADAVG_2 = 283,          /* "loadavg_2"  */
    META_DEV = 284,                /* "dev"  */
    META_PRIO = 285,               /* "prio"  */
    META_PROTO = 286,              /* "proto"  */
    META_PKTTYPE = 287,            /* "pkttype"  */
    META_PKTLEN = 288,             /* "pktlen"  */
    META_DATALEN = 289,            /* "datalen"  */
    META_MACLEN = 290,             /* "maclen"  */
    META_MARK = 291,               /* "mark"  */
    META_TCINDEX = 292,            /* "tcindex"  */
    META_RTCLASSID = 293,          /* "rtclassid"  */
    META_RTIIF = 294,              /* "rtiif"  */
    META_SK_FAMILY = 295,          /* "sk_family"  */
    META_SK_STATE = 296,           /* "sk_state"  */
    META_SK_REUSE = 297,           /* "sk_reuse"  */
    META_SK_REFCNT = 298,          /* "sk_refcnt"  */
    META_SK_RCVBUF = 299,          /* "sk_rcvbuf"  */
    META_SK_SNDBUF = 300,          /* "sk_sndbuf"  */
    META_SK_SHUTDOWN = 301,        /* "sk_shutdown"  */
    META_SK_PROTO = 302,           /* "sk_proto"  */
    META_SK_TYPE = 303,            /* "sk_type"  */
    META_SK_RMEM_ALLOC = 304,      /* "sk_rmem_alloc"  */
    META_SK_WMEM_ALLOC = 305,      /* "sk_wmem_alloc"  */
    META_SK_WMEM_QUEUED = 306,     /* "sk_wmem_queued"  */
    META_SK_RCV_QLEN = 307,        /* "sk_rcv_qlen"  */
    META_SK_SND_QLEN = 308,        /* "sk_snd_qlen"  */
    META_SK_ERR_QLEN = 309,        /* "sk_err_qlen"  */
    META_SK_FORWARD_ALLOCS = 310,  /* "sk_forward_allocs"  */
    META_SK_ALLOCS = 311,          /* "sk_allocs"  */
    META_SK_ROUTE_CAPS = 312,      /* "sk_route_caps"  */
    META_SK_HASH = 313,            /* "sk_hash"  */
    META_SK_LINGERTIME = 314,      /* "sk_lingertime"  */
    META_SK_ACK_BACKLOG = 315,     /* "sk_ack_backlog"  */
    META_SK_MAX_ACK_BACKLOG = 316, /* "sk_max_ack_backlog"  */
    META_SK_PRIO = 317,            /* "sk_prio"  */
    META_SK_RCVLOWAT = 318,        /* "sk_rcvlowat"  */
    META_SK_RCVTIMEO = 319,        /* "sk_rcvtimeo"  */
    META_SK_SNDTIMEO = 320,        /* "sk_sndtimeo"  */
    META_SK_SENDMSG_OFF = 321,     /* "sk_sendmsg_off"  */
    META_SK_WRITE_PENDING = 322,   /* "sk_write_pending"  */
    META_VLAN = 323,               /* "vlan"  */
    META_RXHASH = 324,             /* "rxhash"  */
    META_DEVNAME = 325,            /* "devname"  */
    META_SK_BOUND_IF = 326,        /* "sk_bound_if"  */
    STR = 327,                     /* STR  */
    QUOTED = 328                   /* QUOTED  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif
/* Token kinds.  */
#define YYEMPTY -2
#define YYEOF 0
#define YYerror 256
#define YYUNDEF 257
#define ERROR 258
#define LOGIC 259
#define NOT 260
#define OPERAND 261
#define NUMBER 262
#define ALIGN 263
#define LAYER 264
#define KW_OPEN 265
#define KW_CLOSE 266
#define KW_PLUS 267
#define KW_MASK 268
#define KW_SHIFT 269
#define KW_AT 270
#define EMATCH_CMP 271
#define EMATCH_NBYTE 272
#define EMATCH_TEXT 273
#define EMATCH_META 274
#define KW_EQ 275
#define KW_GT 276
#define KW_LT 277
#define KW_FROM 278
#define KW_TO 279
#define META_RANDOM 280
#define META_LOADAVG_0 281
#define META_LOADAVG_1 282
#define META_LOADAVG_2 283
#define META_DEV 284
#define META_PRIO 285
#define META_PROTO 286
#define META_PKTTYPE 287
#define META_PKTLEN 288
#define META_DATALEN 289
#define META_MACLEN 290
#define META_MARK 291
#define META_TCINDEX 292
#define META_RTCLASSID 293
#define META_RTIIF 294
#define META_SK_FAMILY 295
#define META_SK_STATE 296
#define META_SK_REUSE 297
#define META_SK_REFCNT 298
#define META_SK_RCVBUF 299
#define META_SK_SNDBUF 300
#define META_SK_SHUTDOWN 301
#define META_SK_PROTO 302
#define META_SK_TYPE 303
#define META_SK_RMEM_ALLOC 304
#define META_SK_WMEM_ALLOC 305
#define META_SK_WMEM_QUEUED 306
#define META_SK_RCV_QLEN 307
#define META_SK_SND_QLEN 308
#define META_SK_ERR_QLEN 309
#define META_SK_FORWARD_ALLOCS 310
#define META_SK_ALLOCS 311
#define META_SK_ROUTE_CAPS 312
#define META_SK_HASH 313
#define META_SK_LINGERTIME 314
#define META_SK_ACK_BACKLOG 315
#define META_SK_MAX_ACK_BACKLOG 316
#define META_SK_PRIO 317
#define META_SK_RCVLOWAT 318
#define META_SK_RCVTIMEO 319
#define META_SK_SNDTIMEO 320
#define META_SK_SENDMSG_OFF 321
#define META_SK_WRITE_PENDING 322
#define META_VLAN 323
#define META_RXHASH 324
#define META_DEVNAME 325
#define META_SK_BOUND_IF 326
#define STR 327
#define QUOTED 328

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 47 "lib/route/cls/ematch_syntax.y"

	struct tcf_em_cmp	cmp;
	struct ematch_quoted	q;
	struct rtnl_ematch *	e;
	struct rtnl_pktloc *	loc;
	struct rtnl_meta_value *mv;
	uint32_t		i;
	uint64_t		i64;
	char *			s;

#line 236 "lib/route/cls/ematch_syntax.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif




int ematch_parse (void *scanner, char **errp, struct nl_list_head *root);


#endif /* !YY_EMATCH_LIB_ROUTE_CLS_EMATCH_SYNTAX_H_INCLUDED  */
