/* A Bison parser, made by GNU Bison 3.8.2.  */

/* Bison implementation for Yacc-like parsers in C

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

/* C LALR(1) parser skeleton written by Richard Stallman, by
   simplifying the original so-called "semantic" parser.  */

/* DO NOT RELY ON FEATURES THAT ARE NOT DOCUMENTED in the manual,
   especially those whose name start with YY_ or yy_.  They are
   private implementation details that can be changed or removed.  */

/* All symbols defined below should begin with yy or YY, to avoid
   infringing on user name space.  This should be done even for local
   variables, as they might otherwise be expanded by user macros.
   There are some unavoidable exceptions within include files to
   define necessary library symbols; they are noted "INFRINGES ON
   USER NAME SPACE" below.  */

/* Identify Bison output, and Bison version.  */
#define YYBISON 30802

/* Bison version string.  */
#define YYBISON_VERSION "3.8.2"

/* Skeleton name.  */
#define YYSKELETON_NAME "yacc.c"

/* Pure parsers.  */
#define YYPURE 1

/* Push parsers.  */
#define YYPUSH 0

/* Pull parsers.  */
#define YYPULL 1


/* Substitute the variable and function names.  */
#define yyparse         ematch_parse
#define yylex           ematch_lex
#define yyerror         ematch_error
#define yydebug         ematch_debug
#define yynerrs         ematch_nerrs

/* First part of user prologue.  */
#line 6 "lib/route/cls/ematch_syntax.y"

#include <linux/tc_ematch/tc_em_meta.h>

#include <linux/tc_ematch/tc_em_cmp.h>

#include <netlink/netlink.h>
#include <netlink/utils.h>
#include <netlink/route/pktloc.h>
#include <netlink/route/cls/ematch.h>
#include <netlink/route/cls/ematch/cmp.h>
#include <netlink/route/cls/ematch/nbyte.h>
#include <netlink/route/cls/ematch/text.h>
#include <netlink/route/cls/ematch/meta.h>

#include "nl-route.h"

#define META_ALLOC rtnl_meta_value_alloc_id
#define META_ID(name) TCF_META_ID_##name
#define META_INT TCF_META_TYPE_INT
#define META_VAR TCF_META_TYPE_VAR

#line 98 "lib/route/cls/ematch_syntax.c"

# ifndef YY_CAST
#  ifdef __cplusplus
#   define YY_CAST(Type, Val) static_cast<Type> (Val)
#   define YY_REINTERPRET_CAST(Type, Val) reinterpret_cast<Type> (Val)
#  else
#   define YY_CAST(Type, Val) ((Type) (Val))
#   define YY_REINTERPRET_CAST(Type, Val) ((Type) (Val))
#  endif
# endif
# ifndef YY_NULLPTR
#  if defined __cplusplus
#   if 201103L <= __cplusplus
#    define YY_NULLPTR nullptr
#   else
#    define YY_NULLPTR 0
#   endif
#  else
#   define YY_NULLPTR ((void*)0)
#  endif
# endif

#include "ematch_syntax.h"
/* Symbol kind.  */
enum yysymbol_kind_t
{
  YYSYMBOL_YYEMPTY = -2,
  YYSYMBOL_YYEOF = 0,                      /* "end of file"  */
  YYSYMBOL_YYerror = 1,                    /* error  */
  YYSYMBOL_YYUNDEF = 2,                    /* "invalid token"  */
  YYSYMBOL_ERROR = 3,                      /* ERROR  */
  YYSYMBOL_LOGIC = 4,                      /* LOGIC  */
  YYSYMBOL_NOT = 5,                        /* NOT  */
  YYSYMBOL_OPERAND = 6,                    /* OPERAND  */
  YYSYMBOL_NUMBER = 7,                     /* NUMBER  */
  YYSYMBOL_ALIGN = 8,                      /* ALIGN  */
  YYSYMBOL_LAYER = 9,                      /* LAYER  */
  YYSYMBOL_KW_OPEN = 10,                   /* "("  */
  YYSYMBOL_KW_CLOSE = 11,                  /* ")"  */
  YYSYMBOL_KW_PLUS = 12,                   /* "+"  */
  YYSYMBOL_KW_MASK = 13,                   /* "mask"  */
  YYSYMBOL_KW_SHIFT = 14,                  /* ">>"  */
  YYSYMBOL_KW_AT = 15,                     /* "at"  */
  YYSYMBOL_EMATCH_CMP = 16,                /* "cmp"  */
  YYSYMBOL_EMATCH_NBYTE = 17,              /* "pattern"  */
  YYSYMBOL_EMATCH_TEXT = 18,               /* "text"  */
  YYSYMBOL_EMATCH_META = 19,               /* "meta"  */
  YYSYMBOL_KW_EQ = 20,                     /* "="  */
  YYSYMBOL_KW_GT = 21,                     /* ">"  */
  YYSYMBOL_KW_LT = 22,                     /* "<"  */
  YYSYMBOL_KW_FROM = 23,                   /* "from"  */
  YYSYMBOL_KW_TO = 24,                     /* "to"  */
  YYSYMBOL_META_RANDOM = 25,               /* "random"  */
  YYSYMBOL_META_LOADAVG_0 = 26,            /* "loadavg_0"  */
  YYSYMBOL_META_LOADAVG_1 = 27,            /* "loadavg_1"  */
  YYSYMBOL_META_LOADAVG_2 = 28,            /* "loadavg_2"  */
  YYSYMBOL_META_DEV = 29,                  /* "dev"  */
  YYSYMBOL_META_PRIO = 30,                 /* "prio"  */
  YYSYMBOL_META_PROTO = 31,                /* "proto"  */
  YYSYMBOL_META_PKTTYPE = 32,              /* "pkttype"  */
  YYSYMBOL_META_PKTLEN = 33,               /* "pktlen"  */
  YYSYMBOL_META_DATALEN = 34,              /* "datalen"  */
  YYSYMBOL_META_MACLEN = 35,               /* "maclen"  */
  YYSYMBOL_META_MARK = 36,                 /* "mark"  */
  YYSYMBOL_META_TCINDEX = 37,              /* "tcindex"  */
  YYSYMBOL_META_RTCLASSID = 38,            /* "rtclassid"  */
  YYSYMBOL_META_RTIIF = 39,                /* "rtiif"  */
  YYSYMBOL_META_SK_FAMILY = 40,            /* "sk_family"  */
  YYSYMBOL_META_SK_STATE = 41,             /* "sk_state"  */
  YYSYMBOL_META_SK_REUSE = 42,             /* "sk_reuse"  */
  YYSYMBOL_META_SK_REFCNT = 43,            /* "sk_refcnt"  */
  YYSYMBOL_META_SK_RCVBUF = 44,            /* "sk_rcvbuf"  */
  YYSYMBOL_META_SK_SNDBUF = 45,            /* "sk_sndbuf"  */
  YYSYMBOL_META_SK_SHUTDOWN = 46,          /* "sk_shutdown"  */
  YYSYMBOL_META_SK_PROTO = 47,             /* "sk_proto"  */
  YYSYMBOL_META_SK_TYPE = 48,              /* "sk_type"  */
  YYSYMBOL_META_SK_RMEM_ALLOC = 49,        /* "sk_rmem_alloc"  */
  YYSYMBOL_META_SK_WMEM_ALLOC = 50,        /* "sk_wmem_alloc"  */
  YYSYMBOL_META_SK_WMEM_QUEUED = 51,       /* "sk_wmem_queued"  */
  YYSYMBOL_META_SK_RCV_QLEN = 52,          /* "sk_rcv_qlen"  */
  YYSYMBOL_META_SK_SND_QLEN = 53,          /* "sk_snd_qlen"  */
  YYSYMBOL_META_SK_ERR_QLEN = 54,          /* "sk_err_qlen"  */
  YYSYMBOL_META_SK_FORWARD_ALLOCS = 55,    /* "sk_forward_allocs"  */
  YYSYMBOL_META_SK_ALLOCS = 56,            /* "sk_allocs"  */
  YYSYMBOL_META_SK_ROUTE_CAPS = 57,        /* "sk_route_caps"  */
  YYSYMBOL_META_SK_HASH = 58,              /* "sk_hash"  */
  YYSYMBOL_META_SK_LINGERTIME = 59,        /* "sk_lingertime"  */
  YYSYMBOL_META_SK_ACK_BACKLOG = 60,       /* "sk_ack_backlog"  */
  YYSYMBOL_META_SK_MAX_ACK_BACKLOG = 61,   /* "sk_max_ack_backlog"  */
  YYSYMBOL_META_SK_PRIO = 62,              /* "sk_prio"  */
  YYSYMBOL_META_SK_RCVLOWAT = 63,          /* "sk_rcvlowat"  */
  YYSYMBOL_META_SK_RCVTIMEO = 64,          /* "sk_rcvtimeo"  */
  YYSYMBOL_META_SK_SNDTIMEO = 65,          /* "sk_sndtimeo"  */
  YYSYMBOL_META_SK_SENDMSG_OFF = 66,       /* "sk_sendmsg_off"  */
  YYSYMBOL_META_SK_WRITE_PENDING = 67,     /* "sk_write_pending"  */
  YYSYMBOL_META_VLAN = 68,                 /* "vlan"  */
  YYSYMBOL_META_RXHASH = 69,               /* "rxhash"  */
  YYSYMBOL_META_DEVNAME = 70,              /* "devname"  */
  YYSYMBOL_META_SK_BOUND_IF = 71,          /* "sk_bound_if"  */
  YYSYMBOL_STR = 72,                       /* STR  */
  YYSYMBOL_QUOTED = 73,                    /* QUOTED  */
  YYSYMBOL_YYACCEPT = 74,                  /* $accept  */
  YYSYMBOL_input = 75,                     /* input  */
  YYSYMBOL_expr = 76,                      /* expr  */
  YYSYMBOL_match = 77,                     /* match  */
  YYSYMBOL_ematch = 78,                    /* ematch  */
  YYSYMBOL_cmp_match = 79,                 /* cmp_match  */
  YYSYMBOL_cmp_expr = 80,                  /* cmp_expr  */
  YYSYMBOL_text_from = 81,                 /* text_from  */
  YYSYMBOL_text_to = 82,                   /* text_to  */
  YYSYMBOL_meta_value = 83,                /* meta_value  */
  YYSYMBOL_meta_int_id = 84,               /* meta_int_id  */
  YYSYMBOL_meta_var_id = 85,               /* meta_var_id  */
  YYSYMBOL_pattern = 86,                   /* pattern  */
  YYSYMBOL_pktloc = 87,                    /* pktloc  */
  YYSYMBOL_align = 88,                     /* align  */
  YYSYMBOL_mask = 89,                      /* mask  */
  YYSYMBOL_shift = 90,                     /* shift  */
  YYSYMBOL_operand = 91                    /* operand  */
};
typedef enum yysymbol_kind_t yysymbol_kind_t;


/* Second part of user prologue.  */
#line 58 "lib/route/cls/ematch_syntax.y"

extern int ematch_lex(YYSTYPE *, void *);

#define ematch_error yyerror
static void yyerror(void *scanner, char **errp, struct nl_list_head *root, const char *msg)
{
	if (msg)
		*errp = strdup(msg);
	else
		*errp = NULL;
}

#line 236 "lib/route/cls/ematch_syntax.c"


#ifdef short
# undef short
#endif

/* On compilers that do not define __PTRDIFF_MAX__ etc., make sure
   <limits.h> and (if available) <stdint.h> are included
   so that the code can choose integer types of a good width.  */

#ifndef __PTRDIFF_MAX__
# include <limits.h> /* INFRINGES ON USER NAME SPACE */
# if defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stdint.h> /* INFRINGES ON USER NAME SPACE */
#  define YY_STDINT_H
# endif
#endif

/* Narrow types that promote to a signed type and that can represent a
   signed or unsigned integer of at least N bits.  In tables they can
   save space and decrease cache pressure.  Promoting to a signed type
   helps avoid bugs in integer arithmetic.  */

#ifdef __INT_LEAST8_MAX__
typedef __INT_LEAST8_TYPE__ yytype_int8;
#elif defined YY_STDINT_H
typedef int_least8_t yytype_int8;
#else
typedef signed char yytype_int8;
#endif

#ifdef __INT_LEAST16_MAX__
typedef __INT_LEAST16_TYPE__ yytype_int16;
#elif defined YY_STDINT_H
typedef int_least16_t yytype_int16;
#else
typedef short yytype_int16;
#endif

/* Work around bug in HP-UX 11.23, which defines these macros
   incorrectly for preprocessor constants.  This workaround can likely
   be removed in 2023, as HPE has promised support for HP-UX 11.23
   (aka HP-UX 11i v2) only through the end of 2022; see Table 2 of
   <https://h20195.www2.hpe.com/V2/getpdf.aspx/4AA4-7673ENW.pdf>.  */
#ifdef __hpux
# undef UINT_LEAST8_MAX
# undef UINT_LEAST16_MAX
# define UINT_LEAST8_MAX 255
# define UINT_LEAST16_MAX 65535
#endif

#if defined __UINT_LEAST8_MAX__ && __UINT_LEAST8_MAX__ <= __INT_MAX__
typedef __UINT_LEAST8_TYPE__ yytype_uint8;
#elif (!defined __UINT_LEAST8_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST8_MAX <= INT_MAX)
typedef uint_least8_t yytype_uint8;
#elif !defined __UINT_LEAST8_MAX__ && UCHAR_MAX <= INT_MAX
typedef unsigned char yytype_uint8;
#else
typedef short yytype_uint8;
#endif

#if defined __UINT_LEAST16_MAX__ && __UINT_LEAST16_MAX__ <= __INT_MAX__
typedef __UINT_LEAST16_TYPE__ yytype_uint16;
#elif (!defined __UINT_LEAST16_MAX__ && defined YY_STDINT_H \
       && UINT_LEAST16_MAX <= INT_MAX)
typedef uint_least16_t yytype_uint16;
#elif !defined __UINT_LEAST16_MAX__ && USHRT_MAX <= INT_MAX
typedef unsigned short yytype_uint16;
#else
typedef int yytype_uint16;
#endif

#ifndef YYPTRDIFF_T
# if defined __PTRDIFF_TYPE__ && defined __PTRDIFF_MAX__
#  define YYPTRDIFF_T __PTRDIFF_TYPE__
#  define YYPTRDIFF_MAXIMUM __PTRDIFF_MAX__
# elif defined PTRDIFF_MAX
#  ifndef ptrdiff_t
#   include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  endif
#  define YYPTRDIFF_T ptrdiff_t
#  define YYPTRDIFF_MAXIMUM PTRDIFF_MAX
# else
#  define YYPTRDIFF_T long
#  define YYPTRDIFF_MAXIMUM LONG_MAX
# endif
#endif

#ifndef YYSIZE_T
# ifdef __SIZE_TYPE__
#  define YYSIZE_T __SIZE_TYPE__
# elif defined size_t
#  define YYSIZE_T size_t
# elif defined __STDC_VERSION__ && 199901 <= __STDC_VERSION__
#  include <stddef.h> /* INFRINGES ON USER NAME SPACE */
#  define YYSIZE_T size_t
# else
#  define YYSIZE_T unsigned
# endif
#endif

#define YYSIZE_MAXIMUM                                  \
  YY_CAST (YYPTRDIFF_T,                                 \
           (YYPTRDIFF_MAXIMUM < YY_CAST (YYSIZE_T, -1)  \
            ? YYPTRDIFF_MAXIMUM                         \
            : YY_CAST (YYSIZE_T, -1)))

#define YYSIZEOF(X) YY_CAST (YYPTRDIFF_T, sizeof (X))


/* Stored state numbers (used for stacks). */
typedef yytype_int8 yy_state_t;

/* State numbers in computations.  */
typedef int yy_state_fast_t;

#ifndef YY_
# if defined YYENABLE_NLS && YYENABLE_NLS
#  if ENABLE_NLS
#   include <libintl.h> /* INFRINGES ON USER NAME SPACE */
#   define YY_(Msgid) dgettext ("bison-runtime", Msgid)
#  endif
# endif
# ifndef YY_
#  define YY_(Msgid) Msgid
# endif
#endif


#ifndef YY_ATTRIBUTE_PURE
# if defined __GNUC__ && 2 < __GNUC__ + (96 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_PURE __attribute__ ((__pure__))
# else
#  define YY_ATTRIBUTE_PURE
# endif
#endif

#ifndef YY_ATTRIBUTE_UNUSED
# if defined __GNUC__ && 2 < __GNUC__ + (7 <= __GNUC_MINOR__)
#  define YY_ATTRIBUTE_UNUSED __attribute__ ((__unused__))
# else
#  define YY_ATTRIBUTE_UNUSED
# endif
#endif

/* Suppress unused-variable warnings by "using" E.  */
#if ! defined lint || defined __GNUC__
# define YY_USE(E) ((void) (E))
#else
# define YY_USE(E) /* empty */
#endif

/* Suppress an incorrect diagnostic about yylval being uninitialized.  */
#if defined __GNUC__ && ! defined __ICC && 406 <= __GNUC__ * 100 + __GNUC_MINOR__
# if __GNUC__ * 100 + __GNUC_MINOR__ < 407
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")
# else
#  define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN                           \
    _Pragma ("GCC diagnostic push")                                     \
    _Pragma ("GCC diagnostic ignored \"-Wuninitialized\"")              \
    _Pragma ("GCC diagnostic ignored \"-Wmaybe-uninitialized\"")
# endif
# define YY_IGNORE_MAYBE_UNINITIALIZED_END      \
    _Pragma ("GCC diagnostic pop")
#else
# define YY_INITIAL_VALUE(Value) Value
#endif
#ifndef YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
# define YY_IGNORE_MAYBE_UNINITIALIZED_END
#endif
#ifndef YY_INITIAL_VALUE
# define YY_INITIAL_VALUE(Value) /* Nothing. */
#endif

#if defined __cplusplus && defined __GNUC__ && ! defined __ICC && 6 <= __GNUC__
# define YY_IGNORE_USELESS_CAST_BEGIN                          \
    _Pragma ("GCC diagnostic push")                            \
    _Pragma ("GCC diagnostic ignored \"-Wuseless-cast\"")
# define YY_IGNORE_USELESS_CAST_END            \
    _Pragma ("GCC diagnostic pop")
#endif
#ifndef YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_BEGIN
# define YY_IGNORE_USELESS_CAST_END
#endif


#define YY_ASSERT(E) ((void) (0 && (E)))

#if 1

/* The parser invokes alloca or malloc; define the necessary symbols.  */

# ifdef YYSTACK_USE_ALLOCA
#  if YYSTACK_USE_ALLOCA
#   ifdef __GNUC__
#    define YYSTACK_ALLOC __builtin_alloca
#   elif defined __BUILTIN_VA_ARG_INCR
#    include <alloca.h> /* INFRINGES ON USER NAME SPACE */
#   elif defined _AIX
#    define YYSTACK_ALLOC __alloca
#   elif defined _MSC_VER
#    include <malloc.h> /* INFRINGES ON USER NAME SPACE */
#    define alloca _alloca
#   else
#    define YYSTACK_ALLOC alloca
#    if ! defined _ALLOCA_H && ! defined EXIT_SUCCESS
#     include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
      /* Use EXIT_SUCCESS as a witness for stdlib.h.  */
#     ifndef EXIT_SUCCESS
#      define EXIT_SUCCESS 0
#     endif
#    endif
#   endif
#  endif
# endif

# ifdef YYSTACK_ALLOC
   /* Pacify GCC's 'empty if-body' warning.  */
#  define YYSTACK_FREE(Ptr) do { /* empty */; } while (0)
#  ifndef YYSTACK_ALLOC_MAXIMUM
    /* The OS might guarantee only one guard page at the bottom of the stack,
       and a page size can be as small as 4096 bytes.  So we cannot safely
       invoke alloca (N) if N exceeds 4096.  Use a slightly smaller number
       to allow for a few compiler-allocated temporary stack slots.  */
#   define YYSTACK_ALLOC_MAXIMUM 4032 /* reasonable circa 2006 */
#  endif
# else
#  define YYSTACK_ALLOC YYMALLOC
#  define YYSTACK_FREE YYFREE
#  ifndef YYSTACK_ALLOC_MAXIMUM
#   define YYSTACK_ALLOC_MAXIMUM YYSIZE_MAXIMUM
#  endif
#  if (defined __cplusplus && ! defined EXIT_SUCCESS \
       && ! ((defined YYMALLOC || defined malloc) \
             && (defined YYFREE || defined free)))
#   include <stdlib.h> /* INFRINGES ON USER NAME SPACE */
#   ifndef EXIT_SUCCESS
#    define EXIT_SUCCESS 0
#   endif
#  endif
#  ifndef YYMALLOC
#   define YYMALLOC malloc
#   if ! defined malloc && ! defined EXIT_SUCCESS
void *malloc (YYSIZE_T); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
#  ifndef YYFREE
#   define YYFREE free
#   if ! defined free && ! defined EXIT_SUCCESS
void free (void *); /* INFRINGES ON USER NAME SPACE */
#   endif
#  endif
# endif
#endif /* 1 */

#if (! defined yyoverflow \
     && (! defined __cplusplus \
         || (defined YYSTYPE_IS_TRIVIAL && YYSTYPE_IS_TRIVIAL)))

/* A type that is properly aligned for any stack member.  */
union yyalloc
{
  yy_state_t yyss_alloc;
  YYSTYPE yyvs_alloc;
};

/* The size of the maximum gap between one aligned stack and the next.  */
# define YYSTACK_GAP_MAXIMUM (YYSIZEOF (union yyalloc) - 1)

/* The size of an array large to enough to hold all stacks, each with
   N elements.  */
# define YYSTACK_BYTES(N) \
     ((N) * (YYSIZEOF (yy_state_t) + YYSIZEOF (YYSTYPE)) \
      + YYSTACK_GAP_MAXIMUM)

# define YYCOPY_NEEDED 1

/* Relocate STACK from its old location to the new one.  The
   local variables YYSIZE and YYSTACKSIZE give the old and new number of
   elements in the stack, and YYPTR gives the new location of the
   stack.  Advance YYPTR to a properly aligned location for the next
   stack.  */
# define YYSTACK_RELOCATE(Stack_alloc, Stack)                           \
    do                                                                  \
      {                                                                 \
        YYPTRDIFF_T yynewbytes;                                         \
        YYCOPY (&yyptr->Stack_alloc, Stack, yysize);                    \
        Stack = &yyptr->Stack_alloc;                                    \
        yynewbytes = yystacksize * YYSIZEOF (*Stack) + YYSTACK_GAP_MAXIMUM; \
        yyptr += yynewbytes / YYSIZEOF (*yyptr);                        \
      }                                                                 \
    while (0)

#endif

#if defined YYCOPY_NEEDED && YYCOPY_NEEDED
/* Copy COUNT objects from SRC to DST.  The source and destination do
   not overlap.  */
# ifndef YYCOPY
#  if defined __GNUC__ && 1 < __GNUC__
#   define YYCOPY(Dst, Src, Count) \
      __builtin_memcpy (Dst, Src, YY_CAST (YYSIZE_T, (Count)) * sizeof (*(Src)))
#  else
#   define YYCOPY(Dst, Src, Count)              \
      do                                        \
        {                                       \
          YYPTRDIFF_T yyi;                      \
          for (yyi = 0; yyi < (Count); yyi++)   \
            (Dst)[yyi] = (Src)[yyi];            \
        }                                       \
      while (0)
#  endif
# endif
#endif /* !YYCOPY_NEEDED */

/* YYFINAL -- State number of the termination state.  */
#define YYFINAL  26
/* YYLAST -- Last index in YYTABLE.  */
#define YYLAST   138

/* YYNTOKENS -- Number of terminals.  */
#define YYNTOKENS  74
/* YYNNTS -- Number of nonterminals.  */
#define YYNNTS  18
/* YYNRULES -- Number of rules.  */
#define YYNRULES  84
/* YYNSTATES -- Number of states.  */
#define YYNSTATES  118

/* YYMAXUTOK -- Last valid token kind.  */
#define YYMAXUTOK   328


/* YYTRANSLATE(TOKEN-NUM) -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex, with out-of-bounds checking.  */
#define YYTRANSLATE(YYX)                                \
  (0 <= (YYX) && (YYX) <= YYMAXUTOK                     \
   ? YY_CAST (yysymbol_kind_t, yytranslate[YYX])        \
   : YYSYMBOL_YYUNDEF)

/* YYTRANSLATE[TOKEN-NUM] -- Symbol number corresponding to TOKEN-NUM
   as returned by yylex.  */
static const yytype_int8 yytranslate[] =
{
       0,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     2,     2,     2,     2,
       2,     2,     2,     2,     2,     2,     1,     2,     3,     4,
       5,     6,     7,     8,     9,    10,    11,    12,    13,    14,
      15,    16,    17,    18,    19,    20,    21,    22,    23,    24,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    72,    73
};

#if YYDEBUG
/* YYRLINE[YYN] -- Source line where rule number YYN was defined.  */
static const yytype_int16 yyrline[] =
{
       0,   157,   157,   159,   166,   170,   182,   187,   195,   210,
     228,   255,   274,   302,   304,   309,   330,   331,   337,   338,
     343,   345,   347,   349,   354,   355,   356,   357,   358,   359,
     360,   361,   362,   363,   364,   365,   366,   367,   368,   369,
     370,   371,   372,   373,   374,   375,   376,   377,   378,   379,
     380,   381,   382,   383,   384,   385,   386,   387,   388,   389,
     390,   391,   392,   393,   394,   395,   396,   397,   398,   402,
     403,   410,   414,   443,   456,   482,   483,   485,   491,   492,
     498,   499,   504,   506,   508
};
#endif

/** Accessing symbol of state STATE.  */
#define YY_ACCESSING_SYMBOL(State) YY_CAST (yysymbol_kind_t, yystos[State])

#if 1
/* The user-facing name of the symbol whose (internal) number is
   YYSYMBOL.  No bounds checking.  */
static const char *yysymbol_name (yysymbol_kind_t yysymbol) YY_ATTRIBUTE_UNUSED;

/* YYTNAME[SYMBOL-NUM] -- String name of the symbol SYMBOL-NUM.
   First, the terminals, then, starting at YYNTOKENS, nonterminals.  */
static const char *const yytname[] =
{
  "\"end of file\"", "error", "\"invalid token\"", "ERROR", "LOGIC",
  "NOT", "OPERAND", "NUMBER", "ALIGN", "LAYER", "\"(\"", "\")\"", "\"+\"",
  "\"mask\"", "\">>\"", "\"at\"", "\"cmp\"", "\"pattern\"", "\"text\"",
  "\"meta\"", "\"=\"", "\">\"", "\"<\"", "\"from\"", "\"to\"",
  "\"random\"", "\"loadavg_0\"", "\"loadavg_1\"", "\"loadavg_2\"",
  "\"dev\"", "\"prio\"", "\"proto\"", "\"pkttype\"", "\"pktlen\"",
  "\"datalen\"", "\"maclen\"", "\"mark\"", "\"tcindex\"", "\"rtclassid\"",
  "\"rtiif\"", "\"sk_family\"", "\"sk_state\"", "\"sk_reuse\"",
  "\"sk_refcnt\"", "\"sk_rcvbuf\"", "\"sk_sndbuf\"", "\"sk_shutdown\"",
  "\"sk_proto\"", "\"sk_type\"", "\"sk_rmem_alloc\"", "\"sk_wmem_alloc\"",
  "\"sk_wmem_queued\"", "\"sk_rcv_qlen\"", "\"sk_snd_qlen\"",
  "\"sk_err_qlen\"", "\"sk_forward_allocs\"", "\"sk_allocs\"",
  "\"sk_route_caps\"", "\"sk_hash\"", "\"sk_lingertime\"",
  "\"sk_ack_backlog\"", "\"sk_max_ack_backlog\"", "\"sk_prio\"",
  "\"sk_rcvlowat\"", "\"sk_rcvtimeo\"", "\"sk_sndtimeo\"",
  "\"sk_sendmsg_off\"", "\"sk_write_pending\"", "\"vlan\"", "\"rxhash\"",
  "\"devname\"", "\"sk_bound_if\"", "STR", "QUOTED", "$accept", "input",
  "expr", "match", "ematch", "cmp_match", "cmp_expr", "text_from",
  "text_to", "meta_value", "meta_int_id", "meta_var_id", "pattern",
  "pktloc", "align", "mask", "shift", "operand", YY_NULLPTR
};

static const char *
yysymbol_name (yysymbol_kind_t yysymbol)
{
  return yytname[yysymbol];
}
#endif

#define YYPACT_NINF (-63)

#define yypact_value_is_default(Yyn) \
  ((Yyn) == YYPACT_NINF)

#define YYTABLE_NINF (-76)

#define yytable_value_is_error(Yyn) \
  0

/* YYPACT[STATE-NUM] -- Index in YYTABLE of the portion describing
   STATE-NUM.  */
static const yytype_int8 yypact[] =
{
      -4,    15,   -13,    -8,    11,    10,    14,    25,    29,   -63,
      26,   -63,    37,   -63,   -63,   -63,    16,    33,   -63,   -63,
     -63,    32,     1,     1,   -28,    65,   -63,    11,   -63,   -63,
     -63,    38,    34,   -63,    36,    28,   -24,   -63,   -63,   -63,
     -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,
     -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,
     -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,
     -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,   -63,
     -63,   -63,   -63,   -63,   -63,   -63,    16,    39,    39,   -63,
     -63,    43,   -63,   -62,    31,    65,    44,    42,   -63,    42,
     -63,   -63,    41,     1,    35,    45,   -63,    50,   -63,   -63,
     -63,   -63,     1,    47,   -63,   -63,   -63,   -63
};

/* YYDEFACT[STATE-NUM] -- Default reduction number in state STATE-NUM.
   Performed when YYTABLE does not specify something else to do.  Zero
   means the default is an error.  */
static const yytype_int8 yydefact[] =
{
       2,    75,     0,     0,    75,     0,     0,     0,     0,    73,
       0,     3,     4,     7,     8,    14,     0,     0,     6,    77,
      76,     0,    75,    75,     0,     0,     1,    75,    82,    83,
      84,     0,     0,    12,     0,     0,     0,    21,    24,    25,
      26,    27,    28,    29,    30,    31,    32,    33,    34,    35,
      36,    37,    38,    39,    40,    41,    42,    43,    44,    45,
      46,    47,    48,    49,    50,    51,    52,    53,    54,    55,
      56,    57,    58,    59,    60,    61,    62,    63,    64,    65,
      66,    67,    68,    69,    70,    20,     0,    80,    80,     5,
      15,     0,    13,     0,    16,     0,     0,    78,    23,    78,
      72,    71,     0,    75,    18,     0,    81,     0,    22,    74,
       9,    17,    75,     0,    11,    79,    19,    10
};

/* YYPGOTO[NTERM-NUM].  */
static const yytype_int8 yypgoto[] =
{
     -63,   -63,    13,   -63,    59,   -63,    40,   -63,   -63,   -34,
     -63,   -63,   -63,   -23,   -63,   -36,   -22,   -21
};

/* YYDEFGOTO[NTERM-NUM].  */
static const yytype_int8 yydefgoto[] =
{
       0,    10,    11,    12,    13,    14,    15,   104,   113,    86,
      87,    88,   102,    16,    17,   108,    97,    31
};

/* YYTABLE[YYPACT[STATE-NUM]] -- What to do in state STATE-NUM.  If
   positive, shift that token.  If negative, reduce the rule whose
   number is the opposite.  If YYTABLE_NINF, syntax error.  */
static const yytype_int8 yytable[] =
{
      35,     1,    19,     2,     3,   -75,     4,    20,     2,     3,
     100,   101,     5,     6,     7,     8,     1,    21,     2,     3,
      22,     4,     2,     3,    23,     4,    26,     5,     6,     7,
       8,     5,     6,     7,     8,    24,    28,    29,    30,    25,
      89,    27,    32,    33,    36,    90,    91,    92,    93,    94,
      99,   106,   110,    96,   103,   107,   114,   115,   117,   112,
      18,   105,    34,   109,     0,    95,    98,     0,     9,     0,
       0,     0,    37,     9,     0,     0,     0,     0,     0,     0,
     111,     0,     0,     9,     0,     0,     0,     9,     0,   116,
      38,    39,    40,    41,    42,    43,    44,    45,    46,    47,
      48,    49,    50,    51,    52,    53,    54,    55,    56,    57,
      58,    59,    60,    61,    62,    63,    64,    65,    66,    67,
      68,    69,    70,    71,    72,    73,    74,    75,    76,    77,
      78,    79,    80,    81,    82,    83,    84,     0,    85
};

static const yytype_int8 yycheck[] =
{
      23,     5,    15,     7,     8,     9,    10,    15,     7,     8,
      72,    73,    16,    17,    18,    19,     5,     4,     7,     8,
      10,    10,     7,     8,    10,    10,     0,    16,    17,    18,
      19,    16,    17,    18,    19,    10,    20,    21,    22,    10,
      27,     4,     9,    11,    72,     7,    12,    11,    20,    73,
       7,     7,    11,    14,    23,    13,    11,     7,    11,    24,
       1,    95,    22,    99,    -1,    86,    88,    -1,    72,    -1,
      -1,    -1,     7,    72,    -1,    -1,    -1,    -1,    -1,    -1,
     103,    -1,    -1,    72,    -1,    -1,    -1,    72,    -1,   112,
      25,    26,    27,    28,    29,    30,    31,    32,    33,    34,
      35,    36,    37,    38,    39,    40,    41,    42,    43,    44,
      45,    46,    47,    48,    49,    50,    51,    52,    53,    54,
      55,    56,    57,    58,    59,    60,    61,    62,    63,    64,
      65,    66,    67,    68,    69,    70,    71,    -1,    73
};

/* YYSTOS[STATE-NUM] -- The symbol kind of the accessing symbol of
   state STATE-NUM.  */
static const yytype_int8 yystos[] =
{
       0,     5,     7,     8,    10,    16,    17,    18,    19,    72,
      75,    76,    77,    78,    79,    80,    87,    88,    78,    15,
      15,    76,    10,    10,    10,    10,     0,     4,    20,    21,
      22,    91,     9,    11,    80,    87,    72,     7,    25,    26,
      27,    28,    29,    30,    31,    32,    33,    34,    35,    36,
      37,    38,    39,    40,    41,    42,    43,    44,    45,    46,
      47,    48,    49,    50,    51,    52,    53,    54,    55,    56,
      57,    58,    59,    60,    61,    62,    63,    64,    65,    66,
      67,    68,    69,    70,    71,    73,    83,    84,    85,    76,
       7,    12,    11,    20,    73,    91,    14,    90,    90,     7,
      72,    73,    86,    23,    81,    83,     7,    13,    89,    89,
      11,    87,    24,    82,    11,     7,    87,    11
};

/* YYR1[RULE-NUM] -- Symbol kind of the left-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr1[] =
{
       0,    74,    75,    75,    76,    76,    77,    77,    78,    78,
      78,    78,    78,    79,    79,    80,    81,    81,    82,    82,
      83,    83,    83,    83,    84,    84,    84,    84,    84,    84,
      84,    84,    84,    84,    84,    84,    84,    84,    84,    84,
      84,    84,    84,    84,    84,    84,    84,    84,    84,    84,
      84,    84,    84,    84,    84,    84,    84,    84,    84,    84,
      84,    84,    84,    84,    84,    84,    84,    84,    84,    85,
      85,    86,    86,    87,    87,    88,    88,    88,    89,    89,
      90,    90,    91,    91,    91
};

/* YYR2[RULE-NUM] -- Number of symbols on the right-hand side of rule RULE-NUM.  */
static const yytype_int8 yyr2[] =
{
       0,     2,     0,     1,     1,     3,     2,     1,     1,     6,
       7,     6,     3,     4,     1,     3,     0,     2,     0,     2,
       1,     1,     3,     2,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     1,     1,     1,     1,     1,     1,
       1,     1,     1,     1,     5,     0,     2,     2,     0,     2,
       0,     2,     1,     1,     1
};


enum { YYENOMEM = -2 };

#define yyerrok         (yyerrstatus = 0)
#define yyclearin       (yychar = YYEMPTY)

#define YYACCEPT        goto yyacceptlab
#define YYABORT         goto yyabortlab
#define YYERROR         goto yyerrorlab
#define YYNOMEM         goto yyexhaustedlab


#define YYRECOVERING()  (!!yyerrstatus)

#define YYBACKUP(Token, Value)                                    \
  do                                                              \
    if (yychar == YYEMPTY)                                        \
      {                                                           \
        yychar = (Token);                                         \
        yylval = (Value);                                         \
        YYPOPSTACK (yylen);                                       \
        yystate = *yyssp;                                         \
        goto yybackup;                                            \
      }                                                           \
    else                                                          \
      {                                                           \
        yyerror (scanner, errp, root, YY_("syntax error: cannot back up")); \
        YYERROR;                                                  \
      }                                                           \
  while (0)

/* Backward compatibility with an undocumented macro.
   Use YYerror or YYUNDEF. */
#define YYERRCODE YYUNDEF


/* Enable debugging if requested.  */
#if YYDEBUG

# ifndef YYFPRINTF
#  include <stdio.h> /* INFRINGES ON USER NAME SPACE */
#  define YYFPRINTF fprintf
# endif

# define YYDPRINTF(Args)                        \
do {                                            \
  if (yydebug)                                  \
    YYFPRINTF Args;                             \
} while (0)




# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)                    \
do {                                                                      \
  if (yydebug)                                                            \
    {                                                                     \
      YYFPRINTF (stderr, "%s ", Title);                                   \
      yy_symbol_print (stderr,                                            \
                  Kind, Value, scanner, errp, root); \
      YYFPRINTF (stderr, "\n");                                           \
    }                                                                     \
} while (0)


/*-----------------------------------.
| Print this symbol's value on YYO.  |
`-----------------------------------*/

static void
yy_symbol_value_print (FILE *yyo,
                       yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, void *scanner, char **errp, struct nl_list_head *root)
{
  FILE *yyoutput = yyo;
  YY_USE (yyoutput);
  YY_USE (scanner);
  YY_USE (errp);
  YY_USE (root);
  if (!yyvaluep)
    return;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  YY_USE (yykind);
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}


/*---------------------------.
| Print this symbol on YYO.  |
`---------------------------*/

static void
yy_symbol_print (FILE *yyo,
                 yysymbol_kind_t yykind, YYSTYPE const * const yyvaluep, void *scanner, char **errp, struct nl_list_head *root)
{
  YYFPRINTF (yyo, "%s %s (",
             yykind < YYNTOKENS ? "token" : "nterm", yysymbol_name (yykind));

  yy_symbol_value_print (yyo, yykind, yyvaluep, scanner, errp, root);
  YYFPRINTF (yyo, ")");
}

/*------------------------------------------------------------------.
| yy_stack_print -- Print the state stack from its BOTTOM up to its |
| TOP (included).                                                   |
`------------------------------------------------------------------*/

static void
yy_stack_print (yy_state_t *yybottom, yy_state_t *yytop)
{
  YYFPRINTF (stderr, "Stack now");
  for (; yybottom <= yytop; yybottom++)
    {
      int yybot = *yybottom;
      YYFPRINTF (stderr, " %d", yybot);
    }
  YYFPRINTF (stderr, "\n");
}

# define YY_STACK_PRINT(Bottom, Top)                            \
do {                                                            \
  if (yydebug)                                                  \
    yy_stack_print ((Bottom), (Top));                           \
} while (0)


/*------------------------------------------------.
| Report that the YYRULE is going to be reduced.  |
`------------------------------------------------*/

static void
yy_reduce_print (yy_state_t *yyssp, YYSTYPE *yyvsp,
                 int yyrule, void *scanner, char **errp, struct nl_list_head *root)
{
  int yylno = yyrline[yyrule];
  int yynrhs = yyr2[yyrule];
  int yyi;
  YYFPRINTF (stderr, "Reducing stack by rule %d (line %d):\n",
             yyrule - 1, yylno);
  /* The symbols being reduced.  */
  for (yyi = 0; yyi < yynrhs; yyi++)
    {
      YYFPRINTF (stderr, "   $%d = ", yyi + 1);
      yy_symbol_print (stderr,
                       YY_ACCESSING_SYMBOL (+yyssp[yyi + 1 - yynrhs]),
                       &yyvsp[(yyi + 1) - (yynrhs)], scanner, errp, root);
      YYFPRINTF (stderr, "\n");
    }
}

# define YY_REDUCE_PRINT(Rule)          \
do {                                    \
  if (yydebug)                          \
    yy_reduce_print (yyssp, yyvsp, Rule, scanner, errp, root); \
} while (0)

/* Nonzero means print parse trace.  It is left uninitialized so that
   multiple parsers can coexist.  */
int yydebug;
#else /* !YYDEBUG */
# define YYDPRINTF(Args) ((void) 0)
# define YY_SYMBOL_PRINT(Title, Kind, Value, Location)
# define YY_STACK_PRINT(Bottom, Top)
# define YY_REDUCE_PRINT(Rule)
#endif /* !YYDEBUG */


/* YYINITDEPTH -- initial size of the parser's stacks.  */
#ifndef YYINITDEPTH
# define YYINITDEPTH 200
#endif

/* YYMAXDEPTH -- maximum size the stacks can grow to (effective only
   if the built-in stack extension method is used).

   Do not make this value too large; the results are undefined if
   YYSTACK_ALLOC_MAXIMUM < YYSTACK_BYTES (YYMAXDEPTH)
   evaluated with infinite-precision integer arithmetic.  */

#ifndef YYMAXDEPTH
# define YYMAXDEPTH 10000
#endif


/* Context of a parse error.  */
typedef struct
{
  yy_state_t *yyssp;
  yysymbol_kind_t yytoken;
} yypcontext_t;

/* Put in YYARG at most YYARGN of the expected tokens given the
   current YYCTX, and return the number of tokens stored in YYARG.  If
   YYARG is null, return the number of expected tokens (guaranteed to
   be less than YYNTOKENS).  Return YYENOMEM on memory exhaustion.
   Return 0 if there are more than YYARGN expected tokens, yet fill
   YYARG up to YYARGN. */
static int
yypcontext_expected_tokens (const yypcontext_t *yyctx,
                            yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  int yyn = yypact[+*yyctx->yyssp];
  if (!yypact_value_is_default (yyn))
    {
      /* Start YYX at -YYN if negative to avoid negative indexes in
         YYCHECK.  In other words, skip the first -YYN actions for
         this state because they are default actions.  */
      int yyxbegin = yyn < 0 ? -yyn : 0;
      /* Stay within bounds of both yycheck and yytname.  */
      int yychecklim = YYLAST - yyn + 1;
      int yyxend = yychecklim < YYNTOKENS ? yychecklim : YYNTOKENS;
      int yyx;
      for (yyx = yyxbegin; yyx < yyxend; ++yyx)
        if (yycheck[yyx + yyn] == yyx && yyx != YYSYMBOL_YYerror
            && !yytable_value_is_error (yytable[yyx + yyn]))
          {
            if (!yyarg)
              ++yycount;
            else if (yycount == yyargn)
              return 0;
            else
              yyarg[yycount++] = YY_CAST (yysymbol_kind_t, yyx);
          }
    }
  if (yyarg && yycount == 0 && 0 < yyargn)
    yyarg[0] = YYSYMBOL_YYEMPTY;
  return yycount;
}




#ifndef yystrlen
# if defined __GLIBC__ && defined _STRING_H
#  define yystrlen(S) (YY_CAST (YYPTRDIFF_T, strlen (S)))
# else
/* Return the length of YYSTR.  */
static YYPTRDIFF_T
yystrlen (const char *yystr)
{
  YYPTRDIFF_T yylen;
  for (yylen = 0; yystr[yylen]; yylen++)
    continue;
  return yylen;
}
# endif
#endif

#ifndef yystpcpy
# if defined __GLIBC__ && defined _STRING_H && defined _GNU_SOURCE
#  define yystpcpy stpcpy
# else
/* Copy YYSRC to YYDEST, returning the address of the terminating '\0' in
   YYDEST.  */
static char *
yystpcpy (char *yydest, const char *yysrc)
{
  char *yyd = yydest;
  const char *yys = yysrc;

  while ((*yyd++ = *yys++) != '\0')
    continue;

  return yyd - 1;
}
# endif
#endif

#ifndef yytnamerr
/* Copy to YYRES the contents of YYSTR after stripping away unnecessary
   quotes and backslashes, so that it's suitable for yyerror.  The
   heuristic is that double-quoting is unnecessary unless the string
   contains an apostrophe, a comma, or backslash (other than
   backslash-backslash).  YYSTR is taken from yytname.  If YYRES is
   null, do not copy; instead, return the length of what the result
   would have been.  */
static YYPTRDIFF_T
yytnamerr (char *yyres, const char *yystr)
{
  if (*yystr == '"')
    {
      YYPTRDIFF_T yyn = 0;
      char const *yyp = yystr;
      for (;;)
        switch (*++yyp)
          {
          case '\'':
          case ',':
            goto do_not_strip_quotes;

          case '\\':
            if (*++yyp != '\\')
              goto do_not_strip_quotes;
            else
              goto append;

          append:
          default:
            if (yyres)
              yyres[yyn] = *yyp;
            yyn++;
            break;

          case '"':
            if (yyres)
              yyres[yyn] = '\0';
            return yyn;
          }
    do_not_strip_quotes: ;
    }

  if (yyres)
    return yystpcpy (yyres, yystr) - yyres;
  else
    return yystrlen (yystr);
}
#endif


static int
yy_syntax_error_arguments (const yypcontext_t *yyctx,
                           yysymbol_kind_t yyarg[], int yyargn)
{
  /* Actual size of YYARG. */
  int yycount = 0;
  /* There are many possibilities here to consider:
     - If this state is a consistent state with a default action, then
       the only way this function was invoked is if the default action
       is an error action.  In that case, don't check for expected
       tokens because there are none.
     - The only way there can be no lookahead present (in yychar) is if
       this state is a consistent state with a default action.  Thus,
       detecting the absence of a lookahead is sufficient to determine
       that there is no unexpected or expected token to report.  In that
       case, just report a simple "syntax error".
     - Don't assume there isn't a lookahead just because this state is a
       consistent state with a default action.  There might have been a
       previous inconsistent state, consistent state with a non-default
       action, or user semantic action that manipulated yychar.
     - Of course, the expected token list depends on states to have
       correct lookahead information, and it depends on the parser not
       to perform extra reductions after fetching a lookahead from the
       scanner and before detecting a syntax error.  Thus, state merging
       (from LALR or IELR) and default reductions corrupt the expected
       token list.  However, the list is correct for canonical LR with
       one exception: it will still contain any token that will not be
       accepted due to an error action in a later state.
  */
  if (yyctx->yytoken != YYSYMBOL_YYEMPTY)
    {
      int yyn;
      if (yyarg)
        yyarg[yycount] = yyctx->yytoken;
      ++yycount;
      yyn = yypcontext_expected_tokens (yyctx,
                                        yyarg ? yyarg + 1 : yyarg, yyargn - 1);
      if (yyn == YYENOMEM)
        return YYENOMEM;
      else
        yycount += yyn;
    }
  return yycount;
}

/* Copy into *YYMSG, which is of size *YYMSG_ALLOC, an error message
   about the unexpected token YYTOKEN for the state stack whose top is
   YYSSP.

   Return 0 if *YYMSG was successfully written.  Return -1 if *YYMSG is
   not large enough to hold the message.  In that case, also set
   *YYMSG_ALLOC to the required number of bytes.  Return YYENOMEM if the
   required number of bytes is too large to store.  */
static int
yysyntax_error (YYPTRDIFF_T *yymsg_alloc, char **yymsg,
                const yypcontext_t *yyctx)
{
  enum { YYARGS_MAX = 5 };
  /* Internationalized format string. */
  const char *yyformat = YY_NULLPTR;
  /* Arguments of yyformat: reported tokens (one for the "unexpected",
     one per "expected"). */
  yysymbol_kind_t yyarg[YYARGS_MAX];
  /* Cumulated lengths of YYARG.  */
  YYPTRDIFF_T yysize = 0;

  /* Actual size of YYARG. */
  int yycount = yy_syntax_error_arguments (yyctx, yyarg, YYARGS_MAX);
  if (yycount == YYENOMEM)
    return YYENOMEM;

  switch (yycount)
    {
#define YYCASE_(N, S)                       \
      case N:                               \
        yyformat = S;                       \
        break
    default: /* Avoid compiler warnings. */
      YYCASE_(0, YY_("syntax error"));
      YYCASE_(1, YY_("syntax error, unexpected %s"));
      YYCASE_(2, YY_("syntax error, unexpected %s, expecting %s"));
      YYCASE_(3, YY_("syntax error, unexpected %s, expecting %s or %s"));
      YYCASE_(4, YY_("syntax error, unexpected %s, expecting %s or %s or %s"));
      YYCASE_(5, YY_("syntax error, unexpected %s, expecting %s or %s or %s or %s"));
#undef YYCASE_
    }

  /* Compute error message size.  Don't count the "%s"s, but reserve
     room for the terminator.  */
  yysize = yystrlen (yyformat) - 2 * yycount + 1;
  {
    int yyi;
    for (yyi = 0; yyi < yycount; ++yyi)
      {
        YYPTRDIFF_T yysize1
          = yysize + yytnamerr (YY_NULLPTR, yytname[yyarg[yyi]]);
        if (yysize <= yysize1 && yysize1 <= YYSTACK_ALLOC_MAXIMUM)
          yysize = yysize1;
        else
          return YYENOMEM;
      }
  }

  if (*yymsg_alloc < yysize)
    {
      *yymsg_alloc = 2 * yysize;
      if (! (yysize <= *yymsg_alloc
             && *yymsg_alloc <= YYSTACK_ALLOC_MAXIMUM))
        *yymsg_alloc = YYSTACK_ALLOC_MAXIMUM;
      return -1;
    }

  /* Avoid sprintf, as that infringes on the user's name space.
     Don't have undefined behavior even if the translation
     produced a string with the wrong number of "%s"s.  */
  {
    char *yyp = *yymsg;
    int yyi = 0;
    while ((*yyp = *yyformat) != '\0')
      if (*yyp == '%' && yyformat[1] == 's' && yyi < yycount)
        {
          yyp += yytnamerr (yyp, yytname[yyarg[yyi++]]);
          yyformat += 2;
        }
      else
        {
          ++yyp;
          ++yyformat;
        }
  }
  return 0;
}


/*-----------------------------------------------.
| Release the memory associated to this symbol.  |
`-----------------------------------------------*/

static void
yydestruct (const char *yymsg,
            yysymbol_kind_t yykind, YYSTYPE *yyvaluep, void *scanner, char **errp, struct nl_list_head *root)
{
  YY_USE (yyvaluep);
  YY_USE (scanner);
  YY_USE (errp);
  YY_USE (root);
  if (!yymsg)
    yymsg = "Deleting";
  YY_SYMBOL_PRINT (yymsg, yykind, yyvaluep, yylocationp);

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  switch (yykind)
    {
    case YYSYMBOL_STR: /* STR  */
#line 148 "lib/route/cls/ematch_syntax.y"
            { free(((*yyvaluep).s)); NL_DBG(2, "string destructor\n"); }
#line 1300 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_QUOTED: /* QUOTED  */
#line 150 "lib/route/cls/ematch_syntax.y"
            { free(((*yyvaluep).q).data); NL_DBG(2, "quoted destructor\n"); }
#line 1306 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_text_from: /* text_from  */
#line 149 "lib/route/cls/ematch_syntax.y"
            { rtnl_pktloc_put(((*yyvaluep).loc)); NL_DBG(2, "pktloc destructor\n"); }
#line 1312 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_text_to: /* text_to  */
#line 149 "lib/route/cls/ematch_syntax.y"
            { rtnl_pktloc_put(((*yyvaluep).loc)); NL_DBG(2, "pktloc destructor\n"); }
#line 1318 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_meta_value: /* meta_value  */
#line 151 "lib/route/cls/ematch_syntax.y"
            { rtnl_meta_value_put(((*yyvaluep).mv)); NL_DBG(2, "meta value destructor\n"); }
#line 1324 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_pattern: /* pattern  */
#line 150 "lib/route/cls/ematch_syntax.y"
            { free(((*yyvaluep).q).data); NL_DBG(2, "quoted destructor\n"); }
#line 1330 "lib/route/cls/ematch_syntax.c"
        break;

    case YYSYMBOL_pktloc: /* pktloc  */
#line 149 "lib/route/cls/ematch_syntax.y"
            { rtnl_pktloc_put(((*yyvaluep).loc)); NL_DBG(2, "pktloc destructor\n"); }
#line 1336 "lib/route/cls/ematch_syntax.c"
        break;

      default:
        break;
    }
  YY_IGNORE_MAYBE_UNINITIALIZED_END
}






/*----------.
| yyparse.  |
`----------*/

int
yyparse (void *scanner, char **errp, struct nl_list_head *root)
{
/* Lookahead token kind.  */
int yychar;


/* The semantic value of the lookahead symbol.  */
/* Default value used for initialization, for pacifying older GCCs
   or non-GCC compilers.  */
YY_INITIAL_VALUE (static YYSTYPE yyval_default;)
YYSTYPE yylval YY_INITIAL_VALUE (= yyval_default);

    /* Number of syntax errors so far.  */
    int yynerrs = 0;

    yy_state_fast_t yystate = 0;
    /* Number of tokens to shift before error messages enabled.  */
    int yyerrstatus = 0;

    /* Refer to the stacks through separate pointers, to allow yyoverflow
       to reallocate them elsewhere.  */

    /* Their size.  */
    YYPTRDIFF_T yystacksize = YYINITDEPTH;

    /* The state stack: array, bottom, top.  */
    yy_state_t yyssa[YYINITDEPTH];
    yy_state_t *yyss = yyssa;
    yy_state_t *yyssp = yyss;

    /* The semantic value stack: array, bottom, top.  */
    YYSTYPE yyvsa[YYINITDEPTH];
    YYSTYPE *yyvs = yyvsa;
    YYSTYPE *yyvsp = yyvs;

  int yyn;
  /* The return value of yyparse.  */
  int yyresult;
  /* Lookahead symbol kind.  */
  yysymbol_kind_t yytoken = YYSYMBOL_YYEMPTY;
  /* The variables used to return semantic value and location from the
     action routines.  */
  YYSTYPE yyval;

  /* Buffer for error messages, and its allocated size.  */
  char yymsgbuf[128];
  char *yymsg = yymsgbuf;
  YYPTRDIFF_T yymsg_alloc = sizeof yymsgbuf;

#define YYPOPSTACK(N)   (yyvsp -= (N), yyssp -= (N))

  /* The number of symbols on the RHS of the reduced rule.
     Keep to zero when no symbol should be popped.  */
  int yylen = 0;

  YYDPRINTF ((stderr, "Starting parse\n"));

  yychar = YYEMPTY; /* Cause a token to be read.  */

  goto yysetstate;


/*------------------------------------------------------------.
| yynewstate -- push a new state, which is found in yystate.  |
`------------------------------------------------------------*/
yynewstate:
  /* In all cases, when you get here, the value and location stacks
     have just been pushed.  So pushing a state here evens the stacks.  */
  yyssp++;


/*--------------------------------------------------------------------.
| yysetstate -- set current state (the top of the stack) to yystate.  |
`--------------------------------------------------------------------*/
yysetstate:
  YYDPRINTF ((stderr, "Entering state %d\n", yystate));
  YY_ASSERT (0 <= yystate && yystate < YYNSTATES);
  YY_IGNORE_USELESS_CAST_BEGIN
  *yyssp = YY_CAST (yy_state_t, yystate);
  YY_IGNORE_USELESS_CAST_END
  YY_STACK_PRINT (yyss, yyssp);

  if (yyss + yystacksize - 1 <= yyssp)
#if !defined yyoverflow && !defined YYSTACK_RELOCATE
    YYNOMEM;
#else
    {
      /* Get the current used size of the three stacks, in elements.  */
      YYPTRDIFF_T yysize = yyssp - yyss + 1;

# if defined yyoverflow
      {
        /* Give user a chance to reallocate the stack.  Use copies of
           these so that the &'s don't force the real ones into
           memory.  */
        yy_state_t *yyss1 = yyss;
        YYSTYPE *yyvs1 = yyvs;

        /* Each stack pointer address is followed by the size of the
           data in use in that stack, in bytes.  This used to be a
           conditional around just the two extra args, but that might
           be undefined if yyoverflow is a macro.  */
        yyoverflow (YY_("memory exhausted"),
                    &yyss1, yysize * YYSIZEOF (*yyssp),
                    &yyvs1, yysize * YYSIZEOF (*yyvsp),
                    &yystacksize);
        yyss = yyss1;
        yyvs = yyvs1;
      }
# else /* defined YYSTACK_RELOCATE */
      /* Extend the stack our own way.  */
      if (YYMAXDEPTH <= yystacksize)
        YYNOMEM;
      yystacksize *= 2;
      if (YYMAXDEPTH < yystacksize)
        yystacksize = YYMAXDEPTH;

      {
        yy_state_t *yyss1 = yyss;
        union yyalloc *yyptr =
          YY_CAST (union yyalloc *,
                   YYSTACK_ALLOC (YY_CAST (YYSIZE_T, YYSTACK_BYTES (yystacksize))));
        if (! yyptr)
          YYNOMEM;
        YYSTACK_RELOCATE (yyss_alloc, yyss);
        YYSTACK_RELOCATE (yyvs_alloc, yyvs);
#  undef YYSTACK_RELOCATE
        if (yyss1 != yyssa)
          YYSTACK_FREE (yyss1);
      }
# endif

      yyssp = yyss + yysize - 1;
      yyvsp = yyvs + yysize - 1;

      YY_IGNORE_USELESS_CAST_BEGIN
      YYDPRINTF ((stderr, "Stack size increased to %ld\n",
                  YY_CAST (long, yystacksize)));
      YY_IGNORE_USELESS_CAST_END

      if (yyss + yystacksize - 1 <= yyssp)
        YYABORT;
    }
#endif /* !defined yyoverflow && !defined YYSTACK_RELOCATE */


  if (yystate == YYFINAL)
    YYACCEPT;

  goto yybackup;


/*-----------.
| yybackup.  |
`-----------*/
yybackup:
  /* Do appropriate processing given the current state.  Read a
     lookahead token if we need one and don't already have one.  */

  /* First try to decide what to do without reference to lookahead token.  */
  yyn = yypact[yystate];
  if (yypact_value_is_default (yyn))
    goto yydefault;

  /* Not known => get a lookahead token if don't already have one.  */

  /* YYCHAR is either empty, or end-of-input, or a valid lookahead.  */
  if (yychar == YYEMPTY)
    {
      YYDPRINTF ((stderr, "Reading a token\n"));
      yychar = yylex (&yylval, scanner);
    }

  if (yychar <= YYEOF)
    {
      yychar = YYEOF;
      yytoken = YYSYMBOL_YYEOF;
      YYDPRINTF ((stderr, "Now at end of input.\n"));
    }
  else if (yychar == YYerror)
    {
      /* The scanner already issued an error message, process directly
         to error recovery.  But do not keep the error token as
         lookahead, it is too special and may lead us to an endless
         loop in error recovery. */
      yychar = YYUNDEF;
      yytoken = YYSYMBOL_YYerror;
      goto yyerrlab1;
    }
  else
    {
      yytoken = YYTRANSLATE (yychar);
      YY_SYMBOL_PRINT ("Next token is", yytoken, &yylval, &yylloc);
    }

  /* If the proper action on seeing token YYTOKEN is to reduce or to
     detect an error, take that action.  */
  yyn += yytoken;
  if (yyn < 0 || YYLAST < yyn || yycheck[yyn] != yytoken)
    goto yydefault;
  yyn = yytable[yyn];
  if (yyn <= 0)
    {
      if (yytable_value_is_error (yyn))
        goto yyerrlab;
      yyn = -yyn;
      goto yyreduce;
    }

  /* Count tokens shifted since error; after three, turn off error
     status.  */
  if (yyerrstatus)
    yyerrstatus--;

  /* Shift the lookahead token.  */
  YY_SYMBOL_PRINT ("Shifting", yytoken, &yylval, &yylloc);
  yystate = yyn;
  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END

  /* Discard the shifted token.  */
  yychar = YYEMPTY;
  goto yynewstate;


/*-----------------------------------------------------------.
| yydefault -- do the default action for the current state.  |
`-----------------------------------------------------------*/
yydefault:
  yyn = yydefact[yystate];
  if (yyn == 0)
    goto yyerrlab;
  goto yyreduce;


/*-----------------------------.
| yyreduce -- do a reduction.  |
`-----------------------------*/
yyreduce:
  /* yyn is the number of a rule to reduce with.  */
  yylen = yyr2[yyn];

  /* If YYLEN is nonzero, implement the default value of the action:
     '$$ = $1'.

     Otherwise, the following line sets YYVAL to garbage.
     This behavior is undocumented and Bison
     users should not rely upon it.  Assigning to YYVAL
     unconditionally makes the parser a bit smaller, and it avoids a
     GCC warning that YYVAL may be used uninitialized.  */
  yyval = yyvsp[1-yylen];


  YY_REDUCE_PRINT (yyn);
  switch (yyn)
    {
  case 3: /* input: expr  */
#line 160 "lib/route/cls/ematch_syntax.y"
                {
			nl_list_add_tail(root, &(yyvsp[0].e)->e_list);
		}
#line 1617 "lib/route/cls/ematch_syntax.c"
    break;

  case 4: /* expr: match  */
#line 167 "lib/route/cls/ematch_syntax.y"
                {
			(yyval.e) = (yyvsp[0].e);
		}
#line 1625 "lib/route/cls/ematch_syntax.c"
    break;

  case 5: /* expr: match LOGIC expr  */
#line 171 "lib/route/cls/ematch_syntax.y"
                {
			rtnl_ematch_set_flags((yyvsp[-2].e), (yyvsp[-1].i));

			/* make ematch new head */
			nl_list_add_tail(&(yyvsp[-2].e)->e_list, &(yyvsp[0].e)->e_list);

			(yyval.e) = (yyvsp[-2].e);
		}
#line 1638 "lib/route/cls/ematch_syntax.c"
    break;

  case 6: /* match: NOT ematch  */
#line 183 "lib/route/cls/ematch_syntax.y"
                {
			rtnl_ematch_set_flags((yyvsp[0].e), TCF_EM_INVERT);
			(yyval.e) = (yyvsp[0].e);
		}
#line 1647 "lib/route/cls/ematch_syntax.c"
    break;

  case 7: /* match: ematch  */
#line 188 "lib/route/cls/ematch_syntax.y"
                {
			(yyval.e) = (yyvsp[0].e);
		}
#line 1655 "lib/route/cls/ematch_syntax.c"
    break;

  case 8: /* ematch: cmp_match  */
#line 196 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_ematch *e;

			if (!(e = rtnl_ematch_alloc())) {
				*errp = strdup("Unable to allocate ematch object");
				YYABORT;
			}

			if (rtnl_ematch_set_kind(e, TCF_EM_CMP) < 0)
				BUG();

			rtnl_ematch_cmp_set(e, &(yyvsp[0].cmp));
			(yyval.e) = e;
		}
#line 1674 "lib/route/cls/ematch_syntax.c"
    break;

  case 9: /* ematch: "pattern" "(" pktloc "=" pattern ")"  */
#line 211 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_ematch *e;

			if (!(e = rtnl_ematch_alloc())) {
				*errp = strdup("Unable to allocate ematch object");
				YYABORT;
			}

			if (rtnl_ematch_set_kind(e, TCF_EM_NBYTE) < 0)
				BUG();

			rtnl_ematch_nbyte_set_offset(e, (yyvsp[-3].loc)->layer, (yyvsp[-3].loc)->offset);
			rtnl_pktloc_put((yyvsp[-3].loc));
			rtnl_ematch_nbyte_set_pattern(e, (uint8_t *) (yyvsp[-1].q).data, (yyvsp[-1].q).index);

			(yyval.e) = e;
		}
#line 1696 "lib/route/cls/ematch_syntax.c"
    break;

  case 10: /* ematch: "text" "(" STR QUOTED text_from text_to ")"  */
#line 229 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_ematch *e;

			if (!(e = rtnl_ematch_alloc())) {
				*errp = strdup("Unable to allocate ematch object");
				YYABORT;
			}

			if (rtnl_ematch_set_kind(e, TCF_EM_TEXT) < 0)
				BUG();

			rtnl_ematch_text_set_algo(e, (yyvsp[-4].s));
			rtnl_ematch_text_set_pattern(e, (yyvsp[-3].q).data, (yyvsp[-3].q).index);

			if ((yyvsp[-2].loc)) {
				rtnl_ematch_text_set_from(e, (yyvsp[-2].loc)->layer, (yyvsp[-2].loc)->offset);
				rtnl_pktloc_put((yyvsp[-2].loc));
			}

			if ((yyvsp[-1].loc)) {
				rtnl_ematch_text_set_to(e, (yyvsp[-1].loc)->layer, (yyvsp[-1].loc)->offset);
				rtnl_pktloc_put((yyvsp[-1].loc));
			}

			(yyval.e) = e;
		}
#line 1727 "lib/route/cls/ematch_syntax.c"
    break;

  case 11: /* ematch: "meta" "(" meta_value operand meta_value ")"  */
#line 256 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_ematch *e;

			if (!(e = rtnl_ematch_alloc())) {
				*errp = strdup("Unable to allocate ematch object");
				YYABORT;
			}

			if (rtnl_ematch_set_kind(e, TCF_EM_META) < 0)
				BUG();

			rtnl_ematch_meta_set_lvalue(e, (yyvsp[-3].mv));
			rtnl_ematch_meta_set_rvalue(e, (yyvsp[-1].mv));
			rtnl_ematch_meta_set_operand(e, (yyvsp[-2].i));

			(yyval.e) = e;
		}
#line 1749 "lib/route/cls/ematch_syntax.c"
    break;

  case 12: /* ematch: "(" expr ")"  */
#line 275 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_ematch *e;

			if (!(e = rtnl_ematch_alloc())) {
				*errp = strdup("Unable to allocate ematch object");
				YYABORT;
			}

			if (rtnl_ematch_set_kind(e, TCF_EM_CONTAINER) < 0)
				BUG();

			/* Make e->childs the list head of a the ematch sequence */
			nl_list_add_tail(&e->e_childs, &(yyvsp[-1].e)->e_list);

			(yyval.e) = e;
		}
#line 1770 "lib/route/cls/ematch_syntax.c"
    break;

  case 13: /* cmp_match: "cmp" "(" cmp_expr ")"  */
#line 303 "lib/route/cls/ematch_syntax.y"
                { (yyval.cmp) = (yyvsp[-1].cmp); }
#line 1776 "lib/route/cls/ematch_syntax.c"
    break;

  case 14: /* cmp_match: cmp_expr  */
#line 305 "lib/route/cls/ematch_syntax.y"
                { (yyval.cmp) = (yyvsp[0].cmp); }
#line 1782 "lib/route/cls/ematch_syntax.c"
    break;

  case 15: /* cmp_expr: pktloc operand NUMBER  */
#line 310 "lib/route/cls/ematch_syntax.y"
                {
			if ((yyvsp[-2].loc)->align == TCF_EM_ALIGN_U16 ||
			    (yyvsp[-2].loc)->align == TCF_EM_ALIGN_U32)
				(yyval.cmp).flags = TCF_EM_CMP_TRANS;

			memset(&(yyval.cmp), 0, sizeof((yyval.cmp)));

			(yyval.cmp).mask = (yyvsp[-2].loc)->mask;
			(yyval.cmp).off = (yyvsp[-2].loc)->offset;
			(yyval.cmp).align = (yyvsp[-2].loc)->align;
			(yyval.cmp).layer = (yyvsp[-2].loc)->layer;
			(yyval.cmp).opnd = (yyvsp[-1].i);
			(yyval.cmp).val = (yyvsp[0].i);

			rtnl_pktloc_put((yyvsp[-2].loc));
		}
#line 1803 "lib/route/cls/ematch_syntax.c"
    break;

  case 16: /* text_from: %empty  */
#line 330 "lib/route/cls/ematch_syntax.y"
                { (yyval.loc) = NULL; }
#line 1809 "lib/route/cls/ematch_syntax.c"
    break;

  case 17: /* text_from: "from" pktloc  */
#line 332 "lib/route/cls/ematch_syntax.y"
                { (yyval.loc) = (yyvsp[0].loc); }
#line 1815 "lib/route/cls/ematch_syntax.c"
    break;

  case 18: /* text_to: %empty  */
#line 337 "lib/route/cls/ematch_syntax.y"
                { (yyval.loc) = NULL; }
#line 1821 "lib/route/cls/ematch_syntax.c"
    break;

  case 19: /* text_to: "to" pktloc  */
#line 339 "lib/route/cls/ematch_syntax.y"
                { (yyval.loc) = (yyvsp[0].loc); }
#line 1827 "lib/route/cls/ematch_syntax.c"
    break;

  case 20: /* meta_value: QUOTED  */
#line 344 "lib/route/cls/ematch_syntax.y"
                { (yyval.mv) = rtnl_meta_value_alloc_var((yyvsp[0].q).data, (yyvsp[0].q).len); }
#line 1833 "lib/route/cls/ematch_syntax.c"
    break;

  case 21: /* meta_value: NUMBER  */
#line 346 "lib/route/cls/ematch_syntax.y"
                { (yyval.mv) = rtnl_meta_value_alloc_int((yyvsp[0].i)); }
#line 1839 "lib/route/cls/ematch_syntax.c"
    break;

  case 22: /* meta_value: meta_int_id shift mask  */
#line 348 "lib/route/cls/ematch_syntax.y"
                { (yyval.mv) = META_ALLOC(META_INT, (yyvsp[-2].i), (yyvsp[-1].i), (yyvsp[0].i64)); }
#line 1845 "lib/route/cls/ematch_syntax.c"
    break;

  case 23: /* meta_value: meta_var_id shift  */
#line 350 "lib/route/cls/ematch_syntax.y"
                { (yyval.mv) = META_ALLOC(META_VAR, (yyvsp[-1].i), (yyvsp[0].i), 0); }
#line 1851 "lib/route/cls/ematch_syntax.c"
    break;

  case 24: /* meta_int_id: "random"  */
#line 354 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(RANDOM); }
#line 1857 "lib/route/cls/ematch_syntax.c"
    break;

  case 25: /* meta_int_id: "loadavg_0"  */
#line 355 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(LOADAVG_0); }
#line 1863 "lib/route/cls/ematch_syntax.c"
    break;

  case 26: /* meta_int_id: "loadavg_1"  */
#line 356 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(LOADAVG_1); }
#line 1869 "lib/route/cls/ematch_syntax.c"
    break;

  case 27: /* meta_int_id: "loadavg_2"  */
#line 357 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(LOADAVG_2); }
#line 1875 "lib/route/cls/ematch_syntax.c"
    break;

  case 28: /* meta_int_id: "dev"  */
#line 358 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(DEV); }
#line 1881 "lib/route/cls/ematch_syntax.c"
    break;

  case 29: /* meta_int_id: "prio"  */
#line 359 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(PRIORITY); }
#line 1887 "lib/route/cls/ematch_syntax.c"
    break;

  case 30: /* meta_int_id: "proto"  */
#line 360 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(PROTOCOL); }
#line 1893 "lib/route/cls/ematch_syntax.c"
    break;

  case 31: /* meta_int_id: "pkttype"  */
#line 361 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(PKTTYPE); }
#line 1899 "lib/route/cls/ematch_syntax.c"
    break;

  case 32: /* meta_int_id: "pktlen"  */
#line 362 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(PKTLEN); }
#line 1905 "lib/route/cls/ematch_syntax.c"
    break;

  case 33: /* meta_int_id: "datalen"  */
#line 363 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(DATALEN); }
#line 1911 "lib/route/cls/ematch_syntax.c"
    break;

  case 34: /* meta_int_id: "maclen"  */
#line 364 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(MACLEN); }
#line 1917 "lib/route/cls/ematch_syntax.c"
    break;

  case 35: /* meta_int_id: "mark"  */
#line 365 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(NFMARK); }
#line 1923 "lib/route/cls/ematch_syntax.c"
    break;

  case 36: /* meta_int_id: "tcindex"  */
#line 366 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(TCINDEX); }
#line 1929 "lib/route/cls/ematch_syntax.c"
    break;

  case 37: /* meta_int_id: "rtclassid"  */
#line 367 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(RTCLASSID); }
#line 1935 "lib/route/cls/ematch_syntax.c"
    break;

  case 38: /* meta_int_id: "rtiif"  */
#line 368 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(RTIIF); }
#line 1941 "lib/route/cls/ematch_syntax.c"
    break;

  case 39: /* meta_int_id: "sk_family"  */
#line 369 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_FAMILY); }
#line 1947 "lib/route/cls/ematch_syntax.c"
    break;

  case 40: /* meta_int_id: "sk_state"  */
#line 370 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_STATE); }
#line 1953 "lib/route/cls/ematch_syntax.c"
    break;

  case 41: /* meta_int_id: "sk_reuse"  */
#line 371 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_REUSE); }
#line 1959 "lib/route/cls/ematch_syntax.c"
    break;

  case 42: /* meta_int_id: "sk_refcnt"  */
#line 372 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_REFCNT); }
#line 1965 "lib/route/cls/ematch_syntax.c"
    break;

  case 43: /* meta_int_id: "sk_rcvbuf"  */
#line 373 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_RCVBUF); }
#line 1971 "lib/route/cls/ematch_syntax.c"
    break;

  case 44: /* meta_int_id: "sk_sndbuf"  */
#line 374 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_SNDBUF); }
#line 1977 "lib/route/cls/ematch_syntax.c"
    break;

  case 45: /* meta_int_id: "sk_shutdown"  */
#line 375 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_SHUTDOWN); }
#line 1983 "lib/route/cls/ematch_syntax.c"
    break;

  case 46: /* meta_int_id: "sk_proto"  */
#line 376 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_PROTO); }
#line 1989 "lib/route/cls/ematch_syntax.c"
    break;

  case 47: /* meta_int_id: "sk_type"  */
#line 377 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_TYPE); }
#line 1995 "lib/route/cls/ematch_syntax.c"
    break;

  case 48: /* meta_int_id: "sk_rmem_alloc"  */
#line 378 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_RMEM_ALLOC); }
#line 2001 "lib/route/cls/ematch_syntax.c"
    break;

  case 49: /* meta_int_id: "sk_wmem_alloc"  */
#line 379 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_WMEM_ALLOC); }
#line 2007 "lib/route/cls/ematch_syntax.c"
    break;

  case 50: /* meta_int_id: "sk_wmem_queued"  */
#line 380 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_WMEM_QUEUED); }
#line 2013 "lib/route/cls/ematch_syntax.c"
    break;

  case 51: /* meta_int_id: "sk_rcv_qlen"  */
#line 381 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_RCV_QLEN); }
#line 2019 "lib/route/cls/ematch_syntax.c"
    break;

  case 52: /* meta_int_id: "sk_snd_qlen"  */
#line 382 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_SND_QLEN); }
#line 2025 "lib/route/cls/ematch_syntax.c"
    break;

  case 53: /* meta_int_id: "sk_err_qlen"  */
#line 383 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_ERR_QLEN); }
#line 2031 "lib/route/cls/ematch_syntax.c"
    break;

  case 54: /* meta_int_id: "sk_forward_allocs"  */
#line 384 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_FORWARD_ALLOCS); }
#line 2037 "lib/route/cls/ematch_syntax.c"
    break;

  case 55: /* meta_int_id: "sk_allocs"  */
#line 385 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_ALLOCS); }
#line 2043 "lib/route/cls/ematch_syntax.c"
    break;

  case 56: /* meta_int_id: "sk_route_caps"  */
#line 386 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = __TCF_META_ID_SK_ROUTE_CAPS; }
#line 2049 "lib/route/cls/ematch_syntax.c"
    break;

  case 57: /* meta_int_id: "sk_hash"  */
#line 387 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_HASH); }
#line 2055 "lib/route/cls/ematch_syntax.c"
    break;

  case 58: /* meta_int_id: "sk_lingertime"  */
#line 388 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_LINGERTIME); }
#line 2061 "lib/route/cls/ematch_syntax.c"
    break;

  case 59: /* meta_int_id: "sk_ack_backlog"  */
#line 389 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_ACK_BACKLOG); }
#line 2067 "lib/route/cls/ematch_syntax.c"
    break;

  case 60: /* meta_int_id: "sk_max_ack_backlog"  */
#line 390 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_MAX_ACK_BACKLOG); }
#line 2073 "lib/route/cls/ematch_syntax.c"
    break;

  case 61: /* meta_int_id: "sk_prio"  */
#line 391 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_PRIO); }
#line 2079 "lib/route/cls/ematch_syntax.c"
    break;

  case 62: /* meta_int_id: "sk_rcvlowat"  */
#line 392 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_RCVLOWAT); }
#line 2085 "lib/route/cls/ematch_syntax.c"
    break;

  case 63: /* meta_int_id: "sk_rcvtimeo"  */
#line 393 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_RCVTIMEO); }
#line 2091 "lib/route/cls/ematch_syntax.c"
    break;

  case 64: /* meta_int_id: "sk_sndtimeo"  */
#line 394 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_SNDTIMEO); }
#line 2097 "lib/route/cls/ematch_syntax.c"
    break;

  case 65: /* meta_int_id: "sk_sendmsg_off"  */
#line 395 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_SENDMSG_OFF); }
#line 2103 "lib/route/cls/ematch_syntax.c"
    break;

  case 66: /* meta_int_id: "sk_write_pending"  */
#line 396 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(SK_WRITE_PENDING); }
#line 2109 "lib/route/cls/ematch_syntax.c"
    break;

  case 67: /* meta_int_id: "vlan"  */
#line 397 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(VLAN_TAG); }
#line 2115 "lib/route/cls/ematch_syntax.c"
    break;

  case 68: /* meta_int_id: "rxhash"  */
#line 398 "lib/route/cls/ematch_syntax.y"
                                        { (yyval.i) = META_ID(RXHASH); }
#line 2121 "lib/route/cls/ematch_syntax.c"
    break;

  case 69: /* meta_var_id: "devname"  */
#line 402 "lib/route/cls/ematch_syntax.y"
                                { (yyval.i) = META_ID(DEV); }
#line 2127 "lib/route/cls/ematch_syntax.c"
    break;

  case 70: /* meta_var_id: "sk_bound_if"  */
#line 403 "lib/route/cls/ematch_syntax.y"
                                { (yyval.i) = META_ID(SK_BOUND_IF); }
#line 2133 "lib/route/cls/ematch_syntax.c"
    break;

  case 71: /* pattern: QUOTED  */
#line 411 "lib/route/cls/ematch_syntax.y"
                {
			(yyval.q) = (yyvsp[0].q);
		}
#line 2141 "lib/route/cls/ematch_syntax.c"
    break;

  case 72: /* pattern: STR  */
#line 415 "lib/route/cls/ematch_syntax.y"
                {
			struct nl_addr *addr;

			if (nl_addr_parse((yyvsp[0].s), AF_UNSPEC, &addr) == 0) {
				(yyval.q).len = nl_addr_get_len(addr);

				(yyval.q).index = _NL_MIN((yyval.q).len, nl_addr_get_prefixlen(addr)/8);

				if (!((yyval.q).data = calloc(1, (yyval.q).len))) {
					nl_addr_put(addr);
					YYABORT;
				}

				memcpy((yyval.q).data, nl_addr_get_binary_addr(addr), (yyval.q).len);
				nl_addr_put(addr);
			} else {
				if (asprintf(errp, "invalid pattern \"%s\"", (yyvsp[0].s)) == -1)
					*errp = NULL;
				YYABORT;
			}
		}
#line 2167 "lib/route/cls/ematch_syntax.c"
    break;

  case 73: /* pktloc: STR  */
#line 444 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_pktloc *loc;

			if (rtnl_pktloc_lookup((yyvsp[0].s), &loc) < 0) {
				if (asprintf(errp, "Packet location \"%s\" not found", (yyvsp[0].s)) == -1)
					*errp = NULL;
				YYABORT;
			}

			(yyval.loc) = loc;
		}
#line 2183 "lib/route/cls/ematch_syntax.c"
    break;

  case 74: /* pktloc: align LAYER "+" NUMBER mask  */
#line 457 "lib/route/cls/ematch_syntax.y"
                {
			struct rtnl_pktloc *loc;

			if ((yyvsp[0].i64) && (!(yyvsp[-4].i) || (yyvsp[-4].i) > TCF_EM_ALIGN_U32)) {
				*errp = strdup("mask only allowed for alignments u8|u16|u32");
				YYABORT;
			}

			if (!(loc = rtnl_pktloc_alloc())) {
				*errp = strdup("Unable to allocate packet location object");
				YYABORT;
			}

			loc->name = strdup("<USER-DEFINED>");
			loc->align = (yyvsp[-4].i);
			loc->layer = (yyvsp[-3].i);
			loc->offset = (yyvsp[-1].i);
			loc->mask = (yyvsp[0].i64);

			(yyval.loc) = loc;
		}
#line 2209 "lib/route/cls/ematch_syntax.c"
    break;

  case 75: /* align: %empty  */
#line 482 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = 0; }
#line 2215 "lib/route/cls/ematch_syntax.c"
    break;

  case 76: /* align: ALIGN "at"  */
#line 484 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = (yyvsp[-1].i); }
#line 2221 "lib/route/cls/ematch_syntax.c"
    break;

  case 77: /* align: NUMBER "at"  */
#line 486 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = (yyvsp[-1].i); }
#line 2227 "lib/route/cls/ematch_syntax.c"
    break;

  case 78: /* mask: %empty  */
#line 491 "lib/route/cls/ematch_syntax.y"
                { (yyval.i64) = 0; }
#line 2233 "lib/route/cls/ematch_syntax.c"
    break;

  case 79: /* mask: "mask" NUMBER  */
#line 493 "lib/route/cls/ematch_syntax.y"
                { (yyval.i64) = (yyvsp[0].i); }
#line 2239 "lib/route/cls/ematch_syntax.c"
    break;

  case 80: /* shift: %empty  */
#line 498 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = 0; }
#line 2245 "lib/route/cls/ematch_syntax.c"
    break;

  case 81: /* shift: ">>" NUMBER  */
#line 500 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = (yyvsp[0].i); }
#line 2251 "lib/route/cls/ematch_syntax.c"
    break;

  case 82: /* operand: "="  */
#line 505 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = TCF_EM_OPND_EQ; }
#line 2257 "lib/route/cls/ematch_syntax.c"
    break;

  case 83: /* operand: ">"  */
#line 507 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = TCF_EM_OPND_GT; }
#line 2263 "lib/route/cls/ematch_syntax.c"
    break;

  case 84: /* operand: "<"  */
#line 509 "lib/route/cls/ematch_syntax.y"
                { (yyval.i) = TCF_EM_OPND_LT; }
#line 2269 "lib/route/cls/ematch_syntax.c"
    break;


#line 2273 "lib/route/cls/ematch_syntax.c"

      default: break;
    }
  /* User semantic actions sometimes alter yychar, and that requires
     that yytoken be updated with the new translation.  We take the
     approach of translating immediately before every use of yytoken.
     One alternative is translating here after every semantic action,
     but that translation would be missed if the semantic action invokes
     YYABORT, YYACCEPT, or YYERROR immediately after altering yychar or
     if it invokes YYBACKUP.  In the case of YYABORT or YYACCEPT, an
     incorrect destructor might then be invoked immediately.  In the
     case of YYERROR or YYBACKUP, subsequent parser actions might lead
     to an incorrect destructor call or verbose syntax error message
     before the lookahead is translated.  */
  YY_SYMBOL_PRINT ("-> $$ =", YY_CAST (yysymbol_kind_t, yyr1[yyn]), &yyval, &yyloc);

  YYPOPSTACK (yylen);
  yylen = 0;

  *++yyvsp = yyval;

  /* Now 'shift' the result of the reduction.  Determine what state
     that goes to, based on the state we popped back to and the rule
     number reduced by.  */
  {
    const int yylhs = yyr1[yyn] - YYNTOKENS;
    const int yyi = yypgoto[yylhs] + *yyssp;
    yystate = (0 <= yyi && yyi <= YYLAST && yycheck[yyi] == *yyssp
               ? yytable[yyi]
               : yydefgoto[yylhs]);
  }

  goto yynewstate;


/*--------------------------------------.
| yyerrlab -- here on detecting error.  |
`--------------------------------------*/
yyerrlab:
  /* Make sure we have latest lookahead translation.  See comments at
     user semantic actions for why this is necessary.  */
  yytoken = yychar == YYEMPTY ? YYSYMBOL_YYEMPTY : YYTRANSLATE (yychar);
  /* If not already recovering from an error, report this error.  */
  if (!yyerrstatus)
    {
      ++yynerrs;
      {
        yypcontext_t yyctx
          = {yyssp, yytoken};
        char const *yymsgp = YY_("syntax error");
        int yysyntax_error_status;
        yysyntax_error_status = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
        if (yysyntax_error_status == 0)
          yymsgp = yymsg;
        else if (yysyntax_error_status == -1)
          {
            if (yymsg != yymsgbuf)
              YYSTACK_FREE (yymsg);
            yymsg = YY_CAST (char *,
                             YYSTACK_ALLOC (YY_CAST (YYSIZE_T, yymsg_alloc)));
            if (yymsg)
              {
                yysyntax_error_status
                  = yysyntax_error (&yymsg_alloc, &yymsg, &yyctx);
                yymsgp = yymsg;
              }
            else
              {
                yymsg = yymsgbuf;
                yymsg_alloc = sizeof yymsgbuf;
                yysyntax_error_status = YYENOMEM;
              }
          }
        yyerror (scanner, errp, root, yymsgp);
        if (yysyntax_error_status == YYENOMEM)
          YYNOMEM;
      }
    }

  if (yyerrstatus == 3)
    {
      /* If just tried and failed to reuse lookahead token after an
         error, discard it.  */

      if (yychar <= YYEOF)
        {
          /* Return failure if at end of input.  */
          if (yychar == YYEOF)
            YYABORT;
        }
      else
        {
          yydestruct ("Error: discarding",
                      yytoken, &yylval, scanner, errp, root);
          yychar = YYEMPTY;
        }
    }

  /* Else will try to reuse lookahead token after shifting the error
     token.  */
  goto yyerrlab1;


/*---------------------------------------------------.
| yyerrorlab -- error raised explicitly by YYERROR.  |
`---------------------------------------------------*/
yyerrorlab:
  /* Pacify compilers when the user code never invokes YYERROR and the
     label yyerrorlab therefore never appears in user code.  */
  if (0)
    YYERROR;
  ++yynerrs;

  /* Do not reclaim the symbols of the rule whose action triggered
     this YYERROR.  */
  YYPOPSTACK (yylen);
  yylen = 0;
  YY_STACK_PRINT (yyss, yyssp);
  yystate = *yyssp;
  goto yyerrlab1;


/*-------------------------------------------------------------.
| yyerrlab1 -- common code for both syntax error and YYERROR.  |
`-------------------------------------------------------------*/
yyerrlab1:
  yyerrstatus = 3;      /* Each real token shifted decrements this.  */

  /* Pop stack until we find a state that shifts the error token.  */
  for (;;)
    {
      yyn = yypact[yystate];
      if (!yypact_value_is_default (yyn))
        {
          yyn += YYSYMBOL_YYerror;
          if (0 <= yyn && yyn <= YYLAST && yycheck[yyn] == YYSYMBOL_YYerror)
            {
              yyn = yytable[yyn];
              if (0 < yyn)
                break;
            }
        }

      /* Pop the current state because it cannot handle the error token.  */
      if (yyssp == yyss)
        YYABORT;


      yydestruct ("Error: popping",
                  YY_ACCESSING_SYMBOL (yystate), yyvsp, scanner, errp, root);
      YYPOPSTACK (1);
      yystate = *yyssp;
      YY_STACK_PRINT (yyss, yyssp);
    }

  YY_IGNORE_MAYBE_UNINITIALIZED_BEGIN
  *++yyvsp = yylval;
  YY_IGNORE_MAYBE_UNINITIALIZED_END


  /* Shift the error token.  */
  YY_SYMBOL_PRINT ("Shifting", YY_ACCESSING_SYMBOL (yyn), yyvsp, yylsp);

  yystate = yyn;
  goto yynewstate;


/*-------------------------------------.
| yyacceptlab -- YYACCEPT comes here.  |
`-------------------------------------*/
yyacceptlab:
  yyresult = 0;
  goto yyreturnlab;


/*-----------------------------------.
| yyabortlab -- YYABORT comes here.  |
`-----------------------------------*/
yyabortlab:
  yyresult = 1;
  goto yyreturnlab;


/*-----------------------------------------------------------.
| yyexhaustedlab -- YYNOMEM (memory exhaustion) comes here.  |
`-----------------------------------------------------------*/
yyexhaustedlab:
  yyerror (scanner, errp, root, YY_("memory exhausted"));
  yyresult = 2;
  goto yyreturnlab;


/*----------------------------------------------------------.
| yyreturnlab -- parsing is finished, clean up and return.  |
`----------------------------------------------------------*/
yyreturnlab:
  if (yychar != YYEMPTY)
    {
      /* Make sure we have latest lookahead translation.  See comments at
         user semantic actions for why this is necessary.  */
      yytoken = YYTRANSLATE (yychar);
      yydestruct ("Cleanup: discarding lookahead",
                  yytoken, &yylval, scanner, errp, root);
    }
  /* Do not reclaim the symbols of the rule whose action triggered
     this YYABORT or YYACCEPT.  */
  YYPOPSTACK (yylen);
  YY_STACK_PRINT (yyss, yyssp);
  while (yyssp != yyss)
    {
      yydestruct ("Cleanup: popping",
                  YY_ACCESSING_SYMBOL (+*yyssp), yyvsp, scanner, errp, root);
      YYPOPSTACK (1);
    }
#ifndef yyoverflow
  if (yyss != yyssa)
    YYSTACK_FREE (yyss);
#endif
  if (yymsg != yymsgbuf)
    YYSTACK_FREE (yymsg);
  return yyresult;
}
