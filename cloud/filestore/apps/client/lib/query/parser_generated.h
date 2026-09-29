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

#ifndef YY_YY_PARSER_GENERATED_H_INCLUDED
# define YY_YY_PARSER_GENERATED_H_INCLUDED
/* Debug traces.  */
#ifndef YYDEBUG
# define YYDEBUG 0
#endif
#if YYDEBUG
extern int yydebug;
#endif
/* "%code requires" blocks.  */
#line 52 "parser.y"

#include "parser.h"

#line 53 "parser_generated.h"

/* Token kinds.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
  enum yytokentype
  {
    YYEMPTY = -2,
    YYEOF = 0,                     /* "end of file"  */
    YYerror = 256,                 /* error  */
    YYUNDEF = 257,                 /* "invalid token"  */
    SELECT = 258,                  /* SELECT  */
    FROM = 259,                    /* FROM  */
    WHERE = 260,                   /* WHERE  */
    AND = 261,                     /* AND  */
    OR = 262,                      /* OR  */
    IN = 263,                      /* IN  */
    SUBSTR = 264,                  /* SUBSTR  */
    LIMIT = 265,                   /* LIMIT  */
    INVALID = 266,                 /* INVALID  */
    EQ = 267,                      /* EQ  */
    NE = 268,                      /* NE  */
    IDENTIFIER = 269,              /* IDENTIFIER  */
    STRING = 270,                  /* STRING  */
    NUMBER = 271                   /* NUMBER  */
  };
  typedef enum yytokentype yytoken_kind_t;
#endif

/* Value type.  */
#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
union YYSTYPE
{
#line 61 "parser.y"

    TString* text;
    ui64 number;
    NCloud::NFileStore::NClient::NQuery::TValue* value;
    NCloud::NFileStore::NClient::NQuery::TCondition* condition;
    NCloud::NFileStore::NClient::NQuery::TExpression* expression;
    TVector<TString>* columns;
    TVector<NCloud::NFileStore::NClient::NQuery::TValue>* values;

#line 96 "parser_generated.h"

};
typedef union YYSTYPE YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define YYSTYPE_IS_DECLARED 1
#endif


extern YYSTYPE yylval;


int yyparse (void);

/* "%code provides" blocks.  */
#line 56 "parser.y"

int yylex(void);
void yyerror(const char* message);

#line 116 "parser_generated.h"

#endif /* !YY_YY_PARSER_GENERATED_H_INCLUDED  */
