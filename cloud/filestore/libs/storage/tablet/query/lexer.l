%{
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif

#include "parser.h"

#include <util/generic/string.h>
#include <util/string/cast.h>
#include <util/string/escape.h>

#include <cctype>

#define YY_DECL int yylex( \
    YYSTYPE* value, \
    NCloud::NFileStore::NStorage::NQuery::TParseContext& context)

#define YY_USER_ACTION \
    context.CurrentTokenOffset = context.ScannedInputBytes; \
    context.ScannedInputBytes += yyleng;

////////////////////////////////////////////////////////////////////////////////

static int Keyword(const char* text)
{
    TString value(text);
    for (auto& ch: value) {
        ch = std::tolower(static_cast<unsigned char>(ch));
    }

    if (value == "select") return SELECT;
    if (value == "from") return FROM;
    if (value == "where") return WHERE;
    if (value == "and") return AND;
    if (value == "or") return OR;
    if (value == "in") return IN;
    if (value == "substr") return SUBSTR;
    if (value == "limit") return LIMIT;
    return IDENTIFIER;
}

static TString Unquote(const char* text, size_t length)
{
    return UnescapeC(TStringBuf(text + 1, length - 2));
}
%}

%option noyywrap

%%

    /* Skip whitespace; the byte offsets are still updated. */
[ \t\r\n]+                  ;

    /* Return Bison's end-of-input token, with the offset past the last byte. */
<<EOF>>                     {
    context.CurrentTokenOffset = context.ScannedInputBytes;
    return 0;
}

    /* Comparison operators. Chooses the longest match (e.g. >= over >). */
"=="|"="                    { return EQ; }
"!="                        { return NE; }
">="                        { return GE; }
">"                         { return GT; }
"<="                        { return LE; }
"<"                         { return LT; }

    /* Quoted strings: ordinary characters or a backslash followed by a
     * non-newline character. Unquote removes the surrounding quotes and
     * decodes C-style escape sequences in the string contents. */
'([^\\'\n]|\\.)*'           |
\"([^\\\"\n]|\\.)*\"         {
    value->text = new TString(Unquote(yytext, yyleng));
    return STRING;
}

    /* Unsigned decimal integers; reject values that overflow ui64. */
[0-9]+                      {
    ui64 number = 0;
    if (!TryFromString(TStringBuf(yytext, yyleng), number)) {
        return INVALID;
    }
    value->number = number;
    return NUMBER;
}

    /* Keyword and identifiers; keyword substrings inside names remain identifiers. */
[A-Za-z_][A-Za-z0-9_]*      {
    const int token = Keyword(yytext);
    if (token == IDENTIFIER) {
        value->text = new TString(yytext, yyleng);
    }
    return token;
}

    /* Punctuation uses its character code as the token type; no value needed. */
"*"                         { return '*'; }
"{"                         { return '{'; }
"}"                         { return '}'; }
","                         { return ','; }
"("                         { return '('; }
")"                         { return ')'; }

    /* Reject any remaining character instead of silently ending the query.
     * Dot matches any character except newline, which whitespace handles. */
.                           { return INVALID; }

%%
