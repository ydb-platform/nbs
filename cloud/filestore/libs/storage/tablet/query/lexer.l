%{
#define YY_NO_INPUT 1

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunneeded-internal-declaration"
#endif

#include "parser.h"

#include <util/generic/string.h>
#include <util/string/cast.h>

#include <cctype>

////////////////////////////////////////////////////////////////////////////////

static int Keyword(const char* text)
{
    TString value(text);
    for (auto& ch : value) {
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
    TString result;
    for (size_t i = 1; i + 1 < length; ++i) {
        if (text[i] == '\\' && i + 2 < length) {
            ++i;
        }
        result += text[i];
    }
    return result;
}
%}

%option noyywrap
%option noinput

%%

[ \t\r\n]+                  ;

"=="|"="                    { return EQ; }
"!="                        { return NE; }

'([^\\'\n]|\\.)*'           |
\"([^\\\"\n]|\\.)*\"         {
    yylval.text = new TString(Unquote(yytext, yyleng));
    return STRING;
}

[0-9]+                      {
    ui64 value = 0;
    if (!TryFromString(TStringBuf(yytext, yyleng), value)) {
        return INVALID;
    }
    yylval.number = value;
    return NUMBER;
}

[A-Za-z_][A-Za-z0-9_]*      {
    const int token = Keyword(yytext);
    if (token == IDENTIFIER) {
        yylval.text = new TString(yytext, yyleng);
    }
    return token;
}

"*"                         { return '*'; }
"{"                         { return '{'; }
"}"                         { return '}'; }
","                         { return ','; }
"("                         { return '('; }
")"                         { return ')'; }

.                           { return INVALID; }

%%
