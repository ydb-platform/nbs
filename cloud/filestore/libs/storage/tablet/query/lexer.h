#pragma once

#if !defined(yyFlexLexerOnce)
#include <contrib/tools/flex-old/FlexLexer.h>
#endif

#include "parser.h"

#include <istream>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

class TQueryLexer : public yyFlexLexer
{
public:
    using yyFlexLexer::yylex;

    explicit TQueryLexer(std::istream& input)
        : yyFlexLexer(&input)
    {}

    int yylex(
        YYSTYPE* value,
        NCloud::NFileStore::NStorage::NQuery::TParseContext& context);
};

}   // namespace NCloud::NFileStore::NStorage::NQuery
