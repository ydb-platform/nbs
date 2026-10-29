#pragma once

#include "parser.h"

#include <contrib/tools/flex-old/FlexLexer.h>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

class TQueryLexer: public yyFlexLexer
{
public:
    using yyFlexLexer::yyFlexLexer;
    using yyFlexLexer::yylex;

    int yylex(
        YYSTYPE* value,
        NCloud::NFileStore::NStorage::NQuery::TParseContext& context);
};

}   // namespace NCloud::NFileStore::NStorage::NQuery
