#include "query.h"

#include "lexer.h"
#include "parser.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TSelect> Parse(TStringBuf input)
{
    const std::string inputText(input.data(), input.size());
    std::istringstream inputStream(inputText);
    TParseContext context;
    TQueryLexer lexer(&inputStream);
    const int result = yyparse(context, lexer);

    if (result != 0) {
        if (context.Error.Message.empty()) {
            context.Error.Message = "invalid query";
        }
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << context.Error.Message << " at byte "
                             << context.Error.Offset);
    }

    return std::move(context.Query);
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
