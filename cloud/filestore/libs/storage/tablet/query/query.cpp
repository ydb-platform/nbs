#include "query.h"
#include "parser.h"

#include <util/string/builder.h>
#include <util/string/escape.h>

#include <mutex>

struct yy_buffer_state;

extern yy_buffer_state* yy_scan_bytes(const char*, int);
extern void yy_delete_buffer(yy_buffer_state*);

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TSelect> Parse(TStringBuf input)
{
    // The legacy Flex scanner still uses shared internal state.
    static std::mutex Mutex;
    const std::lock_guard guard(Mutex);

    TParseContext context;
    auto* buffer = yy_scan_bytes(input.data(), input.size());
    const int result = yyparse(context);
    yy_delete_buffer(buffer);

    if (result != 0) {
        if (context.Error.Message.empty()) {
            context.Error.Message = "invalid query";
        }
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << context.Error.Message
                             << " at byte " << context.Error.Offset);
    }

    return std::move(context.Query);
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
