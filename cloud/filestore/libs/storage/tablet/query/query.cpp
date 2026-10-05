#include "query.h"
#include "parser.h"

#include <mutex>

struct yy_buffer_state;

extern yy_buffer_state* yy_scan_bytes(const char*, int);
extern void yy_delete_buffer(yy_buffer_state*);

namespace NCloud::NFileStore::NStorage::NQuery {

////////////////////////////////////////////////////////////////////////////////

struct TParseContext
{
    TSelect Query;
    TString Error;
};

TParseContext CurrentParseContext;

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error)
{
    static std::mutex Mutex;
    const std::lock_guard guard(Mutex);

    CurrentParseContext.Query = TSelect{};
    CurrentParseContext.Error.clear();

    auto* buffer = yy_scan_bytes(input.data(), input.size());
    const int result = yyparse();
    yy_delete_buffer(buffer);

    if (result != 0) {
        if (error) {
            error->Message = CurrentParseContext.Error.empty()
                ? "invalid query"
                : CurrentParseContext.Error;
        }
        return Nothing();
    }

    if (error) {
        *error = {};
    }
    return std::move(CurrentParseContext.Query);
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
