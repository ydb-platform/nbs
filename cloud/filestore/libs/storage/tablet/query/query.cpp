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
    TParseError Error;
};

TParseContext CurrentParseContext;
size_t CurrentTokenOffset = 0;
size_t ScannedInputBytes = 0;

TMaybe<TSelect> Parse(TStringBuf input, TParseError* error)
{
    static std::mutex Mutex;
    const std::lock_guard guard(Mutex);

    CurrentParseContext.Query = TSelect{};
    CurrentParseContext.Error = {};
    CurrentTokenOffset = 0;
    ScannedInputBytes = 0;

    auto* buffer = yy_scan_bytes(input.data(), input.size());
    const int result = yyparse();
    yy_delete_buffer(buffer);

    if (result != 0) {
        if (error) {
            *error = CurrentParseContext.Error;
            if (error->Message.empty()) {
                error->Message = "invalid query";
            }
        }
        return Nothing();
    }

    if (error) {
        *error = {};
    }
    return std::move(CurrentParseContext.Query);
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
