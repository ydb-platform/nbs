#include "monpage_helpers.h"

#include <library/cpp/json/writer/json.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString JsonError(const NProto::TError& e)
{
    TStringStream ss;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &ss);
    writer.BeginObject();
    writer.WriteKey("error");
    writer.WriteString(FormatError(e));
    writer.EndObject();
    return ss.Str();
}

}   // namespace NCloud::NFileStore::NStorage
