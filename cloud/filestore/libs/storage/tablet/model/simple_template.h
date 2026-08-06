#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

void OutputTemplate(
    const TString& templateData,
    const THashMap<TString, TString>& vars,
    IOutputStream& out);

}   // namespace NCloud::NFileStore
