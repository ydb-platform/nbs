#pragma once

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TServiceKikimrConfig
{
    TString WriteBackCacheFilePath;
};

}   // namespace NCloud::NFileStore
