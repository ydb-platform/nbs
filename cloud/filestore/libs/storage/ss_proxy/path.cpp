#include "public.h"

#include <library/cpp/string_utils/quote/quote.h>

#include <util/digest/murmur.h>
#include <util/string/cast.h>
#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString ComputeFolder(const TString& fileSystemId)
{
    constexpr ui32 folders = 1 << 10;

    const auto hash =
        MurmurHash<ui32>(fileSystemId.data(), fileSystemId.size());
    return "_" + IntToString<16>(hash % folders);
}

TString GetFileSystemPath(const TString& rootPath, const TString& fileSystemId)
{
    static const char safe[] = "";

    TString path(fileSystemId);
    ::Quote(path, safe);

    return Join("/", rootPath, ComputeFolder(fileSystemId), path);
}

}   // namespace NCloud::NFileStore::NStorage
