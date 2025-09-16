#include "volume_label.h"

#include <library/cpp/string_utils/quote/quote.h>

#include <util/digest/murmur.h>
#include <util/string/cast.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf SecondaryDiskSuffix = "-copy";

////////////////////////////////////////////////////////////////////////////////

TString ComputeFolder(const TString& diskId)
{
    constexpr ui32 folders = 1 << 10;

    const auto hash = MurmurHash<ui32>(diskId.data(), diskId.size());

    return "_" + IntToString<16>(hash % folders);
}

std::tuple<TString, TString> ExtractParentDirAndName(
    const TString& rootDir,
    const TString& volumePath)
{
    TString parentDir = rootDir;
    TString volumeName = volumePath;

    {
        TStringBuf l;
        TStringBuf r;

        TStringBuf(volumeName).RSplit('/', l, r);

        parentDir += "/";
        parentDir += l;

        {
            char* begin = parentDir.begin();
            char* end = parentDir.vend();

            parentDir.erase(
                Unique(
                    begin,
                    end,
                    [](auto l, auto r) { return (l == r) && (l == '/'); }
                ),
                end);
        }

        if (parentDir && parentDir.back() == '/') {
            parentDir.pop_back();
        }

        volumeName = r;
    }

    return std::tie(parentDir, volumeName);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString DiskIdToPathDeprecated(const TString& diskId)
{
    TString path(diskId);
    ::CGIEscape(path);
    return path;
}

TString DiskIdToPath(const TString& diskId)
{
    TString path(diskId);
    const char safe[] = "";

    ::Quote(path, safe);

    if (path) {
        path = ComputeFolder(diskId) + "/" + path;
    }

    return path;
}

TString PathNameToDiskId(const TString& pathName)
{
    TString diskId(pathName);
    ::CGIUnescape(diskId);
    return diskId;
}

std::tuple<TString, TString> DiskIdToVolumeDirAndNameDeprecated(
    const TString& rootDir,
    const TString& diskId)
{
    TString path = DiskIdToPathDeprecated(diskId);
    return ExtractParentDirAndName(rootDir, path);
}

std::tuple<TString, TString> DiskIdToVolumeDirAndName(
    const TString& rootDir,
    const TString& diskId)
{
    TString path = DiskIdToPath(diskId);
    return ExtractParentDirAndName(rootDir, path);
}

TString GetSecondaryDiskId(const TString& diskId)
{
    return diskId + SecondaryDiskSuffix;
}

TString GetLogicalDiskId(const TString& diskId)
{
    if (diskId.EndsWith(SecondaryDiskSuffix)) {
        return diskId.substr(0, diskId.size() - SecondaryDiskSuffix.size());
    }
    return diskId;
}

bool IsSecondaryDiskId(const TString& diskId)
{
    return diskId.EndsWith(SecondaryDiskSuffix);
}

}   // namespace NCloud::NBlockStore::NStorage
