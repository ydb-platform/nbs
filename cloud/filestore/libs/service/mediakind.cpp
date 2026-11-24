#include "mediakind.h"

#include <util/string/builder.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<NCloud::NProto::EStorageMediaKind> ParseFileStoreMediaKind(
    TStringBuf s)
{
    if (s == "ssd") {
        return NCloud::NProto::STORAGE_MEDIA_SSD;
    }

    if (s == "hdd" || s == "hybrid" || s == "default") {
        return NCloud::NProto::STORAGE_MEDIA_HDD;
    }

    return MakeError(
        E_ARGUMENT,
        TStringBuilder()
            << "invalid storage media kind: " << s
            << ", should be one of 'ssd', 'hdd', 'hybrid', 'default'");
}

////////////////////////////////////////////////////////////////////////////////

TString FastShardStorageKindToString(NProto::EFastShardStorageKind kind)
{
    switch (kind) {
        case NProto::FAST_SHARD_STORAGE_MIRROR:
            return "mirror";
        case NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR:
            return "quorum_mirror";
        default:
            return "unknown";
    }
}

TResultOrError<NProto::EFastShardStorageKind> ParseFastShardStorageKind(
    TStringBuf s)
{
    if (s == "mirror") {
        return NProto::FAST_SHARD_STORAGE_MIRROR;
    }

    if (s == "quorum_mirror") {
        return NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR;
    }

    return MakeError(
        E_ARGUMENT,
        TStringBuilder()
            << "invalid fast shard storage kind: " << s
            << ", should be one of 'mirror', 'quorum_mirror'");
}

}   // namespace NCloud::NFileStore
