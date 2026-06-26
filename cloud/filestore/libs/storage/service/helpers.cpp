#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void Convert(
    const NKikimrFileStore::TConfig& config,
    NProto::TFileStore& fileStore)
{
    fileStore.SetFileSystemId(config.GetFileSystemId());
    fileStore.SetProjectId(config.GetProjectId());
    fileStore.SetFolderId(config.GetFolderId());
    fileStore.SetCloudId(config.GetCloudId());
    fileStore.SetBlockSize(config.GetBlockSize());
    fileStore.SetBlocksCount(config.GetBlocksCount());
    fileStore.SetNodesCount(config.GetNodesCount());
    fileStore.SetStorageMediaKind(config.GetStorageMediaKind());
    fileStore.SetConfigVersion(config.GetVersion());
}

////////////////////////////////////////////////////////////////////////////////

TInplaceBitmap::TInplaceBitmap(const TString& data)
    : Data(data)
{}

bool TInplaceBitmap::Get(ui32 shardNo) const
{
    const size_t byte = shardNo >> 3;
    const ui8 bit = shardNo & 7;

    return byte < Data.size() &&
           (static_cast<ui8>(Data[byte]) & (ui8(1) << bit));
}

////////////////////////////////////////////////////////////////////////////////

TMutableInplaceBitmap::TMutableInplaceBitmap(TString& data)
    : Data(data)
{}

bool TMutableInplaceBitmap::Get(ui32 index) const
{
    return TInplaceBitmap(Data).Get(index);
}

void TMutableInplaceBitmap::Set(ui32 index)
{
    const size_t byte = index >> 3;
    const ui8 bit = index & 7;

    if (Data.size() <= byte) {
        Data.resize(byte + 1, 0);
    }

    Data[byte] = static_cast<char>(
        static_cast<ui8>(Data[byte]) | (static_cast<ui8>(1) << bit));
}

void TMutableInplaceBitmap::Clear()
{
    Data.clear();
}

}   // namespace NCloud::NFileStore::NStorage
