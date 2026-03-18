#pragma once

#include <cloud/blockstore/libs/storage/model/channel_permissions.h>

#include <contrib/ydb/core/base/blobstorage.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

EChannelPermissions StorageStatusFlags2ChannelPermissions(
    NKikimr::TStorageStatusFlags ssf);

}   // namespace NCloud::NBlockStore::NStorage
