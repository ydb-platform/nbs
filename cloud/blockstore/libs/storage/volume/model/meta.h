#pragma once

#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeMetaHistoryItem
{
    TInstant Timestamp;
    NProto::TVolumeMeta Meta;
};

}   // namespace NCloud::NBlockStore
