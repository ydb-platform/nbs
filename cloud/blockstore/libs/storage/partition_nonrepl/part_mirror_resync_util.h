#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/size_literals.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaDescriptor
{
    TString ReplicaId;
    ui32 ReplicaIndex = 0;
    NActors::TActorId ActorId;
};

////////////////////////////////////////////////////////////////////////////////

// TODO: increase x4?
// Keep the value less than MaxBufferSize in
// cloud/blockstore/libs/rdma/iface/client.h
constexpr ui64 ResyncRangeSize = 4_MB;

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range, ui32 blockSize);
TBlockRange64 RangeId2BlockRange(ui32 rangeId, ui32 blockSize);

}   // namespace NCloud::NBlockStore::NStorage
