#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/size_literals.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EBlockRangeChecksumStatus
{
    Unknown,
    MinorError,
    MajorError,
    Ok
};

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

bool CanFixMismatch(bool isMinor, NProto::EResyncPolicy resyncPolicy);

std::unique_ptr<NActors::IActor> MakeResyncRangeActor(
    TRequestInfoPtr requestInfo,
    ui32 blockSize,
    TBlockRange64 range,
    TVector<TReplicaDescriptor> replicas,
    TString writerClientId,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::EResyncPolicy resyncPolicy,
    EBlockRangeChecksumStatus checksumStatus,
    NActors::TActorId volumeActorId,
    bool assignVolumeRequestId);

}   // namespace NCloud::NBlockStore::NStorage
