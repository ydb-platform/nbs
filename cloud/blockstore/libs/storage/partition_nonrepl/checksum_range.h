#pragma once

#include "part_mirror_resync_util.h"
#include "part_nonrepl_events_private.h"

#include <cloud/blockstore/libs/diagnostics/profile_log.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TChecksumRangeActorCompanion
{
private:
    TVector<TReplicaDescriptor> Replicas;

    TBlockRange64 Range;
    TInstant ChecksumStartTs;
    TDuration ChecksumDuration;
    ui32 CalculatedChecksumsCount = 0;
    TVector<ui64> Checksums;
    NProto::TError Error;

public:
    explicit TChecksumRangeActorCompanion(TVector<TReplicaDescriptor> replicas);

    TChecksumRangeActorCompanion() = default;

    void CalculateChecksums(
        const NActors::TActorContext& ctx,
        TBlockRange64 range);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);
    void HandleChecksumUndelivery(const NActors::TActorContext& ctx);

    bool IsFinished() const;
    const TVector<ui64>& GetChecksums() const;
    NProto::TError GetError() const;
    TInstant GetChecksumStartTs() const;
    TDuration GetChecksumDuration() const;
    IProfileLog::TRangeInfo GetRangeInfo() const;

private:
    void CalculateReplicaChecksum(
        const NActors::TActorContext& ctx,
        TBlockRange64 range,
        int idx);
};

}   // namespace NCloud::NBlockStore::NStorage
