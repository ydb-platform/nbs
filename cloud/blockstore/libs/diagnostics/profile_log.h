#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>
#include <util/generic/variant.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IProfileLog
    : IStartable
{
    struct TBlockInfo
    {
        ui64 BlockIndex = 0;
        ui32 Checksum = 0;
    };

    struct TReplicaChecksums {
        ui32 ReplicaId = 0;
        TVector<ui32> Checksums;
    };

    struct TRangeInfo
    {
        TBlockRange64 Range;
        TVector<TReplicaChecksums> ReplicaChecksums;
    };

    struct TBlockCommitId
    {
        ui64 BlockIndex = 0;
        ui64 MinCommitIdOld = 0;
        ui64 MaxCommitIdOld = 0;
        ui64 MinCommitIdNew = 0;
        ui64 MaxCommitIdNew = 0;
    };

    struct TBlobUpdate
    {
        TBlockRange32 BlockRange;
        ui64 CommitId = 0;
    };

    struct TSysReadWriteRequest
    {
        ESysRequestType RequestType = ESysRequestType::MAX;
        TDuration Duration;
        TVector<TBlockRange64> Ranges;
    };

    struct TSysReadWriteRequestWithChecksums
    {
        ESysRequestType RequestType = ESysRequestType::MAX;
        TDuration Duration;
        TRangeInfo RangeInfo;
    };

    struct TSysReadWriteRequestBlockInfos
    {
        ESysRequestType RequestType = ESysRequestType::MAX;
        TVector<TBlockInfo> BlockInfos;
        ui64 CommitId = 0;
    };

    struct TSysReadWriteRequestBlockCommitIds
    {
        ESysRequestType RequestType = ESysRequestType::MAX;
        TVector<TBlockCommitId> BlockCommitIds;
        ui64 CommitId = 0;
    };

    struct TCleanupRequestBlobUpdates
    {
        TVector<TBlobUpdate> BlobUpdates;
        ui64 CleanupCommitId = 0;
    };

    struct TReadWriteRequest
    {
        EBlockStoreRequest RequestType = EBlockStoreRequest::MAX;
        TDuration Duration;
        TDuration PostponedTime;
        TBlockRange64 Range;
    };

    struct TReadWriteRequestBlockInfos
    {
        EBlockStoreRequest RequestType = EBlockStoreRequest::MAX;
        TVector<TBlockInfo> BlockInfos;
        ui64 CommitId = 0;
    };

    struct TMiscRequest
    {
        EBlockStoreRequest RequestType = EBlockStoreRequest::MAX;
        TDuration Duration;
    };

    struct TDescribeBlocksRequest
    {
        EPrivateRequestType RequestType = EPrivateRequestType::MAX;
        TDuration Duration;
        TBlockRange64 Range;
    };

    struct TRecord
    {
        TString DiskId;
        TInstant Ts;
        std::variant<
            TReadWriteRequest,
            TSysReadWriteRequest,
            TSysReadWriteRequestWithChecksums,
            TReadWriteRequestBlockInfos,
            TSysReadWriteRequestBlockInfos,
            TSysReadWriteRequestBlockCommitIds,
            TDescribeBlocksRequest,
            TMiscRequest,
            TCleanupRequestBlobUpdates
        > Request;
    };

    virtual void Write(TRecord record) = 0;
    virtual bool Flush() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TProfileLogSettings
{
    TString FilePath;
    TDuration TimeThreshold;
};

IProfileLogPtr CreateProfileLog(
    TProfileLogSettings settings,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

IProfileLogPtr CreateProfileLogStub();

}   // namespace NCloud::NBlockStore
