#pragma once

#include "storage_group.h"

#include <cloud/fastshard/libs/common/context.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/sequencer.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

/**
 * Keeping track of largest write lsn acked so far.
 */
class TDeviceProxy
{
public:
    const TString DeviceUUID;

private:
    const IStorageNodePtr StorageNode;
    const TStorageGroupConfig Config;
    const ITimerPtr Timer;
    silk::FiberSequencer Acked;

public:
    TDeviceProxy(
            TStorageDevice device,
            TStorageGroupConfig config,
            ITimerPtr timer)
        : DeviceUUID(std::move(device.DeviceUUID))
        , StorageNode(std::move(device.Node))
        , Config(std::move(config))
        , Timer(std::move(timer))
    {}

    bool CanServe(ui64 lsn) const
    {
        return Acked.get() >= lsn;
    }

    NProto::TError Write(
        NCloud::NFastShard::TFastShardContext& ctx,
        const NProto::TWriteLogRecordRequest& request);

    NProto::TError Read(
        NCloud::NFastShard::TFastShardContext& ctx,
        const NProto::TReadPagesRequest& request,
        NProto::TReadPagesResponse* response);

    NProto::TError ReadJournalTail(
        NCloud::NFastShard::TFastShardContext& ctx,
        ui64 afterLsn,
        ui32 maxRecords,
        NProto::TReadJournalTailResponse* response);

    ui64 GetLastAckedLsn() const
    {
        return Acked.get();
    }

    void SeedLsn(ui64 lsn)
    {
        Acked.advance(lsn);
    }

    NProto::TError AdvanceLsnLowWatermark(
        NCloud::NFastShard::TFastShardContext& ctx,
        ui64 lsn);

    NProto::TError Acquire(NCloud::NFastShard::TFastShardContext& ctx);

    NProto::TError Release(NCloud::NFastShard::TFastShardContext& ctx);

private:
    NProto::TError MakeProxyError(
        const NProto::TError& error,
        TStringBuf what) const;
};

using TDeviceProxyPtr = std::shared_ptr<TDeviceProxy>;

}   // namespace NCloud::NFileStore::NStorage::NFastShard
