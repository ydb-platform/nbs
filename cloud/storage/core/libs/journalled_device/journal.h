#pragma once

#include "public.h"

#include "lsn_barriers.h"
#include "log_chain.h"
#include "log_index.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <atomic>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

class TJournal
{
private:
    const IKeyBufferStorePtr MetaStore;
    const IPageStorePtr DataStore;

    TLogRecordChain LogRecordChain;
    TLogPageMap LogPageMap;
    TWatermarkTracker FlushedLsnTracker;

    std::atomic<bool> AdvancingLastAckedLsn = false;
    std::atomic<ui64> LastAckedLsn = 0;

public:
    TJournal(IKeyBufferStorePtr logMetaStore, IPageStorePtr logDataStore);

    // Restoring

    [[nodiscard]] NThreading::TFuture<NCloud::NProto::TError> Restore();

    // Device API

    [[nodiscard]] auto Write(
        const NCloud::NProto::TWriteLogRecordRequest& request)
        -> NThreading::TFuture<NCloud::NProto::TError>;

    [[nodiscard]] auto Read(
        const NCloud::NProto::TReadPagesRequest& request) const
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse>;

    [[nodiscard]] auto ReadTail(ui64 afterLsn, ui64 maxRecordCnt) const
        -> NThreading::TFuture<NCloud::NProto::TReadJournalTailResponse>;

    [[nodiscard]] auto AdvanceLastAckedLsn(ui64 lastAckedLsn)
        -> NThreading::TFuture<NCloud::NProto::TError>;

    // Background cleanup

    [[nodiscard]] auto GetFirstRecordToFlush() const
        -> TFutureResultOrError<NCloud::NProto::TJournalRecord>;

    void MarkRecordAsFlushed(ui64 lsn);

    [[nodiscard]] auto CleanupFlushedRecords()
        -> NThreading::TFuture<NCloud::NProto::TError>;

private:
    [[nodiscard]] NCloud::NProto::TError FillPageGroups(
        const TVector<std::pair<ui64, TPageGroupRef>>& index,
        google::protobuf::RepeatedPtrField<
            NCloud::NProto::TDevicePageGroup>* pageGroups) const;
};

}   // namespace NCloud::NJournalled
