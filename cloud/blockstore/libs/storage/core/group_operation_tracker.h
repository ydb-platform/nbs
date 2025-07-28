#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TGroupOperationTimeTracker
{
public:
    enum class EStatus
    {
        Inflight,
        Finished,
    };

    enum class EGroupOperationType
    {
        Read,
        Write,
    };

    struct TBucketInfo
    {
        TString TransactionName;
        TString Key;
        TString Description;
        TString Tooltip;
    };

private:
    struct TTimeHistogram: public THistogram<TRequestUsTimeBuckets>
    {
        TTimeHistogram()
            : THistogram<TRequestUsTimeBuckets>(
                  EHistogramCounterOption::ReportSingleCounter)
        {}
    };

    struct TKey
    {
        TString TransactionName;
        EStatus Status = EStatus::Inflight;

        [[nodiscard]] TString GetHtmlPrefix() const;

        bool operator==(const TKey& rhs) const = default;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    struct TTransactionInflight
    {
        ui64 StartTime = 0;
        TString TransactionName;
    };

    TVector<TString> TransactionTypes;

    THashMap<ui64, TTransactionInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    explicit TGroupOperationTimeTracker() = default;

    void OnStarted(
        ui64 transactionId,
        ui32 groupId,
        EGroupOperationType operationType,
        ui64 startTime);

    void OnFinished(ui64 transactionId, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
};

}   // namespace NCloud::NBlockStore::NStorage
