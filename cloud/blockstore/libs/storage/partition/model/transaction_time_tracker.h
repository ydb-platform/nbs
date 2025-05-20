#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/partition/part_tx.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TTransactionTimeTracker : public ITransactionTracker
{
public:
    enum class EStatus
    {
        Inflight,
        Finished,
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

    THashMap<ui64, TTransactionInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    TTransactionTimeTracker();

    static TVector<TBucketInfo> GetTransactionBuckets();
    static TVector<TBucketInfo> GetTimeBuckets();

    // Implements ITransactionTracker
    void OnStarted(
        ui64 transactionId,
        TString transactionName,
        ui64 startTime) override;

    void OnFinished(ui64 transactionId, ui64 finishTime) override;

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
};

}   // namespace NCloud::NBlockStore::NStorage
