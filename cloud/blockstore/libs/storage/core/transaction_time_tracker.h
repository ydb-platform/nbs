#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// A class for tracking time taken by transactions and implementing the
// ITransactionTracker interface. This class maintains histograms for different
// statuses of transactions and provides methods to track when transactions
// start and finish. It also allows retrieval of statistical information in JSON
// format.

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

    const TVector<TString> TransactionTypes;

    THashMap<ui64, TTransactionInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    explicit TTransactionTimeTracker(std::span<const TString> transactionTypes);

    [[nodiscard]] TVector<TBucketInfo> GetTransactionBuckets() const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;

    // Implements ITransactionTracker
    void OnStarted(
        ui64 transactionId,
        TString transactionName,
        ui64 startTime) override;

    void OnFinished(ui64 transactionId, ui64 finishTime) override;

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;

    void ResetStats();
};

}   // namespace NCloud::NBlockStore::NStorage
