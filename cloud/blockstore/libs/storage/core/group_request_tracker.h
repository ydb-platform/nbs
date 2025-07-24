#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TGroupOperationTimeTracker: public ITransactionTracker
{
public:
    enum class EOperationType : ui8
    {
        Read,
        Write,
    };

private:
    struct TOperationKey
    {
        EOperationType Operation;
        ui64 GroupId;

        bool operator==(const TOperationKey& rhs) const noexcept
        {
            return Operation == rhs.Operation && GroupId == rhs.GroupId;
        }
    };

    struct TOperationKeyHash
    {
        size_t operator()(const TOperationKey& key) const noexcept
        {
            size_t res = static_cast<size_t>(key.Operation);
            res ^= (size_t)key.GroupId + 0x9e3779b9 + (res << 6) + (res >> 2);
            return res;
        }
    };

    struct TTimeHistogram
    {
        static constexpr size_t BUCKETS_COUNT =
            TRequestUsTimeBuckets::BUCKETS_COUNT;
        ui64 Buckets[BUCKETS_COUNT] = {};

        void Increment(ui64 micros);

        size_t GetBucketIndex(ui64 micros) const;
    };

    struct TInflightOperation
    {
        ui64 StartTime = 0;
        TOperationKey Key;
    };

private:
    THashMap<ui64, TInflightOperation> Inflight;   // txID -> inflight info
    THashMap<TOperationKey, TTimeHistogram, TOperationKeyHash> Histograms;

public:
    explicit TGroupOperationTimeTracker() = default;

    void OnStarted(
        ui64 transactionId,
        TString transactionName,
        ui64 startTime) override;
    void OnFinished(ui64 transactionId, ui64 finishTime) override;

    TString GetStatJson(ui64 nowCycles) const;

    void GetAllOperationKeys(TVector<TOperationKey>& keys) const;
    ui64 GetHistogramCount(const TOperationKey& key, const TString& timeBucket) const;

private:
    static TMaybe<TOperationKey> ParseTransactionName(const TString& name);
    static TString OperationTypeToString(EOperationType op);
};

}   // namespace NCloud::NBlockStore::NStorage
