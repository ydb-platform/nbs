#pragma once

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TBSGroupOperationTimeTracker
{
public:
    enum class EStatus
    {
        Inflight,
        Finished,
    };

    enum class EOperationType
    {
        Read,
        Write,
        Patch,
    };

    struct TBucketInfo
    {
        TString OperationName;
        TString Key;
        TString Description;
        TString Tooltip;
    };

    struct TOperationInflight
    {
        ui64 StartTime = 0;
        TString OperationName;
        ui32 GroupId = 0;
        ui32 BlockSize = 0;
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
        TString OperationName;
        ui32 GroupId = 0;
        EStatus Status = EStatus::Inflight;

        [[nodiscard]] TString GetHtmlPrefix() const;

        bool operator==(const TKey& rhs) const = default;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    TVector<TString> OperationTypes;

    THashMap<ui64, TOperationInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    explicit TBSGroupOperationTimeTracker() = default;

    void OnStarted(
        ui64 operationId,
        ui32 groupId,
        EOperationType operationType,
        ui64 startTime,
        ui32 blockSize);

    void OnFinished(ui64 operationId, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;

    void ResetStats();

    [[nodiscard]] const THashMap<
        ui64,
        TBSGroupOperationTimeTracker::TOperationInflight>&
    GetInflightOperations() const;
};

}   // namespace NCloud::NBlockStore::NStorage
