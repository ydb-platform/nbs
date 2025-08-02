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
        TString OperationName;
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

    struct TOperationInflight
    {
        ui64 StartTime = 0;
        TString OperationName;
        ui32 GroupId = 0;
    };

    TVector<TString> OperationTypes;

    THashMap<ui64, TOperationInflight> Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    explicit TGroupOperationTimeTracker() = default;

    void OnStarted(
        ui64 operationId,
        ui32 groupId,
        EGroupOperationType operationType,
        ui64 startTime);

    void OnFinished(ui64 operationId, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;
};

}   // namespace NCloud::NBlockStore::NStorage
