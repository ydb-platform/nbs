#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRequestsTimeTracker
{
public:
    enum class ERequestType
    {
        Read,
        Write,
        Zero,
        Describe,
        Last = Describe,
    };

    enum class ERequestStatus
    {
        Inflight,
        Success,
        Fail,
    };

    struct TBucketInfo
    {
        TString Key;
        TString Description;
        TString Tooltip;
    };

    struct TFirstSuccessStat
    {
        ERequestType RequestType = ERequestType::Read;
        TDuration FirstRequestStartTime;
        TDuration SuccessfulRequestStartTime;
        TDuration SuccessfulRequestFinishTime;
        size_t FailCount = 0;
    };

    struct TRequestInflight
    {
        ui64 StartTime = 0;
        TBlockRange64 BlockRange;
        ERequestType RequestType = ERequestType::Read;
    };

    using TInflightMap = THashMap<ui64, TRequestInflight>;

private:
    constexpr static size_t RequestTypeCount =
        static_cast<size_t>(ERequestType::Last) + 1;

    struct TTimeHistogram: public THistogram<TRequestUsTimeBuckets>
    {
        size_t BlockCount = 0;

        TTimeHistogram()
            : THistogram<TRequestUsTimeBuckets>(
                  EHistogramCounterOption::ReportSingleCounter)
        {}
    };

    struct TKey
    {
        size_t SizeBucket = 0;
        ERequestType RequestType = ERequestType::Read;
        ERequestStatus RequestStatus = ERequestStatus::Fail;

        [[nodiscard]] TString GetHtmlPrefix() const;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    struct TEqual
    {
        bool operator()(const TKey& lhs, const TKey& rhs) const;
    };

    struct TFirstRequest
    {
        ui64 StartTime = 0;
        ui64 FinishTime = 0;
        size_t FailCount = 0;
    };

    const ui64 ConstructionTime;

    std::array<TFirstRequest, RequestTypeCount> FirstRequests;
    TInflightMap InflightRequests;
    THashMap<TKey, TTimeHistogram, THash, TEqual> Histograms;

    [[nodiscard]] NJson::TJsonValue BuildPercentilesJson() const;

    [[nodiscard]] std::optional<TRequestsTimeTracker::TFirstSuccessStat>
    StatFirstSuccess(
        const TRequestInflight& request,
        bool success,
        ui64 finishTime);

    struct TThroughputTracker
    {
        ui64 StartTime = 0;
        ui64 TotalBlocks = 0;
        ui64 TotalOps = 0;

        void AddOperation(ui64 blocks);
        std::pair<double, double> GetRatesAndReset(
            ui64 currentTime,
            ui32 blockSize);
    };

    std::array<TThroughputTracker, RequestTypeCount> ThroughputCounters;

public:
    explicit TRequestsTimeTracker(const ui64 constructionTime);

    static TVector<TBucketInfo> GetSizeBuckets(ui32 blockSize);
    static TVector<TBucketInfo> GetTimeBuckets();
    static TVector<TBucketInfo> GetPercentileBuckets();

    void OnRequestStarted(
        ERequestType requestType,
        ui64 requestId,
        TBlockRange64 blockRange,
        ui64 startTime);

    // Marks that the request is completed and returns stat when the request
    // succeeds for the first time.
    [[nodiscard]] std::optional<TFirstSuccessStat>
    OnRequestFinished(ui64 requestId, bool success, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles, ui32 blockSize);

    void ResetStats();

    [[nodiscard]] const TInflightMap& GetInflightOperations() const;
};

}   // namespace NCloud::NBlockStore::NStorage
