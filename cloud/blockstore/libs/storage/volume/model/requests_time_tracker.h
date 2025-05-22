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

    struct TBucketInfo
    {
        TString Key;
        TString Description;
        TString Tooltip;
    };

private:
    enum class ERequestStatus
    {
        Inflight,
        Success,
        Fail,
    };

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

    struct TRequestInflight
    {
        ui64 StartTime = 0;
        TBlockRange64 BlockRange;
        ERequestType RequestType = ERequestType::Read;
    };

    THashMap<ui64, TRequestInflight> InflightRequests;
    THashMap<TKey, TTimeHistogram, THash, TEqual> Histograms;

    [[nodiscard]] NJson::TJsonValue BuildPercentilesJson() const;

public:
    explicit TRequestsTimeTracker();

    static TVector<TBucketInfo> GetSizeBuckets(ui32 blockSize);
    static TVector<TBucketInfo> GetTimeBuckets();
    static TVector<TBucketInfo> GetPercentileBuckets();

    void OnRequestStarted(
        ERequestType requestType,
        ui64 requestId,
        TBlockRange64 blockRange,
        ui64 startTime);

    void OnRequestFinished(ui64 requestId, bool success, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles, ui32 blockSize) const;
};

}   // namespace NCloud::NBlockStore::NStorage
