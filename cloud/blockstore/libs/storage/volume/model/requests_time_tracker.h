#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/core/histogram.h>

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
        Read = 0,
        Write = 1,
        Zero = 2,
        Last = Zero,
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
        size_t SizeBucket;
        ERequestType RequestType;
        ERequestStatus RequestStatus;

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
        ui64 StartAt = 0;
        TBlockRange64 BlockRange;
        ERequestType RequestType = ERequestType::Read;
    };

    THashMap<ui64, TRequestInflight> InflightRequests;
    THashMap<TKey, TTimeHistogram, THash, TEqual> Histograms;

public:
    explicit TRequestsTimeTracker();

    void OnRequestStart(
        ERequestType requestType,
        ui64 requestId,
        TBlockRange64 blockRange,
        ui64 startTime);

    void OnRequestFinished(ui64 requestId, bool success, ui64 finishTime);

    [[nodiscard]] TString GetStatJson(ui64 now, ui32 blockSize) const;
    [[nodiscard]] TVector<TBucketInfo> GetSizeBuckets(ui32 blockSize) const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;
};

}   // namespace NCloud::NBlockStore::NStorage
