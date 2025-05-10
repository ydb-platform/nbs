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
        Read,
        Write,
        Zero,
        Last,
    };

    enum class ERequestStatus
    {
        Inflight,
        Success,
        Fail,
        Last,
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
        TInstant StartAt;
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
        TBlockRange64 blockRange);

    void OnRequestFinished(ui64 requestId, bool success);

    [[nodiscard]] TString GetStatJson() const;
    [[nodiscard]] TVector<TString> GetSizeBuckets() const;
    [[nodiscard]] TVector<TString> GetTimeBuckets() const;
};

}   // namespace NCloud::NBlockStore::NStorage
