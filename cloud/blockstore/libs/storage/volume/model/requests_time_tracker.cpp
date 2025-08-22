#include "requests_time_tracker.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

#include <span>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockCountBucketsCount = 12;
constexpr size_t TotalSizeBucket = BlockCountBucketsCount;
constexpr std::array<size_t, BlockCountBucketsCount> RequestSizeBuckets = {
    1,
    2,
    4,
    8,
    16,
    32,
    64,
    128,
    256,
    512,
    1024,
    std::numeric_limits<size_t>::max()};

const TVector<TPercentileDesc> Percentiles{
    {0.100, "10"},
    {0.200, "20"},
    {0.300, "30"},
    {0.400, "40"},
    {0.500, "50"},
    {0.600, "60"},
    {0.700, "70"},
    {0.800, "80"},
    {0.900, "90"},
    {0.990, "99"},
    {0.999, "99.9"},
    {1.0, "100"},
};

////////////////////////////////////////////////////////////////////////////////

size_t GetSizeBucket(TBlockRange64 range)
{
    auto idx = std::distance(
        RequestSizeBuckets.begin(),
        LowerBound(
            RequestSizeBuckets.begin(),
            RequestSizeBuckets.end(),
            range.Size()));
    return idx;
}

const TString& GetTimeBucketName(TDuration duration)
{
    static const auto TimeNames = TRequestUsTimeBuckets::MakeNames();

    auto idx = std::distance(
        TRequestUsTimeBuckets::Buckets.begin(),
        LowerBound(
            TRequestUsTimeBuckets::Buckets.begin(),
            TRequestUsTimeBuckets::Buckets.end(),
            duration.MicroSeconds()));
    return TimeNames[idx];
}

TVector<double> BuildPercentilesForHistogram(
    const std::span<const ui64> histogram)
{
    Y_DEBUG_ABORT_UNLESS(
        histogram.size() == TRequestUsTimeBuckets::Buckets.size());

    TVector<TBucketInfo> buckets;
    buckets.reserve(histogram.size());
    for (size_t i = 0; i < histogram.size(); ++i) {
        buckets.emplace_back(TRequestUsTimeBuckets::Buckets[i], histogram[i]);
    }

    return CalculateWeightedPercentiles(buckets, Percentiles);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString TRequestsTimeTracker::TKey::GetHtmlPrefix() const
{
    static const auto Sizes = TRequestsTimeTracker::GetSizeBuckets(1);

    TStringBuilder builder;
    switch (RequestType) {
        case TRequestsTimeTracker::ERequestType::Read: {
            builder << "R_";
            break;
        }
        case TRequestsTimeTracker::ERequestType::Write: {
            builder << "W_";
            break;
        }
        case TRequestsTimeTracker::ERequestType::Zero: {
            builder << "Z_";
            break;
        }
        case TRequestsTimeTracker::ERequestType::Describe: {
            builder << "D_";
            break;
        }
    }
    builder << Sizes[SizeBucket].Key;
    switch (RequestStatus) {
        case ERequestStatus::Inflight: {
            builder << "_inflight_";
            break;
        }
        case ERequestStatus::Success: {
            builder << "_ok_";
            break;
        }
        case ERequestStatus::Fail: {
            builder << "_fail_";
            break;
        }
    }
    return builder;
}

ui64 TRequestsTimeTracker::THash::operator()(const TKey& key) const
{
    return IntHash(key.SizeBucket) +
           IntHash(static_cast<size_t>(key.RequestType) << 5) +
           IntHash(static_cast<size_t>(key.RequestStatus) << 8);
}

bool TRequestsTimeTracker::TEqual::operator()(
    const TKey& lhs,
    const TKey& rhs) const
{
    auto makeTie = [](const TKey& val)
    {
        return std::tie(val.SizeBucket, val.RequestType, val.RequestStatus);
    };
    return makeTie(lhs) == makeTie(rhs);
}

////////////////////////////////////////////////////////////////////////////////

TRequestsTimeTracker::TRequestsTimeTracker(const ui64 constructionTime)
    : ConstructionTime(constructionTime)
{
    for (size_t sizeBucket = 0; sizeBucket <= TotalSizeBucket; ++sizeBucket) {
        for (size_t requestType = 0;
             requestType <= static_cast<size_t>(ERequestType::Last);
             ++requestType)
        {
            auto key = TKey{
                .SizeBucket = sizeBucket,
                .RequestType = static_cast<ERequestType>(requestType),
                .RequestStatus = ERequestStatus::Inflight};
            Histograms[key];
        }
    }
}

void TRequestsTimeTracker::OnRequestStarted(
    ERequestType requestType,
    ui64 requestId,
    TBlockRange64 blockRange,
    ui64 startTime)
{
    const auto requestTypeIndex = static_cast<size_t>(requestType);
    auto& firstRequest = FirstRequests[requestTypeIndex];
    if (firstRequest.StartTime == 0) {
        firstRequest.StartTime = startTime;
    }

    InflightRequests.emplace(
        requestId,
        TRequestInflight{
            .StartTime = startTime,
            .BlockRange = blockRange,
            .RequestType = requestType});
}

std::optional<TRequestsTimeTracker::TFirstSuccessStat>
TRequestsTimeTracker::StatFirstSuccess(
    const TRequestInflight& request,
    bool success,
    ui64 finishTime)
{
    const auto requestTypeIndex = static_cast<size_t>(request.RequestType);
    auto& firstRequest = FirstRequests[requestTypeIndex];
    if (firstRequest.FinishTime != 0) {
        return std::nullopt;
    }

    if (!success) {
        ++firstRequest.FailCount;
        return std::nullopt;
    }

    firstRequest.FinishTime = finishTime;

    return TFirstSuccessStat{
        .RequestType = request.RequestType,
        .FirstRequestStartTime =
            CyclesToDurationSafe(firstRequest.StartTime - ConstructionTime),
        .SuccessfulRequestStartTime =
            CyclesToDurationSafe(request.StartTime - ConstructionTime),
        .SuccessfulRequestFinishTime =
            CyclesToDurationSafe(firstRequest.FinishTime - ConstructionTime),
        .FailCount = firstRequest.FailCount};
}

std::optional<TRequestsTimeTracker::TFirstSuccessStat>
TRequestsTimeTracker::OnRequestFinished(
    ui64 requestId,
    bool success,
    ui64 finishTime)
{
    TRequestInflight request;
    {
        auto it = InflightRequests.find(requestId);
        if (it == InflightRequests.end()) {
            return {};
        }
        request = it->second;
        InflightRequests.erase(requestId);
    }

    auto duration = CyclesToDurationSafe(finishTime - request.StartTime);

    TKey key{
        .SizeBucket = GetSizeBucket(request.BlockRange),
        .RequestType = request.RequestType,
        .RequestStatus =
            success ? ERequestStatus::Success : ERequestStatus::Fail};
    Histograms[key].Increment(duration.MicroSeconds());
    Histograms[key].BlockCount += request.BlockRange.Size();

    key.SizeBucket = TotalSizeBucket;
    Histograms[key].Increment(duration.MicroSeconds());
    Histograms[key].BlockCount += request.BlockRange.Size();

    return StatFirstSuccess(request, success, finishTime);
}

NJson::TJsonValue TRequestsTimeTracker::BuildPercentilesJson() const
{
    static const auto PercentileBuckets =
        TRequestsTimeTracker::GetPercentileBuckets();

    NJson::TJsonValue result(NJson::EJsonValueType::JSON_MAP);

    for (size_t sizeBucket = 0; sizeBucket <= TotalSizeBucket; ++sizeBucket) {
        for (size_t requestType = 0;
             requestType <= static_cast<size_t>(ERequestType::Last);
             ++requestType)
        {
            const TKey key{
                .SizeBucket = sizeBucket,
                .RequestType = static_cast<ERequestType>(requestType),
                .RequestStatus = ERequestStatus::Success};

            if (const auto* histogram = Histograms.FindPtr(key)) {
                const auto timePercentiles =
                    BuildPercentilesForHistogram(histogram->Buckets);
                for (size_t percentileIdx = 0;
                     percentileIdx < timePercentiles.size();
                     ++percentileIdx)
                {
                    const auto htmlKey = key.GetHtmlPrefix() +
                                         PercentileBuckets[percentileIdx].Key;
                    result[htmlKey] = FormatDuration(TDuration::MicroSeconds(
                        timePercentiles[percentileIdx]));
                }
            }
        }
    }
    return result;
}

TString TRequestsTimeTracker::GetStatJson(ui64 nowCycles, ui32 blockSize) const
{
    NJson::TJsonValue allStat(NJson::EJsonValueType::JSON_MAP);

    const auto times = TRequestUsTimeBuckets::MakeNames();

    // Build finished request counters.
    for (const auto& [key, histogram]: Histograms) {
        size_t total = 0;
        const auto htmlPrefix = key.GetHtmlPrefix();
        for (size_t i = 0; i < TRequestUsTimeBuckets::BUCKETS_COUNT; ++i) {
            allStat[htmlPrefix + times[i]] =
                (histogram.Buckets[i] ? ToString(histogram.Buckets[i]) : "");
            total += histogram.Buckets[i];
        }
        allStat[htmlPrefix + "Count"] = ToString(total);
        allStat[htmlPrefix + "TotalSize"] =
            FormatByteSize(histogram.BlockCount * blockSize);
    }

    // Build inflight requests counters
    auto getHtmlKey = [](size_t sizeBucket,
                         ERequestType requestType,
                         TStringBuf timeBucketName)
    {
        auto key = TKey{
            .SizeBucket = sizeBucket,
            .RequestType = requestType,
            .RequestStatus = ERequestStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [requestId, request]: InflightRequests) {
        auto requestType = request.RequestType;
        const auto sizeBucket = GetSizeBucket(request.BlockRange);
        const auto& timeBucketName = GetTimeBucketName(
            CyclesToDurationSafe(nowCycles - request.StartTime));

        // Time
        ++inflight[getHtmlKey(sizeBucket, requestType, timeBucketName)];
        ++inflight[getHtmlKey(sizeBucket, requestType, "Count")];
        ++inflight[getHtmlKey(TotalSizeBucket, requestType, timeBucketName)];
        ++inflight[getHtmlKey(TotalSizeBucket, requestType, "Count")];

        // Size
        inflight[getHtmlKey(sizeBucket, requestType, "TotalSize")] +=
            request.BlockRange.Size() * blockSize;
        inflight[getHtmlKey(TotalSizeBucket, requestType, "TotalSize")] +=
            request.BlockRange.Size() * blockSize;
    }
    for (const auto& [key, count]: inflight) {
        if (key.EndsWith("TotalSize")) {
            allStat[key] = FormatByteSize(count);
        } else {
            allStat[key] = ToString(count);
        }
    }

    NJson::TJsonValue json;
    json["stat"] = std::move(allStat);
    json["percentiles"] = BuildPercentilesJson();

    TStringStream out;
    NJson::WriteJson(&out, &json);
    return out.Str();
}

// static
TVector<TRequestsTimeTracker::TBucketInfo> TRequestsTimeTracker::GetSizeBuckets(
    ui32 blockSize)
{
    TVector<TRequestsTimeTracker::TBucketInfo> result;

    size_t lastBucket = 0;
    for (size_t i = 0; i < RequestSizeBuckets.size() - 1; ++i) {
        const size_t sizeBucket = RequestSizeBuckets[i];
        TRequestsTimeTracker::TBucketInfo bucket{.Key = ToString(sizeBucket)};

        if (lastBucket && lastBucket != sizeBucket) {
            bucket.Description =
                TStringBuilder()
                << "[" << FormatByteSize(lastBucket * blockSize) << ".."
                << FormatByteSize(sizeBucket * blockSize) << "]";
        } else {
            bucket.Description =
                TStringBuilder()
                << "[" << FormatByteSize(sizeBucket * blockSize) << "]";
        }
        lastBucket = sizeBucket + 1;
        result.push_back(std::move(bucket));
    }

    result.emplace_back(TRequestsTimeTracker::TBucketInfo{
        .Key = "Inf",
        .Description = "[" + FormatByteSize(lastBucket * blockSize) + "..Inf]",
        .Tooltip = ""});
    result.emplace_back(TRequestsTimeTracker::TBucketInfo{
        .Key = "Total",
        .Description = "Total",
        .Tooltip = ""});
    return result;
}

// static
TVector<TRequestsTimeTracker::TBucketInfo>
TRequestsTimeTracker::GetTimeBuckets()
{
    TVector<TBucketInfo> result;
    TDuration last;
    for (const auto& time: TRequestUsTimeBuckets::MakeNames()) {
        const auto us = TryFromString<ui64>(time);

        TBucketInfo bucket{
            .Key = time,
            .Description =
                us ? FormatDuration(TDuration::MicroSeconds(*us)) : time,
            .Tooltip =
                "[" + FormatDuration(last) + ".." + bucket.Description + "]"};

        last = TDuration::MicroSeconds(us.GetOrElse(0));
        result.push_back(std::move(bucket));
    }
    result.push_back(TBucketInfo{
        .Key = "Count",
        .Description = "Count",
        .Tooltip = "Total request count"});
    result.push_back(TBucketInfo{
        .Key = "TotalSize",
        .Description = "Total Size",
        .Tooltip = "Total requests size"});
    return result;
}

// static
TVector<TRequestsTimeTracker::TBucketInfo>
TRequestsTimeTracker::GetPercentileBuckets()
{
    TVector<TBucketInfo> result;
    for (const auto& p: Percentiles) {
        result.push_back(TBucketInfo{
            .Key = "P" + ToString(p.second),
            .Description = ToString(p.second),
            .Tooltip = ""});
    }
    return result;
}

void TRequestsTimeTracker::ResetStats()
{
    for (auto& [key, histogram]: Histograms) {
        histogram.Reset();
    }
}

TVector<std::pair<ui64, TRequestsTimeTracker::TRequestInflight>>
TRequestsTimeTracker::GetInflightOperations() const
{
    TVector<std::pair<ui64, TRequestInflight>> result(
        InflightRequests.begin(),
        InflightRequests.end());

    Sort(
        result,
        [](const auto& a, const auto& b)
        { return a.second.StartTime < b.second.StartTime; });

    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
