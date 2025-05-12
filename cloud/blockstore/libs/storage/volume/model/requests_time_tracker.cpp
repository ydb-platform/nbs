#include "requests_time_tracker.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf TotalTimeBucketName = "Total";
constexpr TStringBuf TotalSizeBucketName = "TotalSize";

constexpr size_t BlockCountBucketsCount = 12;
constexpr size_t TotalSizeBucket = BlockCountBucketsCount;
constexpr std::array<size_t, BlockCountBucketsCount> RequestSizeBuckets = {
    {1,
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
     std::numeric_limits<size_t>::max()}};

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

TVector<TRequestsTimeTracker::TBucketInfo> MakeSizeBuckets(ui32 blockSize)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString TRequestsTimeTracker::TKey::GetHtmlPrefix() const
{
    static const auto Sizes = MakeSizeBuckets(1);

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
           IntHash(static_cast<size_t>(key.RequestType)) +
           IntHash(static_cast<size_t>(key.RequestStatus));
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

TRequestsTimeTracker::TRequestsTimeTracker()
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

void TRequestsTimeTracker::OnRequestStart(
    ERequestType requestType,
    ui64 requestId,
    TBlockRange64 blockRange,
    ui64 startTime)
{
    InflightRequests.emplace(
        requestId,
        TRequestInflight{
            .StartAt = startTime,
            .BlockRange = blockRange,
            .RequestType = requestType});
}

void TRequestsTimeTracker::OnRequestFinished(
    ui64 requestId,
    bool success,
    ui64 finishTime)
{
    TRequestInflight request;
    {
        auto it = InflightRequests.find(requestId);
        if (it == InflightRequests.end()) {
            return;
        }
        request = it->second;
        InflightRequests.erase(requestId);
    }

    auto duration = CyclesToDurationSafe(finishTime - request.StartAt);

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
}

TString TRequestsTimeTracker::GetStatJson(ui64 now, ui32 blockSize) const
{
    NJson::TJsonValue allStat(NJson::EJsonValueType::JSON_MAP);

    const auto times = TRequestUsTimeBuckets::MakeNames();

    // Build finished request counters.
    for (const auto& [key, histogram]: Histograms) {
        size_t total = 0;
        const auto htmlPrefix = key.GetHtmlPrefix();
        for (size_t j = 0; j < TRequestUsTimeBuckets::BUCKETS_COUNT; ++j) {
            allStat[htmlPrefix + times[j]] =
                (histogram.Buckets[j] ? ToString(histogram.Buckets[j]) : "");
            total += histogram.Buckets[j];
        }
        allStat[htmlPrefix + TotalTimeBucketName] = ToString(total);
        allStat[htmlPrefix + TotalSizeBucketName] =
            FormatByteSize(histogram.BlockCount * blockSize);
    }

    // Build inflight requests counters
    auto getHtmlKey =
        [](size_t sizeBucket, ERequestType requestType, TStringBuf timeBacket)
    {
        auto key = TKey{
            .SizeBucket = sizeBucket,
            .RequestType = requestType,
            .RequestStatus = ERequestStatus::Inflight};
        return key.GetHtmlPrefix() + timeBacket;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [requestId, request]: InflightRequests) {
        auto requestType = request.RequestType;
        const auto sizeBucket = GetSizeBucket(request.BlockRange);
        const auto& timeBucketName =
            GetTimeBucketName(CyclesToDurationSafe(now - request.StartAt));

        // Time
        ++inflight[getHtmlKey(sizeBucket, requestType, timeBucketName)];
        ++inflight[getHtmlKey(sizeBucket, requestType, TotalTimeBucketName)];
        ++inflight[getHtmlKey(TotalSizeBucket, requestType, timeBucketName)];
        ++inflight
            [getHtmlKey(TotalSizeBucket, requestType, TotalTimeBucketName)];

        // Size
        inflight[getHtmlKey(sizeBucket, requestType, TotalSizeBucketName)] +=
            request.BlockRange.Size() * blockSize;
        inflight
            [getHtmlKey(TotalSizeBucket, requestType, TotalSizeBucketName)] +=
            request.BlockRange.Size() * blockSize;
    }
    for (const auto& [key, count]: inflight) {
        if (key.EndsWith(TotalSizeBucketName)) {
            allStat[key] = FormatByteSize(count);
        } else {
            allStat[key] = ToString(count);
        }
    }

    NJson::TJsonValue json;
    json["stat"] = std::move(allStat);

    TStringStream out;
    NJson::WriteJson(&out, &json);
    return out.Str();
}

TVector<TRequestsTimeTracker::TBucketInfo> TRequestsTimeTracker::GetSizeBuckets(
    ui32 blockSize) const
{
    return MakeSizeBuckets(blockSize);
}

TVector<TRequestsTimeTracker::TBucketInfo>
TRequestsTimeTracker::GetTimeBuckets() const
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
        .Key = TString(TotalTimeBucketName),
        .Description = "Total",
        .Tooltip = ""});
    result.push_back(TBucketInfo{
        .Key = TString(TotalSizeBucketName),
        .Description = "Total Size",
        .Tooltip = ""});
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
