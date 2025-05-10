#include "requests_time_tracker.h"

#include "util/string/builder.h"

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t BlockCountBucketsCount = 11;

constexpr std::array<size_t, BlockCountBucketsCount> RequestSizeBuckets = {
    {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}};

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

TVector<TString> MakeSizeNames()
{
    TVector<TString> names;
    for (ui32 i = 0; i < RequestSizeBuckets.size(); ++i) {
        if (i != RequestSizeBuckets.size() - 1) {
            names.emplace_back(ToString(RequestSizeBuckets[i]));
        } else {
            names.emplace_back("Inf");
        }
    }

    names.push_back("Total");
    return names;
}

}   // namespace

//==============================================================================

TString TRequestsTimeTracker::TKey::GetHtmlPrefix() const
{
    static const TVector<TString> Sizes = MakeSizeNames();

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
        case TRequestsTimeTracker::ERequestType::Last: {
            break;
        }
    }
    builder << Sizes[SizeBucket];
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
        case ERequestStatus::Last: {
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

//==============================================================================

TRequestsTimeTracker::TRequestsTimeTracker()
{
    // Note! SizeBucket == BlockCountBucketsCount is total.
    for (size_t i = 0; i < BlockCountBucketsCount + 1; ++i) {
        for (size_t requestType = 0;
             requestType < static_cast<size_t>(ERequestType::Last);
             ++requestType)
        {
            auto key = TKey{
                .SizeBucket = i,
                .RequestType = static_cast<ERequestType>(requestType),
                .RequestStatus = ERequestStatus::Inflight};
            Histograms[key];
        }
    }
}

void TRequestsTimeTracker::OnRequestStart(
    ERequestType requestType,
    ui64 requestId,
    TBlockRange64 blockRange)
{
    InflightRequests.emplace(
        requestId,
        TRequestInflight{
            .StartAt = TInstant::Now(),
            .BlockRange = blockRange,
            .RequestType = requestType});
}

void TRequestsTimeTracker::OnRequestFinished(ui64 requestId, bool success)
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

    auto duration = TInstant::Now() - request.StartAt;

    TKey key{
        .SizeBucket = GetSizeBucket(request.BlockRange),
        .RequestType = request.RequestType,
        .RequestStatus =
            success ? ERequestStatus::Success : ERequestStatus::Fail};
    Histograms[key].Increment(duration.MicroSeconds());

    key.SizeBucket = RequestSizeBuckets.size();
    Histograms[key].Increment(duration.MicroSeconds());
}

TString TRequestsTimeTracker::GetStatJson() const
{
    TStringStream out;
    NJson::TJsonValue json;

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
        allStat[htmlPrefix + "Total"] = ToString(total);
    }

    // Build inflight requests counters
    auto getHtmlKey = [](size_t sizeBucket,
                         ERequestType requestType,
                         const TString& timeBacket)
    {
        auto key = TKey{
            .SizeBucket = sizeBucket,
            .RequestType = requestType,
            .RequestStatus = ERequestStatus::Inflight};
        return key.GetHtmlPrefix() + timeBacket;
    };

    TMap<TString, size_t> inflight;
    const auto now = TInstant::Now();
    for (const auto& [requestId, request]: InflightRequests) {
        auto requestType = request.RequestType;
        const auto sizeBucket = GetSizeBucket(request.BlockRange);
        const auto& timeBucketName = GetTimeBucketName(now - request.StartAt);
        const auto totalSizeBucket = RequestSizeBuckets.size();
        const auto& totalTimeBucketName = "Total";

        ++inflight[getHtmlKey(sizeBucket, requestType, timeBucketName)];
        ++inflight[getHtmlKey(sizeBucket, requestType, totalTimeBucketName)];
        ++inflight[getHtmlKey(totalSizeBucket, requestType, timeBucketName)];
        ++inflight
            [getHtmlKey(totalSizeBucket, requestType, totalTimeBucketName)];
    }
    for (const auto& [key, count]: inflight) {
        allStat[key] = count;
    }

    json["stat"] = std::move(allStat);

    NJson::WriteJson(&out, &json);
    return out.Str();
}

TVector<TString> TRequestsTimeTracker::GetSizeBuckets() const
{
    return MakeSizeNames();
}

TVector<TString> TRequestsTimeTracker::GetTimeBuckets() const
{
    TVector<TString> result = TRequestUsTimeBuckets::MakeNames();
    result.push_back("Total");
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
