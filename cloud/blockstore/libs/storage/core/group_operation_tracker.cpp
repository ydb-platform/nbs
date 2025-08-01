#include "group_operation_tracker.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString TGroupOperationTimeTracker::TKey::GetHtmlPrefix() const
{
    TStringBuilder builder;

    builder << OperationName << "_" << GroupId;

    switch (Status) {
        case EStatus::Finished: {
            builder << "_finished_";
            break;
        }
        case EStatus::Inflight: {
            builder << "_inflight_";
            break;
        }
    }
    return builder;
}

ui64 TGroupOperationTimeTracker::THash::operator()(const TKey& key) const
{
    return MultiHash(
        static_cast<size_t>(key.Status),
        key.OperationName,
        key.GroupId);
}

////////////////////////////////////////////////////////////////////////////////

void TGroupOperationTimeTracker::OnStarted(
    ui64 operationId,
    ui32 groupId,
    EGroupOperationType operationType,
    ui64 startTime)
{
    TString operationName;

    switch (operationType) {
        case EGroupOperationType::Read: {
            operationName = "Read";
            break;
        }
        case EGroupOperationType::Write: {
            operationName = "Write";
            break;
        }
    }

    TKey key{
        .OperationName = operationName,
        .GroupId = groupId,
        .Status = EStatus::Inflight};

    if (!Histograms.contains(key)) {
        Histograms[key];
    }

    Inflight.emplace(
        operationId,
        TOperationInflight{
            .StartTime = startTime,
            .OperationName = operationName,
            .GroupId = groupId});
}

void TGroupOperationTimeTracker::OnFinished(ui64 operationId, ui64 finishTime)
{
    auto it = Inflight.find(operationId);
    if (it == Inflight.end()) {
        return;
    }

    auto& operation = it->second;

    auto duration = CyclesToDurationSafe(finishTime - operation.StartTime);

    TKey key{
        .OperationName = std::move(operation.OperationName),
        .GroupId = operation.GroupId,
        .Status = EStatus::Finished};
    Histograms[key].Increment(duration.MicroSeconds());

    key.OperationName = "Total";
    key.GroupId = 0;
    Histograms[key].Increment(duration.MicroSeconds());

    Inflight.erase(operationId);
}

TString TGroupOperationTimeTracker::GetStatJson(ui64 nowCycles) const
{
    NJson::TJsonValue allStat(NJson::EJsonValueType::JSON_MAP);

    const auto times = TRequestUsTimeBuckets::MakeNames();

    // Build finished Operation counters.
    for (const auto& [key, histogram]: Histograms) {
        size_t total = 0;
        const auto htmlPrefix = key.GetHtmlPrefix();
        for (size_t i = 0; i < TRequestUsTimeBuckets::BUCKETS_COUNT; ++i) {
            allStat[htmlPrefix + times[i]] =
                (histogram.Buckets[i] ? ToString(histogram.Buckets[i]) : "");
            total += histogram.Buckets[i];
        }
        allStat[htmlPrefix + "Total"] = ToString(total);
    }

    // Build inflight Operation counters
    auto getHtmlKey = [](const TString& operationName,
                         ui32 groupId,
                         TStringBuf timeBucketName)
    {
        auto key = TKey{
            .OperationName = operationName,
            .GroupId = groupId,
            .Status = EStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [OperationId, Operation]: Inflight) {
        const auto& timeBucketName = GetTimeBucketName(
            CyclesToDurationSafe(nowCycles - Operation.StartTime));

        ++inflight[getHtmlKey(
            Operation.OperationName,
            Operation.GroupId,
            timeBucketName)];
        ++inflight
            [getHtmlKey(Operation.OperationName, Operation.GroupId, "Total")];
        ++inflight[getHtmlKey("Total", 0, timeBucketName)];
        ++inflight[getHtmlKey("Total", 0, "Total")];
    }
    for (const auto& [key, count]: inflight) {
        allStat[key] = "+ " + ToString(count);
    }

    NJson::TJsonValue json;
    json["stat"] = std::move(allStat);

    TStringStream out;
    NJson::WriteJson(&out, &json);
    return out.Str();
}

TVector<TGroupOperationTimeTracker::TBucketInfo>
TGroupOperationTimeTracker::GetTimeBuckets() const
{
    TVector<TBucketInfo> result;
    TDuration last;
    for (const auto& time: TRequestUsTimeBuckets::MakeNames()) {
        const auto us = TryFromString<ui64>(time);

        TBucketInfo bucket{
            .OperationName = {},
            .Key = time,
            .Description =
                us ? FormatDuration(TDuration::MicroSeconds(*us)) : time,
            .Tooltip =
                "[" + FormatDuration(last) + ".." + bucket.Description + "]"};

        last = TDuration::MicroSeconds(us.GetOrElse(0));
        result.push_back(std::move(bucket));
    }
    result.push_back(TBucketInfo{
        .OperationName = {},
        .Key = "Total",
        .Description = "Total",
        .Tooltip = ""});
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
