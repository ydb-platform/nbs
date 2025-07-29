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

    builder << ToString(OperationName);

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
    return MultiHash(static_cast<size_t>(key.Status), key.OperationName);
}

////////////////////////////////////////////////////////////////////////////////

void TGroupOperationTimeTracker::OnStarted(
    ui64 OperationId,
    ui32 groupId,
    EGroupOperationType operationType,
    ui64 startTime)
{
    TStringBuilder OperationName;

    switch (operationType) {
        case EGroupOperationType::Read: {
            OperationName << "Read_";
            break;
        }
        case EGroupOperationType::Write: {
            OperationName << "Write_";
            break;
        }
    }
    OperationName << groupId;
    auto key =
        TKey{.OperationName = OperationName, .Status = EStatus::Inflight};
    if (!Histograms.contains(key)) {
        Histograms[key];
    }
    Inflight.emplace(
        OperationId,
        TOperationInflight{
            .StartTime = startTime,
            .OperationName = std::move(OperationName)});
}

void TGroupOperationTimeTracker::OnFinished(ui64 OperationId, ui64 finishTime)
{
    auto it = Inflight.find(OperationId);
    if (it == Inflight.end()) {
        return;
    }

    auto& Operation = it->second;

    auto duration = CyclesToDurationSafe(finishTime - Operation.StartTime);

    TKey key{
        .OperationName = std::move(Operation.OperationName),
        .Status = EStatus::Finished};
    Histograms[key].Increment(duration.MicroSeconds());

    key.OperationName = "Total";
    Histograms[key].Increment(duration.MicroSeconds());

    Inflight.erase(OperationId);
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
    auto getHtmlKey =
        [](const TString& OperationName, TStringBuf timeBucketName)
    {
        auto key =
            TKey{.OperationName = OperationName, .Status = EStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [OperationId, Operation]: Inflight) {
        const auto& timeBucketName = GetTimeBucketName(
            CyclesToDurationSafe(nowCycles - Operation.StartTime));

        ++inflight[getHtmlKey(Operation.OperationName, timeBucketName)];
        ++inflight[getHtmlKey(Operation.OperationName, "Total")];
        ++inflight[getHtmlKey("Total", timeBucketName)];
        ++inflight[getHtmlKey("Total", "Total")];
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

}   // namespace NCloud::NBlockStore::NStorage
