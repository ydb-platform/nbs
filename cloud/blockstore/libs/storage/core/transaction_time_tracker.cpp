#include "transaction_time_tracker.h"

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

TString TTransactionTimeTracker::TKey::GetHtmlPrefix() const
{
    TStringBuilder builder;

    builder << ToString(TransactionName);

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

ui64 TTransactionTimeTracker::THash::operator()(const TKey& key) const
{
    return MultiHash(static_cast<size_t>(key.Status), key.TransactionName);
}

////////////////////////////////////////////////////////////////////////////////

TTransactionTimeTracker::TTransactionTimeTracker(
        std::span<const TString> transactionTypes)
    : TransactionTypes(transactionTypes.begin(), transactionTypes.end())
{
    for (const auto& transaction: TransactionTypes) {
        auto key =
            TKey{.TransactionName = transaction, .Status = EStatus::Inflight};
        Histograms[key];
    }
}

void TTransactionTimeTracker::OnStarted(
    ui64 transactionId,
    TString transactionName,
    ui64 startTime)
{
    Inflight.emplace(
        transactionId,
        TTransactionInflight{
            .StartTime = startTime,
            .TransactionName = std::move(transactionName)});
}

void TTransactionTimeTracker::OnFinished(ui64 transactionId, ui64 finishTime)
{
    auto it = Inflight.find(transactionId);
    if (it == Inflight.end()) {
        return;
    }

    auto& transaction = it->second;

    auto duration = CyclesToDurationSafe(finishTime - transaction.StartTime);

    TKey key{
        .TransactionName = std::move(transaction.TransactionName),
        .Status = EStatus::Finished};
    Histograms[key].Increment(duration.MicroSeconds());

    key.TransactionName = "Total";
    Histograms[key].Increment(duration.MicroSeconds());

    Inflight.erase(transactionId);
}

TString TTransactionTimeTracker::GetStatJson(ui64 nowCycles) const
{
    NJson::TJsonValue allStat(NJson::EJsonValueType::JSON_MAP);

    const auto times = TRequestUsTimeBuckets::MakeNames();

    // Build finished transaction counters.
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

    // Build inflight transaction counters
    auto getHtmlKey =
        [](const TString& transactionName, TStringBuf timeBucketName)
    {
        auto key = TKey{
            .TransactionName = transactionName,
            .Status = EStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [transactionId, transaction]: Inflight) {
        const auto& timeBucketName = GetTimeBucketName(
            CyclesToDurationSafe(nowCycles - transaction.StartTime));

        ++inflight[getHtmlKey(transaction.TransactionName, timeBucketName)];
        ++inflight[getHtmlKey(transaction.TransactionName, "Total")];
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

TVector<TTransactionTimeTracker::TBucketInfo>
TTransactionTimeTracker::GetTransactionBuckets() const
{
    TVector<TBucketInfo> result;

    for (const auto& transaction: TransactionTypes) {
        TBucketInfo bucket{
            .TransactionName = transaction,
            .Key = transaction,
            .Description = transaction,
            .Tooltip = ""};
        result.push_back(std::move(bucket));
    }
    return result;
}

TVector<TTransactionTimeTracker::TBucketInfo>
TTransactionTimeTracker::GetTimeBuckets() const
{
    TVector<TBucketInfo> result;
    TDuration last;
    for (const auto& time: TRequestUsTimeBuckets::MakeNames()) {
        const auto us = TryFromString<ui64>(time);

        TBucketInfo bucket{
            .TransactionName = {},
            .Key = time,
            .Description =
                us ? FormatDuration(TDuration::MicroSeconds(*us)) : time,
            .Tooltip =
                "[" + FormatDuration(last) + ".." + bucket.Description + "]"};

        last = TDuration::MicroSeconds(us.GetOrElse(0));
        result.push_back(std::move(bucket));
    }
    result.push_back(TBucketInfo{
        .TransactionName = {},
        .Key = "Total",
        .Description = "Total",
        .Tooltip = ""});
    return result;
}

// void TTransactionTimeTracker::Init(std::span<const TString> transactionTypes)
// {
//     TransactionTypes.assign(transactionTypes.begin(), transactionTypes.end());
//     Histograms.clear();
//     for (const auto& transaction: TransactionTypes) {
//         auto key =
//             TKey{.TransactionName = transaction, .Status = EStatus::Inflight};
//         Histograms[key];
//     }
// }

}   // namespace NCloud::NBlockStore::NStorage
