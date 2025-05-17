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

    builder << ToString(TransactionType);

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
    return IntHash(static_cast<size_t>(key.Status)) +
           IntHash(static_cast<size_t>(key.TransactionType) << 2);
}

bool TTransactionTimeTracker::TEqual::operator()(
    const TKey& lhs,
    const TKey& rhs) const
{
    auto makeTie = [](const TKey& val)
    {
        return std::tie(val.TransactionType, val.Status);
    };
    return makeTie(lhs) == makeTie(rhs);
}

////////////////////////////////////////////////////////////////////////////////

TTransactionTimeTracker::TTransactionTimeTracker()
{
    for (size_t i = 0; i <= static_cast<size_t>(ETransactionType::Total); ++i) {
        auto key = TKey{
            .TransactionType = static_cast<ETransactionType>(i),
            .Status = EStatus::Inflight};
        Histograms[key];
    }
}

void TTransactionTimeTracker::OnStarted(
    ETransactionType transactionType,
    ui64 transactionId,
    ui64 startTime)
{
    Inflight.emplace(
        transactionId,
        TTransactionInflight{
            .StartTime = startTime,
            .TransactionType = transactionType});
}

void TTransactionTimeTracker::OnFinished(ui64 transactionId, ui64 finishTime)
{
    TTransactionInflight transaction;
    {
        auto it = Inflight.find(transactionId);
        if (it == Inflight.end()) {
            return;
        }
        transaction = it->second;
        Inflight.erase(transactionId);
    }

    auto duration = CyclesToDurationSafe(finishTime - transaction.StartTime);

    TKey key{
        .TransactionType = transaction.TransactionType,
        .Status = EStatus::Finished};
    Histograms[key].Increment(duration.MicroSeconds());

    key.TransactionType = ETransactionType::Total;
    Histograms[key].Increment(duration.MicroSeconds());
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
        [](ETransactionType transactionType, TStringBuf timeBucketName)
    {
        auto key = TKey{
            .TransactionType = transactionType,
            .Status = EStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    TMap<TString, size_t> inflight;

    for (const auto& [transactionId, transaction]: Inflight) {
        const auto& timeBucketName = GetTimeBucketName(
            CyclesToDurationSafe(nowCycles - transaction.StartTime));

        ++inflight[getHtmlKey(transaction.TransactionType, timeBucketName)];
        ++inflight[getHtmlKey(transaction.TransactionType, "Total")];
        ++inflight[getHtmlKey(ETransactionType::Total, timeBucketName)];
        ++inflight[getHtmlKey(ETransactionType::Total, "Total")];
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
TTransactionTimeTracker::GetTransactionBuckets()
{
    TVector<TBucketInfo> result;

    for (size_t i = 0; i <= static_cast<size_t>(ETransactionType::Total); ++i) {
        TBucketInfo bucket{
            .TransactionType = static_cast<ETransactionType>(i),
            .Key = ToString(static_cast<ETransactionType>(i)),
            .Description = ToString(static_cast<ETransactionType>(i)),
            .Tooltip = ""};
        result.push_back(std::move(bucket));
    }
    return result;
}

TVector<TTransactionTimeTracker::TBucketInfo>
TTransactionTimeTracker::GetTimeBuckets()
{
    TVector<TBucketInfo> result;
    TDuration last;
    for (const auto& time: TRequestUsTimeBuckets::MakeNames()) {
        const auto us = TryFromString<ui64>(time);

        TBucketInfo bucket{
            .TransactionType = ETransactionType::None,
            .Key = time,
            .Description =
                us ? FormatDuration(TDuration::MicroSeconds(*us)) : time,
            .Tooltip =
                "[" + FormatDuration(last) + ".." + bucket.Description + "]"};

        last = TDuration::MicroSeconds(us.GetOrElse(0));
        result.push_back(std::move(bucket));
    }
    result.push_back(TBucketInfo{
        .TransactionType = ETransactionType::None,
        .Key = "Total",
        .Description = "Total",
        .Tooltip = ""});
    return result;
}

}   // namespace NCloud::NBlockStore::NStorage
