// #include "group_request_tracker.h"

// #include <cloud/storage/core/libs/common/format.h>

// #include <library/cpp/json/json_writer.h>
// #include <library/cpp/json/writer/json_value.h>

// #include <util/datetime/cputimer.h>
// #include <util/string/builder.h>

// namespace NCloud::NBlockStore::NStorage {

// void TGroupOperationTimeTracker::TTimeHistogram::Increment(ui64 micros)
// {
//     size_t idx = GetBucketIndex(micros);
//     ++Buckets[idx];
// }

// size_t TGroupOperationTimeTracker::TTimeHistogram::GetBucketIndex(
//     ui64 micros) const
// {
//     const auto& buckets = NCloud::TRequestUsTimeBuckets::Buckets;
//     auto it = std::lower_bound(buckets.begin(), buckets.end(), (double)micros);
//     if (it == buckets.end()) {
//         return buckets.size() - 1;
//     }
//     return std::distance(buckets.begin(), it);
// }

// TMaybe<TGroupOperationTimeTracker::TOperationKey>
// TGroupOperationTimeTracker::ParseTransactionName(const TString& name)
// {
//     // Ожидается формат: Read_XXX или Write_XXX
//     TVector<TStringBuf> parts =
//         StringSplitter(name).Split('_').ToList<TStringBuf>();
//     if (parts.size() != 2) {
//         return {};
//     }

//     EOperationType op;
//     if (parts[0] == "Read") {
//         op = EOperationType::Read;
//     } else if (parts[0] == "Write") {
//         op = EOperationType::Write;
//     } else {
//         return {};
//     }

//     ui64 groupId = 0;
//     if (!TryFromString(parts[1], groupId)) {
//         return {};
//     }

//     return TOperationKey{op, groupId};
// }

// void TGroupOperationTimeTracker::OnStarted(
//     ui64 transactionId,
//     TString transactionName,
//     ui64 startTime)
// {
//     auto maybeKey = ParseTransactionName(transactionName);
//     if (!maybeKey) {
//         // Игнорируем транзакции, которые не подходят под шаблон
//         return;
//     }

//     Inflight.emplace(
//         transactionId,
//         TInflightOperation{.StartTime = startTime, .Key = *maybeKey});
// }

// void TGroupOperationTimeTracker::OnFinished(ui64 transactionId, ui64 finishTime)
// {
//     auto it = Inflight.find(transactionId);
//     if (it == Inflight.end()) {
//         return;
//     }

//     auto& inflightOp = it->second;

//     auto duration = CyclesToDurationSafe(finishTime - inflightOp.StartTime);

//     Histograms[inflightOp.Key].Increment(duration.MicroSeconds());

//     Inflight.erase(it);
// }

// TString TGroupOperationTimeTracker::OperationTypeToString(EOperationType op)
// {
//     switch (op) {
//         case EOperationType::Read:
//             return "Read";
//         case EOperationType::Write:
//             return "Write";
//         default:
//             return "Unknown";
//     }
// }

// TString TGroupOperationTimeTracker::GetStatJson(ui64 nowCycles) const
// {
//     NJson::TJsonValue root(NJson::EJsonValueType::JSON_MAP);
//     NJson::TJsonValue finishedArr(NJson::EJsonValueType::JSON_ARRAY);

//     for (const auto& [key, histo]: Histograms) {
//         NJson::TJsonValue entry(NJson::EJsonValueType::JSON_MAP);
//         entry["Operation"] = OperationTypeToString(key.Operation);
//         entry["GroupId"] = ToString(key.GroupId);

//         NJson::TJsonValue bucketsArr(NJson::EJsonValueType::JSON_ARRAY);
//         for (size_t i = 0; i < TTimeHistogram::BUCKETS_COUNT; ++i) {
//             bucketsArr.AppendValue(ToString(histo.Buckets[i]));
//         }
//         entry["Buckets"] = std::move(bucketsArr);

//         finishedArr.AppendValue(std::move(entry));
//     }
//     root["Finished"] = std::move(finishedArr);

//     NJson::TJsonValue inflightArr(NJson::EJsonValueType::JSON_ARRAY);
//     for (const auto& [txId, inflightOp]: Inflight) {
//         NJson::TJsonValue entry(NJson::EJsonValueType::JSON_MAP);
//         entry["TransactionId"] = ToString(txId);
//         entry["Operation"] = OperationTypeToString(inflightOp.Key.Operation);
//         entry["GroupId"] = ToString(inflightOp.Key.GroupId);

//         auto inflightDurationUs =
//             CyclesToDurationSafe(nowCycles - inflightOp.StartTime)
//                 .MicroSeconds();
//         entry["InflightDurationUs"] = ToString(inflightDurationUs);

//         inflightArr.AppendValue(std::move(entry));
//     }
//     root["Inflight"] = std::move(inflightArr);

//     TStringStream ss;
//     NJson::WriteJson(&ss, &root);
//     return ss.Str();
// }

// void TGroupOperationTimeTracker::GetAllOperationKeys(TVector<TOperationKey>& keys) const {
//     keys.clear();
//     keys.reserve(Histograms.size());
//     for (const auto& [key, _] : Histograms) {
//         keys.push_back(key);
//     }
// }

// ui64 TGroupOperationTimeTracker::GetHistogramCount(const TOperationKey& key, const TString& timeBucket) const {
//     auto it = Histograms.find(key);
//     if (it == Histograms.end()) {
//         return 0;
//     }
//     const auto& histogram = it->second;

//     const auto bucketNames = TRequestUsTimeBuckets::MakeNames();

//     auto itBucket = std::find(bucketNames.begin(), bucketNames.end(), timeBucket);
//     if (itBucket == bucketNames.end()) {
//         return 0;
//     }
//     size_t idx = std::distance(bucketNames.begin(), itBucket);
//     if (idx >= TTimeHistogram::BUCKETS_COUNT) {
//         return 0;
//     }

//     return histogram.Buckets[idx];
// }

// }   // namespace NCloud::NBlockStore::NStorage
