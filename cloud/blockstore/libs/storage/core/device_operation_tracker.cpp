#include "device_operation_tracker.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/datetime/cputimer.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString TDeviceOperationTracker::TKey::GetHtmlPrefix() const
{
    TStringBuilder builder;

    builder << RequestType << "_" << DeviceUUID;

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

ui64 TDeviceOperationTracker::THash::operator()(const TKey& key) const
{
    return MultiHash(
        key.RequestType,
        key.DeviceUUID,
        key.AgentId,
        static_cast<size_t>(key.Status));
}

////////////////////////////////////////////////////////////////////////////////

void TDeviceOperationTracker::RebuildFromDeviceInfos()
{
    DeviceToAgent.clear();
    for (const auto& deviceInfo: DeviceInfos) {
        DeviceToAgent[deviceInfo.DeviceUUID] = deviceInfo.AgentId;
    }

    Histograms.clear();
    Inflight.clear();

    const ERequestType requestTypes[] = {
        ERequestType::Read,
        ERequestType::Write,
        ERequestType::Zero,
        ERequestType::Checksum};

    for (const auto& requestType: requestTypes) {
        for (const auto& deviceInfo: DeviceInfos) {
            Histograms.try_emplace(TKey{
                .RequestType = requestType,
                .DeviceUUID = deviceInfo.DeviceUUID,
                .AgentId = deviceInfo.AgentId,
                .Status = EStatus::Finished});
        }

        Histograms.try_emplace(TKey{
            .RequestType = requestType,
            .DeviceUUID = "Total",
            .AgentId = "Total",
            .Status = EStatus::Finished});
    }
}

TDeviceOperationTracker::TDeviceOperationTracker(
    TVector<TDeviceInfo> deviceInfos)
    : DeviceInfos(std::move(deviceInfos))
{
    RebuildFromDeviceInfos();
}

void TDeviceOperationTracker::OnStarted(
    TOperationId operationId,
    const TString& deviceUUID,
    ERequestType requestType,
    ui64 startTime)
{
    auto it = DeviceToAgent.find(deviceUUID);
    if (it == DeviceToAgent.end()) {
        return;
    }

    const TString& agentId = it->second;

    Inflight.emplace(
        operationId,
        TOperationInFlight{
            .StartTime = startTime,
            .RequestType = requestType,
            .DeviceUUID = deviceUUID,
            .AgentId = agentId});
}

void TDeviceOperationTracker::OnFinished(
    TOperationId operationId,
    ui64 finishTime)
{
    auto it = Inflight.find(operationId);
    if (it == Inflight.end()) {
        return;
    }

    auto& operation = it->second;
    auto duration = CyclesToDurationSafe(finishTime - operation.StartTime);

    TKey specificKey{
        .RequestType = operation.RequestType,
        .DeviceUUID = std::move(operation.DeviceUUID),
        .AgentId = std::move(operation.AgentId),
        .Status = EStatus::Finished};

    Histograms[specificKey].Increment(duration.MicroSeconds());

    Inflight.erase(it);
}

TString TDeviceOperationTracker::GetStatJson(ui64 nowCycles) const
{
    NJson::TJsonValue allStat(NJson::EJsonValueType::JSON_MAP);
    const auto times = TRequestUsTimeBuckets::MakeNames();

    for (const auto& [key, histogram]: Histograms) {
        if (key.Status != EStatus::Finished) {
            continue;
        }

        size_t total = 0;
        const auto htmlPrefix = key.GetHtmlPrefix();

        for (size_t i = 0; i < TRequestUsTimeBuckets::BUCKETS_COUNT; ++i) {
            total += histogram.Buckets[i];
            allStat[htmlPrefix + times[i]] = ToString(histogram.Buckets[i]);
        }

        allStat[htmlPrefix + "Total"] = ToString(total);
    }

    auto getInflightHtmlKey = [](ERequestType requestType,
                                 const TString& deviceUUID,
                                 TStringBuf timeBucketName)
    {
        auto key = TKey{
            .RequestType = requestType,
            .DeviceUUID = deviceUUID,
            .AgentId = "",
            .Status = EStatus::Inflight};
        return key.GetHtmlPrefix() + timeBucketName;
    };

    THashMap<TString, size_t> inflight;

    for (const auto& [operationId, operation]: Inflight) {
        const auto& timeBucketName = TRequestUsTimeBuckets::GetBucketName(
            CyclesToDurationSafe(nowCycles - operation.StartTime));

        ++inflight[getInflightHtmlKey(
            operation.RequestType,
            operation.DeviceUUID,
            timeBucketName)];
        ++inflight[getInflightHtmlKey(
            operation.RequestType,
            operation.DeviceUUID,
            "Total")];
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

void TDeviceOperationTracker::UpdateDevices(TVector<TDeviceInfo> deviceInfos)
{
    DeviceInfos = std::move(deviceInfos);
    RebuildFromDeviceInfos();
}

auto TDeviceOperationTracker::GetTimeBuckets() const -> TVector<TBucketInfo>
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
        .Key = "Total",
        .Description = "Total",
        .Tooltip = "Total operations"});

    return result;
}

TVector<TDeviceOperationTracker::TDeviceInfo>
TDeviceOperationTracker::GetDeviceInfos() const
{
    return DeviceInfos;
}

void TDeviceOperationTracker::ResetStats()
{
    for (auto& [key, histogram]: Histograms) {
        if (key.Status == EStatus::Finished) {
            histogram.Reset();
        }
    }
}

const TDeviceOperationTracker::TInflightMap&
TDeviceOperationTracker::GetInflightOperations() const
{
    return Inflight;
}

}   // namespace NCloud::NBlockStore::NStorage
