#include "device_operation_tracker.h"

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

TString TDeviceOperationTracker::ToString(ERequestType type)
{
    switch (type) {
        case ERequestType::Read:
            return "Read";
        case ERequestType::Write:
            return "Write";
        case ERequestType::Zero:
            return "Zero";
        case ERequestType::Checksum:
            return "Checksum";
    }
    return "Unknown";
}

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

TDeviceOperationTracker::TDeviceOperationTracker(
    std::span<const TDeviceInfo> deviceInfos)
    : DeviceInfos(deviceInfos.begin(), deviceInfos.end())
{
    for (const auto& deviceInfo: DeviceInfos) {
        DeviceToAgent[deviceInfo.DeviceUUID] = deviceInfo.AgentId;
    }

    const TVector<ERequestType> requestTypes = {
        ERequestType::Read,
        ERequestType::Write,
        ERequestType::Zero,
        ERequestType::Checksum};

    for (const auto& requestType: requestTypes) {
        const TString requestTypeStr = ToString(requestType);

        for (const auto& deviceInfo: DeviceInfos) {
            auto key = TKey{
                .RequestType = requestTypeStr,
                .DeviceUUID = deviceInfo.DeviceUUID,
                .AgentId = deviceInfo.AgentId,
                .Status = EStatus::Finished};
            Histograms[key];
        }
    }
}

void TDeviceOperationTracker::OnStarted(
    ui64 operationId,
    const TString& deviceUUID,
    ERequestType requestType,
    ui64 startTime)
{
    TString agentId = "";
    auto it = DeviceToAgent.find(deviceUUID);
    if (it != DeviceToAgent.end()) {
        agentId = it->second;
    } else {
        return;
    }

    Inflight.emplace(
        operationId,
        TOperationInflight{
            .StartTime = startTime,
            .RequestType = ToString(requestType),
            .DeviceUUID = deviceUUID,
            .AgentId = std::move(agentId)});
}

void TDeviceOperationTracker::OnFinished(ui64 operationId, ui64 finishTime)
{
    auto it = Inflight.find(operationId);
    if (it == Inflight.end()) {
        return;
    }

    auto& operation = it->second;
    auto duration = CyclesToDurationSafe(finishTime - operation.StartTime);

    TKey specificKey{
        .RequestType = operation.RequestType,
        .DeviceUUID = operation.DeviceUUID,
        .AgentId = operation.AgentId,
        .Status = EStatus::Finished};
    Histograms[specificKey].Increment(duration.MicroSeconds());

    Inflight.erase(operationId);
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
            allStat[htmlPrefix + times[i]] = ::ToString(histogram.Buckets[i]);
        }

        allStat[htmlPrefix + "Total"] = ::ToString(total);
    }

    auto getInflightHtmlKey = [](const TString& requestType,
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

    TMap<TString, size_t> inflight;

    for (const auto& [operationId, operation]: Inflight) {
        const auto& timeBucketName = GetTimeBucketName(
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
        allStat[key] = "+ " + ::ToString(count);
    }

    NJson::TJsonValue json;
    json["stat"] = std::move(allStat);

    TStringStream out;
    NJson::WriteJson(&out, &json);
    return out.Str();
}

void TDeviceOperationTracker::UpdateDevices(
    std::span<const TDeviceInfo> deviceInfos)
{
    DeviceInfos.assign(deviceInfos.begin(), deviceInfos.end());

    DeviceToAgent.clear();
    for (const auto& deviceInfo: DeviceInfos) {
        DeviceToAgent[deviceInfo.DeviceUUID] = deviceInfo.AgentId;
    }

    Histograms.clear();
    Inflight.clear();

    const TVector<ERequestType> requestTypes = {
        ERequestType::Read,
        ERequestType::Write,
        ERequestType::Zero,
        ERequestType::Checksum};

    for (const auto& requestType: requestTypes) {
        const TString requestTypeStr = ToString(requestType);

        for (const auto& deviceInfo: DeviceInfos) {
            auto key = TKey{
                .RequestType = requestTypeStr,
                .DeviceUUID = deviceInfo.DeviceUUID,
                .AgentId = deviceInfo.AgentId,
                .Status = EStatus::Finished};
            Histograms[key];
        }

        auto totalKey = TKey{
            .RequestType = requestTypeStr,
            .DeviceUUID = "Total",
            .AgentId = "Total",
            .Status = EStatus::Finished};
        Histograms[totalKey];
    }
}

TVector<TDeviceOperationTracker::TBucketInfo>
TDeviceOperationTracker::GetTimeBuckets() const
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
