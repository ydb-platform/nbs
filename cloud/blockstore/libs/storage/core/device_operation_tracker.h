#pragma once

#include <cloud/blockstore/libs/storage/core/histogram.h>
#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDeviceOperationTracker
{
public:
    enum class EStatus
    {
        Inflight,
        Finished,
    };

    enum class ERequestType
    {
        Read,
        Write,
        Zero,
        Checksum
    };

    struct TBucketInfo
    {
        TString OperationName;
        TString Key;
        TString Description;
        TString Tooltip;
    };

    struct TOperationInflight
    {
        ui64 StartTime = 0;
        TString RequestType;
        TString DeviceUUID;
        TString AgentId;
    };

    struct TDeviceInfo
    {
        TString DeviceUUID;
        TString AgentId;
    };

    using TInflightMap = THashMap<ui64, TOperationInflight>;

private:
    struct TTimeHistogram: public THistogram<TRequestUsTimeBuckets>
    {
        TTimeHistogram()
            : THistogram<TRequestUsTimeBuckets>(
                  EHistogramCounterOption::ReportSingleCounter)
        {}
    };

    struct TKey
    {
        TString RequestType;
        TString DeviceUUID;
        TString AgentId;
        EStatus Status = EStatus::Inflight;

        [[nodiscard]] TString GetHtmlPrefix() const;

        bool operator==(const TKey& rhs) const = default;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    TVector<TDeviceInfo> DeviceInfos;
    THashMap<TString, TString> DeviceToAgent;

    TInflightMap Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    TDeviceOperationTracker() = default;
    explicit TDeviceOperationTracker(TVector<TDeviceInfo> deviceInfos);

    void OnStarted(
        ui64 operationId,
        const TString& deviceUUID,
        ERequestType requestType,
        ui64 startTime);

    void OnFinished(ui64 operationId, ui64 finishTime);

    void UpdateDevices(std::span<const TDeviceInfo> deviceInfos);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;
    [[nodiscard]] TVector<TDeviceInfo> GetDeviceInfos() const;

    void ResetStats();

    [[nodiscard]] const TInflightMap& GetInflightOperations() const;
};

}   // namespace NCloud::NBlockStore::NStorage
