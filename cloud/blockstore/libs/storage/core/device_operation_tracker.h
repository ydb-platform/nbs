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
        TString Key;
        TString Description;
        TString Tooltip;
    };

    struct TOperationInFlight
    {
        ui64 StartTime = 0;
        ERequestType RequestType;
        TString AgentId;
    };

    using TOperationId = ui64;
    using TInflightMap = THashMap<TOperationId, TOperationInFlight>;

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
        ERequestType RequestType;
        TString AgentId;
        EStatus Status = EStatus::Inflight;

        [[nodiscard]] TString GetHtmlPrefix() const;

        bool operator==(const TKey& rhs) const = default;
    };

    struct THash
    {
        ui64 operator()(const TKey& key) const;
    };

    TSet<TString> Agents;

    TInflightMap Inflight;
    THashMap<TKey, TTimeHistogram, THash> Histograms;

public:
    TDeviceOperationTracker() = default;

    void OnStarted(
        TOperationId operationId,
        const TString& agentId,
        ERequestType requestType,
        ui64 startTime);

    static void UpdateTrackingFrequency(ui32 trackingFrequency);
    static ui64 GenerateId(ui64 identifiersToReserve);

    void OnFinished(TOperationId operationId, ui64 finishTime);

    void UpdateAgents(TSet<TString> agents);

    [[nodiscard]] TString GetStatJson(ui64 nowCycles) const;
    [[nodiscard]] TVector<TBucketInfo> GetTimeBuckets() const;
    [[nodiscard]] TVector<TBucketInfo> GetAgents() const;

    void ResetStats();

    [[nodiscard]] const TInflightMap& GetInflightOperations() const;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
