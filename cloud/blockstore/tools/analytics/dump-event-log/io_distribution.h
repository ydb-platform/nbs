#pragma once

#include <cloud/blockstore/tools/analytics/dump-event-log/profile_log_event_handler.h>

#include <library/cpp/json/writer/json_value.h>

#include <util/generic/bitmap.h>
#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IDistributionCalculator
{
    virtual ~IDistributionCalculator() = default;

    [[nodiscard]] virtual bool HaveMeaning() const = 0;
    [[nodiscard]] virtual TString GetName() const = 0;
    virtual void Apply(ui32 requestType, TBlockRange64 blockRange) = 0;
    [[nodiscard]] virtual NJson::TJsonValue Dump() = 0;
};

using IDistributionCalculatorPtr = std::unique_ptr<IDistributionCalculator>;

////////////////////////////////////////////////////////////////////////////////

class TIODistributionStat: public IProfileLogEventHandler
{
    class TVolumeStat
    {
        const TDiskInfo DiskInfo;
        TVector<IDistributionCalculatorPtr> Distributions;

    public:
        explicit TVolumeStat(const TDiskInfo& diskInfo);

        void ProcessRequest(
            const TDiskInfo& diskInfo,
            const TTimeData& timeData,
            ui32 requestType,
            TBlockRange64 blockRange,
            const TReplicaChecksums& replicaChecksums,
            const TInflightData& inflightData);

        [[nodiscard]] NJson::TJsonValue Dump() const;
    };

    const TString Filename;

    TMap<TString, TVolumeStat> Volumes;

public:
    explicit TIODistributionStat(const TString& filename);

    void ProcessRequest(
        const TDiskInfo& diskInfo,
        const TTimeData& timeData,
        ui32 requestType,
        TBlockRange64 blockRange,
        const TReplicaChecksums& replicaChecksums,
        const TInflightData& inflightData) override;

    void Finish() override;

private:
    TVolumeStat& GetVolumeStat(const TDiskInfo& diskInfo);
};

}   // namespace NCloud::NBlockStore
