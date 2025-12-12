#include "io_distribution.h"

#include <cloud/blockstore/libs/service/request.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/writer/json_value.h>

#include <util/generic/algorithm.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
namespace NCloud::NBlockStore {

namespace {

///////////////////////////////////////////////////////////////////////////////

struct TStripeConfig
{
    TString Name;
    const ui64 StripeSize = 0;
    const ui64 StripeGroupCount = 1;
    TSet<EBlockStoreRequest> RequestTypes;

    [[nodiscard]] NJson::TJsonValue Dump() const
    {
        NJson::TJsonValue config;

        config["Name"] = Name;
        config["StripeSize"] = StripeSize;
        config["StripeGroupCount"] = StripeGroupCount;

        NJson::TJsonValue requestTypes;
        for (auto requestType: RequestTypes) {
            requestTypes.AppendValue(GetBlockStoreRequestName(requestType));
        }
        config["RequestTypes"] = requestTypes;

        return config;
    }
};

class TIODistribution: public IDistributionCalculator
{
    const TDiskInfo& DiskInfo;
    const TStripeConfig StripeConfig;
    const ui64 BlocksPerStripe{StripeConfig.StripeSize / DiskInfo.BlockSize};
    const ui64 StripeCount{DiskInfo.BlockCount / BlocksPerStripe};
    const ui64 StartOffset{DiskInfo.RangeWithIO.Start / BlocksPerStripe};

    ui64 RequestCount = 0;
    ui64 SplittedRequestCount = 0;
    ui64 TotalIoBlockCount = 0;
    TVector<ui32> BlockCountPerStripe;
    ui32 MinBlocksPerRequest = 0;
    ui32 MaxBlocksPerRequest = 0;

public:
    TIODistribution(TStripeConfig stripeConfig, const TDiskInfo& diskInfo)
        : DiskInfo(diskInfo)
        , StripeConfig(std::move(stripeConfig))
    {
        ui32 leftUsedStripe = DiskInfo.RangeWithIO.Start / BlocksPerStripe;
        ui32 rightUsedStripe = DiskInfo.RangeWithIO.End / BlocksPerStripe;

        BlockCountPerStripe.resize(rightUsedStripe - leftUsedStripe + 1);
    }

    TString GetName() const override
    {
        return StripeConfig.Name;
    }

    void Apply(ui32 requestType, TBlockRange64 blockRange) override
    {
        const bool shouldHandle =
            StripeConfig.RequestTypes.empty() ||
            StripeConfig.RequestTypes.contains(
                static_cast<EBlockStoreRequest>(requestType));

        if (!shouldHandle) {
            return;
        }

        TotalIoBlockCount += blockRange.Size();
        MinBlocksPerRequest =
            MinBlocksPerRequest
                ? Min<ui32>(MinBlocksPerRequest, blockRange.Size())
                : blockRange.Size();
        MaxBlocksPerRequest = Max<ui32>(MaxBlocksPerRequest, blockRange.Size());

        auto startStripe = blockRange.Start / BlocksPerStripe;
        auto endStripe = blockRange.End / BlocksPerStripe;
        for (auto i = startStripe; i <= endStripe; ++i) {
            const auto stripeRange =
                TBlockRange64::WithLength(i * BlocksPerStripe, BlocksPerStripe);

            BlockCountPerStripe[i - StartOffset] +=
                stripeRange.Intersect(blockRange).Size();
        }
        ++RequestCount;
        SplittedRequestCount += endStripe - startStripe;
    }

    NJson::TJsonValue Dump() override
    {
        NJson::TJsonValue data;
        data["Config"] = StripeConfig.Dump();

        ui32 maxIoBlockPerStripe = 0;
        ui32 stripeWithIoCount = 0;
        for (unsigned int count: BlockCountPerStripe) {
            maxIoBlockPerStripe = Max(maxIoBlockPerStripe, count);
            stripeWithIoCount += count > 0 ? 1 : 0;
        }

        data["IoTotalBlockCount"] = TotalIoBlockCount;
        data["IoTotalRequestCount"] = RequestCount;
        data["StripeCount"] = DiskInfo.BlockCount / BlocksPerStripe;
        data["StripeWithIoCount"] = stripeWithIoCount;
        data["StripeWithIo%"] = 100.0 * stripeWithIoCount / StripeCount;
        data["MinBlockPerRequest"] = MinBlocksPerRequest;
        data["MaxBlockPerRequest"] = MaxBlocksPerRequest;
        data["AvgBlockPerRequest"] =
            RequestCount ? static_cast<double>(TotalIoBlockCount) / RequestCount
                         : 0;
        data["SplittedRequestCount"] = SplittedRequestCount;

        data["AvgIoPerBlock"] =
            static_cast<double>(TotalIoBlockCount) / DiskInfo.BlockCount;
        const double avgIoPerStripe =
            static_cast<double>(TotalIoBlockCount) / StripeCount;
        data["AvgIoPerStripe"] = avgIoPerStripe;
        data["StripeGroupsDisbalance"] = CalcStripeGroupsDisbalance();
        data["TopLoadedStripes"] = CalcTopLoaded();

        return data;
    }

    NJson::TJsonValue CalcStripeGroupsDisbalance()
    {
        if (!TotalIoBlockCount) {
            return {};
        }

        NJson::TJsonValue data;
        TVector<ui64> ioByStripeGroups(StripeConfig.StripeGroupCount);
        for (size_t i = 0; i < BlockCountPerStripe.size(); ++i) {
            const size_t stripeGroupIndex =
                (i + StartOffset) % StripeConfig.StripeGroupCount;
            ioByStripeGroups[stripeGroupIndex] += BlockCountPerStripe[i];
        }
        const double fairIoFraction = 1.0 / StripeConfig.StripeGroupCount;
        const double maxIoFraction =
            100.0 *
            *MaxElement(ioByStripeGroups.begin(), ioByStripeGroups.end()) /
            TotalIoBlockCount;
        const double minIoFraction =
            100.0 *
            *MinElement(ioByStripeGroups.begin(), ioByStripeGroups.end()) /
            TotalIoBlockCount;

        data["FairFraction%"] = 100.0 * fairIoFraction;
        data["MaxIoFraction%"] = maxIoFraction;
        data["MinIoFraction%"] = minIoFraction;
        data["Disbalance%"] = maxIoFraction / fairIoFraction;

        return data;
    }

    NJson::TJsonValue CalcTopLoaded()
    {
        Sort(BlockCountPerStripe, [&](ui32 a, ui32 b) { return a > b; });

        NJson::TJsonValue data;
        struct TStripesStat
        {
            double BlockCount = 0.0;
            double StripeCount = 0.0;
            double IoBlockCount = 0.0;
            double Overload = 0.0;

            [[nodiscard]] NJson::TJsonValue Dump(ui64 totalIoBlockCount) const
            {
                NJson::TJsonValue data;
                data["BlockCount"] = BlockCount;
                data["StripeCount"] = StripeCount;
                data["IoBlockCount"] = IoBlockCount;
                data["IoFraction%"] = 100.0 * IoBlockCount / totalIoBlockCount;
                data["Overload"] = Overload;
                return data;
            }
        };

        const std::pair<TStringBuf, double> percents[] = {
            {"Top_0.1%", 0.001},
            {"Top_1%", 0.01},
            {"Top_5%", 0.05},
            {"Top_10%", 0.10},
            {"Top_20%", 0.20},
            {"Top_50%", 0.50},
            {"Top_1", 1.0 * BlocksPerStripe / DiskInfo.BlockCount},
            {"Top_2", 2.0 * BlocksPerStripe / DiskInfo.BlockCount},
            {"Top_5", 5.0 * BlocksPerStripe / DiskInfo.BlockCount},
            {"Top_10", 10.0 * BlocksPerStripe / DiskInfo.BlockCount}};

        auto countIo = [&](double percent) -> TStripesStat
        {
            TStripesStat result{
                .BlockCount = DiskInfo.BlockCount * percent,
                .StripeCount = DiskInfo.BlockCount * percent / BlocksPerStripe};

            if (TotalIoBlockCount == 0) {
                return result;
            }

            for (size_t i = 0;
                 i < BlockCountPerStripe.size() && i < result.StripeCount;
                 ++i)
            {
                result.IoBlockCount += BlockCountPerStripe[i];
                if (static_cast<double>(i + 1) > result.StripeCount) {
                    result.IoBlockCount -=
                        (static_cast<double>(i) + 1.0 - result.StripeCount) *
                        BlockCountPerStripe[i];
                }
            }

            const double avgIoPerBlock =
                static_cast<double>(TotalIoBlockCount) / DiskInfo.BlockCount;
            result.Overload =
                (result.IoBlockCount / result.BlockCount) / avgIoPerBlock;
            return result;
        };

        for (auto [name, percent]: percents) {
            data[name] = countIo(percent).Dump(TotalIoBlockCount);
        }

        return data;
    }
};

}   // namespace

///////////////////////////////////////////////////////////////////////////////

TIODistributionStat::TVolumeStat::TVolumeStat(const TDiskInfo& diskInfo)
    : DiskInfo(diskInfo)
{
    auto addCalculators =
        [&](const TString& prefix, const TSet<EBlockStoreRequest>& requestTypes)
    {
        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix,
                    .StripeSize = DiskInfo.BlockCount * DiskInfo.BlockSize,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x2 (93 GiB) NRD/Mirror3",
                    .StripeSize = 1_GB * 93,
                    .StripeGroupCount = 2,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x2 (32 MiB) two partitions SSD",
                    .StripeSize = 32_MB,
                    .StripeGroupCount = 2,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x2 (4 MiB)",
                    .StripeSize = 4_MB,
                    .StripeGroupCount = 2,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x4 (4 MiB)",
                    .StripeSize = 4_MB,
                    .StripeGroupCount = 4,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x8 (4 MiB)",
                    .StripeSize = 4_MB,
                    .StripeGroupCount = 8,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x16 (4 MiB)",
                    .StripeSize = 4_MB,
                    .StripeGroupCount = 16,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x32 (4 MiB)",
                    .StripeSize = 4_MB,
                    .StripeGroupCount = 32,
                    .RequestTypes = requestTypes},
                diskInfo));

        Distributions.emplace_back(
            std::make_unique<TIODistribution>(
                TStripeConfig{
                    .Name = prefix + " Stripe x32 (128 MiB) NBS 2.0",
                    .StripeSize = 128_MB,
                    .StripeGroupCount = 32,
                    .RequestTypes = requestTypes},
                diskInfo));
    };

    addCalculators(
        "Read+Write+Zero",
        {EBlockStoreRequest::ReadBlocks,
         EBlockStoreRequest::ReadBlocksLocal,
         EBlockStoreRequest::WriteBlocks,
         EBlockStoreRequest::WriteBlocksLocal,
         EBlockStoreRequest::ZeroBlocks});
    addCalculators(
        "Read",
        {EBlockStoreRequest::ReadBlocks, EBlockStoreRequest::ReadBlocksLocal});
    addCalculators(
        "Write+Zero",
        {EBlockStoreRequest::WriteBlocks,
         EBlockStoreRequest::WriteBlocksLocal,
         EBlockStoreRequest::ZeroBlocks});
}

void TIODistributionStat::TVolumeStat::ProcessRequest(
    const TDiskInfo& diskInfo,
    const TTimeData& timeData,
    ui32 requestType,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums,
    const TInflightData& inflightData)
{
    Y_UNUSED(diskInfo);
    Y_UNUSED(timeData);
    Y_UNUSED(replicaChecksums);
    Y_UNUSED(inflightData);

    for (auto& distribution: Distributions) {
        distribution->Apply(requestType, blockRange);
    }
}

NJson::TJsonValue TIODistributionStat::TVolumeStat::Dump() const
{
    NJson::TJsonValue diskInfo;
    diskInfo["DiskId"] = DiskInfo.DiskId;
    diskInfo["BlockSize"] = DiskInfo.BlockSize;
    diskInfo["BlockCount"] = DiskInfo.BlockCount;
    diskInfo["DiskSize"] = DiskInfo.BlockSize * DiskInfo.BlockCount;

    NJson::TJsonValue data;
    for (const auto& distribution: Distributions) {
        data[distribution->GetName()] = distribution->Dump();
    }
    diskInfo["Distributions"] = std::move(data);

    return diskInfo;
}

///////////////////////////////////////////////////////////////////////////////

TIODistributionStat::TIODistributionStat(const TString& filename)
    : Filename(filename)
{}

TIODistributionStat::~TIODistributionStat()
{
    Dump();
}

void TIODistributionStat::ProcessRequest(
    const TDiskInfo& diskInfo,
    const TTimeData& timeData,
    ui32 requestType,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums,
    const TInflightData& inflightData)
{
    if (!IsReadWriteZeroRequestType(requestType)) {
        return;
    }

    GetVolumeStat(diskInfo).ProcessRequest(
        diskInfo,
        timeData,
        requestType,
        blockRange,
        replicaChecksums,
        inflightData);
}

TIODistributionStat::TVolumeStat& TIODistributionStat::GetVolumeStat(
    const TDiskInfo& diskInfo)
{
    if (auto it = Volumes.find(diskInfo.DiskId); it != Volumes.end()) {
        return it->second;
    }

    auto [it, inserted] =
        Volumes.emplace(diskInfo.DiskId, TVolumeStat{diskInfo});

    return it->second;
}

void TIODistributionStat::Dump()
{
    TFileOutput out(Filename);

    NJson::TJsonValue all;
    for (const auto& [volume, distributions]: Volumes) {
        all.AppendValue(distributions.Dump());
    }
    NJson::WriteJson(
        &out,
        &all,
        NJson::TJsonWriterConfig{
            .FormatOutput = true,
            .SortKeys = true,
            .WriteNanAsString = true,
        });
}

}   // namespace NCloud::NBlockStore
