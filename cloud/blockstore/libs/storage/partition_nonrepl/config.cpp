#include "config.h"

#include <cloud/storage/core/libs/common/helpers.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<ui64> MakeBlockIndices(const TDevices& devices)
{
    TVector<ui64> result;
    ui64 blockIndex = 0;
    for (const auto& device: devices) {
        result.push_back(blockIndex);
        blockIndex += device.GetBlocksCount();
    }
    return result;
}

bool AllDevicesPresent(const TDevices& devices)
{
    return AllOf(
        devices,
        [](const NProto::TDeviceConfig& config)
        { return !config.GetDeviceUUID().empty(); });
}

}   // namespace

TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfig(
    TNonreplicatedPartitionConfigInitParams params)
    : Devices(std::move(params.Devices))
    , IOMode(params.IOMode)
    , Name(std::move(params.Name))
    , BlockSize(params.BlockSize)
    , VolumeInfo(params.VolumeInfo)
    , ParentActorId(params.ParentActorId)
    , MuteIOErrors(params.MuteIOErrors)
    , FreshDeviceIds(std::move(params.FreshDeviceIds))
    , OutdatedDeviceIds(std::move(params.OutdatedDeviceIds))
    , LaggingDevicesAllowed(params.LaggingDevicesAllowed)
    , MaxTimedOutDeviceStateDuration(params.MaxTimedOutDeviceStateDuration)
    , MaxTimedOutDeviceStateDurationOverridden(
          params.MaxTimedOutDeviceStateDurationOverridden)
    , UseSimpleMigrationBandwidthLimiter(
          params.UseSimpleMigrationBandwidthLimiter)
    , BlockIndices(MakeBlockIndices(Devices))
    , CanReadFromAllDevices(FreshDeviceIds.empty() && OutdatedDeviceIds.empty() && AllDevicesPresent(Devices))
{
    Y_ABORT_UNLESS(Devices.size());
}

TNonreplicatedPartitionConfigPtr TNonreplicatedPartitionConfig::Fork(
    TDevices devices) const
{
    THashSet<TString> freshDeviceIds;
    THashSet<TString> outdatedDeviceIds;
    for (const auto& device: devices) {
        const auto& uuid = device.GetDeviceUUID();

        if (FreshDeviceIds.contains(uuid)) {
            freshDeviceIds.insert(uuid);
        }
        if (OutdatedDeviceIds.contains(uuid)) {
            outdatedDeviceIds.insert(uuid);
        }
    }

    TNonreplicatedPartitionConfigInitParams params{
        std::move(devices),
        VolumeInfo,
        Name,
        BlockSize,
        ParentActorId,
        IOMode,
        MuteIOErrors,
        std::move(freshDeviceIds),
        std::move(outdatedDeviceIds),
        LaggingDevicesAllowed,
        MaxTimedOutDeviceStateDuration,
        MaxTimedOutDeviceStateDurationOverridden,
        UseSimpleMigrationBandwidthLimiter};
    return std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));
}

TVector<TDeviceRequest> TNonreplicatedPartitionConfig::ToDeviceRequests(
    const TBlockRange64 blockRange) const
{
    TVector<TDeviceRequest> res;
    VisitDeviceRequests(
        blockRange,
        [&](const ui32 i,
            const ui32 relativeDeviceIdx,
            const TBlockRange64 requestRange,
            const TBlockRange64 relativeRange)
        {
            if (Devices[i].GetDeviceUUID()) {
                res.emplace_back(
                    Devices[i],
                    i,
                    relativeDeviceIdx,
                    requestRange,
                    relativeRange);
            }

            return false;
        });

    return res;
}

bool TNonreplicatedPartitionConfig::DevicesReadyForReading(
    const TBlockRange64 blockRange) const
{
    return DevicesReadyForReading(blockRange, {});
}

bool TNonreplicatedPartitionConfig::DevicesReadyForReading(
    const TBlockRange64 blockRange,
    const THashSet<TString>& excludeAgentIds) const
{
    if (CanReadFromAllDevices && excludeAgentIds.empty()) {
        return true;
    }

    const bool result = VisitDeviceRequests(
        blockRange,
        [&](const ui32 i,
            const ui32 relativeDeviceIdx,
            const TBlockRange64 requestRange,
            const TBlockRange64 relativeRange)
        {
            Y_UNUSED(relativeDeviceIdx);
            Y_UNUSED(requestRange);
            Y_UNUSED(relativeRange);

            // Note. Keep sync with CanReadFromAllDevices calculation.
            return !Devices[i].GetDeviceUUID() ||
                   excludeAgentIds.contains(Devices[i].GetAgentId()) ||
                   FreshDeviceIds.contains(Devices[i].GetDeviceUUID()) ||
                   OutdatedDeviceIds.contains(Devices[i].GetDeviceUUID());
        });
    return result;
}

void TNonreplicatedPartitionConfig::AugmentErrorFlags(
    NCloud::NProto::TError& error) const
{
    if (MuteIOErrors) {
        SetErrorProtoFlag(error, NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
    }
    if (error.GetCode() == E_IO_SILENT) {
        SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
    }
}

NCloud::NProto::TError TNonreplicatedPartitionConfig::MakeError(
    ui32 code,
    TString message,
    ui32 flags) const
{
    auto error = NCloud::MakeError(code, std::move(message), flags);
    AugmentErrorFlags(error);
    return error;
}

NCloud::NProto::TError TNonreplicatedPartitionConfig::MakeIOError(
    TString message) const
{
    ui32 flags = 0;
    ui32 code = E_IO;
    if (MuteIOErrors) {
        SetProtoFlag(flags, NCloud::NProto::EF_SILENT);
        // for legacy clients
        code = E_IO_SILENT;
    }

    return MakeError(code, std::move(message), flags);
}

TVector<TBlockRange64>
TNonreplicatedPartitionConfig::SplitBlockRangeByDevicesBorder(
    const TBlockRange64 blockRange) const
{
    TVector<TBlockRange64> result;
    VisitDeviceRequests(
        blockRange,
        [&](const ui32 i,
            const ui32 relativeDeviceIdx,
            const TBlockRange64 requestRange,
            const TBlockRange64 relativeRange)
        {
            Y_UNUSED(relativeRange);
            Y_UNUSED(i);
            Y_UNUSED(relativeDeviceIdx);
            result.emplace_back(requestRange);
            return false;
        });

    return result;
}

TBlockRange64 TNonreplicatedPartitionConfig::DeviceRange(ui32 i) const
{
    return TBlockRange64::WithLength(
        BlockIndices[i],
        Devices[i].GetBlocksCount());
}

bool TNonreplicatedPartitionConfig::VisitDeviceRequests(
    const TBlockRange64 blockRange,
    const TDeviceRequestVisitor& visitor) const
{
    const auto f =
        UpperBound(BlockIndices.begin(), BlockIndices.end(), blockRange.Start);

    const auto l =
        UpperBound(BlockIndices.begin(), BlockIndices.end(), blockRange.End);

    const auto fi = std::distance(BlockIndices.begin(), f) - 1;
    const auto li = std::distance(BlockIndices.begin(), l) - 1;

    Y_ABORT_UNLESS(fi < Devices.size());
    Y_ABORT_UNLESS(li < Devices.size());

    for (ui32 i = fi, relativeDeviceIdx = 0; i <= li; ++i, ++relativeDeviceIdx)
    {
        const auto subRange = DeviceRange(i).Intersect(blockRange);
        const auto interrupted = visitor(
            i,
            relativeDeviceIdx,
            subRange,
            TBlockRange64::MakeClosedInterval(
                subRange.Start - BlockIndices[i],
                subRange.End - BlockIndices[i]));

        if (interrupted) {
            return false;
        }
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
