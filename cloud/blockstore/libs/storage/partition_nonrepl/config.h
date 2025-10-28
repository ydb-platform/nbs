#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/utility.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDevices = google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>;
using TMigrations = google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>;

struct TDeviceRequest
{
    const NProto::TDeviceConfig& Device;
    const ui32 DeviceIdx;
    const ui32 RelativeDeviceIdx;
    const TBlockRange64 BlockRange;
    const TBlockRange64 DeviceBlockRange;

    TDeviceRequest(
            const NProto::TDeviceConfig& device,
            const ui32 deviceIdx,
            const ui32 relativeDeviceIdx,
            const TBlockRange64& blockRange,
            const TBlockRange64& deviceBlockRange)
        : Device(device)
        , DeviceIdx(deviceIdx)
        , RelativeDeviceIdx(relativeDeviceIdx)
        , BlockRange(blockRange)
        , DeviceBlockRange(deviceBlockRange)
    {}
};

class TNonreplicatedPartitionConfig
{
public:
    struct TVolumeInfo
    {
        TInstant CreationTs;
        NProto::EStorageMediaKind MediaKind;
        NProto::EEncryptionMode EncryptionMode;
    };

    struct TNonreplicatedPartitionConfigInitParams
    {
        TDevices Devices;
        TVolumeInfo VolumeInfo;
        TString Name;
        ui32 BlockSize;
        NActors::TActorId ParentActorId;
        NProto::EVolumeIOMode IOMode = NProto::VOLUME_IO_OK;
        bool MuteIOErrors = false;
        THashSet<TString> FreshDeviceIds;
        THashSet<TString> OutdatedDeviceIds;
        bool LaggingDevicesAllowed = false;
        TDuration MaxTimedOutDeviceStateDuration;
        bool MaxTimedOutDeviceStateDurationOverridden = false;
        bool UseSimpleMigrationBandwidthLimiter = true;

        TNonreplicatedPartitionConfigInitParams(
                TDevices devices,
                TVolumeInfo volumeInfo,
                TString name,
                ui32 blockSize,
                NActors::TActorId parentActorId)
            : Devices(std::move(devices))
            , VolumeInfo(volumeInfo)
            , Name(std::move(name))
            , BlockSize(blockSize)
            , ParentActorId(parentActorId)
        {}

        TNonreplicatedPartitionConfigInitParams(
                TDevices devices,
                TVolumeInfo volumeInfo,
                TString name,
                ui32 blockSize,
                NActors::TActorId parentActorId,
                NProto::EVolumeIOMode iOMode,
                bool muteIOErrors,
                THashSet<TString> freshDeviceIds,
                THashSet<TString> outdatedDeviceIds,
                bool laggingDevicesAllowed,
                TDuration maxTimedOutDeviceStateDuration,
                bool maxTimedOutDeviceStateDurationOverridden,
                bool useSimpleMigrationBandwidthLimiter)
            : Devices(std::move(devices))
            , VolumeInfo(volumeInfo)
            , Name(std::move(name))
            , BlockSize(blockSize)
            , ParentActorId(parentActorId)
            , IOMode(iOMode)
            , MuteIOErrors(muteIOErrors)
            , FreshDeviceIds(std::move(freshDeviceIds))
            , OutdatedDeviceIds(std::move(outdatedDeviceIds))
            , LaggingDevicesAllowed(laggingDevicesAllowed)
            , MaxTimedOutDeviceStateDuration(maxTimedOutDeviceStateDuration)
            , MaxTimedOutDeviceStateDurationOverridden(
                  maxTimedOutDeviceStateDurationOverridden)
            , UseSimpleMigrationBandwidthLimiter(
                  useSimpleMigrationBandwidthLimiter)
        {}

        ~TNonreplicatedPartitionConfigInitParams() = default;
    };

private:
    TDevices Devices;
    const NProto::EVolumeIOMode IOMode;
    const TString Name;
    const ui32 BlockSize;
    const TVolumeInfo VolumeInfo;
    const NActors::TActorId ParentActorId;
    const bool MuteIOErrors;
    const THashSet<TString> FreshDeviceIds;
    // List of devices that previously were lagging and now have outdated data.
    // Can only appear on mirror disks.
    const THashSet<TString> OutdatedDeviceIds;
    // Whether a replica of a mirror disk is allowed to lag.
    const bool LaggingDevicesAllowed;
    const TDuration MaxTimedOutDeviceStateDuration;
    const bool MaxTimedOutDeviceStateDurationOverridden;
    const bool UseSimpleMigrationBandwidthLimiter;

    TVector<ui64> BlockIndices;

public:
    explicit TNonreplicatedPartitionConfig(
            TNonreplicatedPartitionConfigInitParams params)
        : Devices(std::move(params.Devices))
        , IOMode(params.IOMode)
        , Name(std::move(params.Name))
        , BlockSize(params.BlockSize)
        , VolumeInfo(params.VolumeInfo)
        , ParentActorId(std::move(params.ParentActorId))
        , MuteIOErrors(params.MuteIOErrors)
        , FreshDeviceIds(std::move(params.FreshDeviceIds))
        , OutdatedDeviceIds(std::move(params.OutdatedDeviceIds))
        , LaggingDevicesAllowed(params.LaggingDevicesAllowed)
        , MaxTimedOutDeviceStateDuration(params.MaxTimedOutDeviceStateDuration)
        , MaxTimedOutDeviceStateDurationOverridden(
              params.MaxTimedOutDeviceStateDurationOverridden)
        , UseSimpleMigrationBandwidthLimiter(
              params.UseSimpleMigrationBandwidthLimiter)
    {
        Y_ABORT_UNLESS(Devices.size());

        ui64 blockIndex = 0;
        for (const auto& device: Devices) {
            BlockIndices.push_back(blockIndex);
            blockIndex += device.GetBlocksCount();
        }
    }

public:
    TNonreplicatedPartitionConfigPtr Fork(TDevices devices) const
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
        return std::make_shared<TNonreplicatedPartitionConfig>(
            std::move(params));
    }

    const auto& GetDevices() const
    {
        return Devices;
    }

    auto& AccessDevices()
    {
        return Devices;
    }

    bool IsReadOnly() const
    {
        return IOMode != NProto::VOLUME_IO_OK;
    }

    bool GetMuteIOErrors() const
    {
        return MuteIOErrors;
    }

    const auto& GetName() const
    {
        return Name;
    }

    ui64 GetBlockCount() const
    {
        return BlockIndices.back() + Devices.rbegin()->GetBlocksCount();
    }

    auto GetBlockSize() const
    {
        return BlockSize;
    }

    const auto& GetVolumeInfo() const
    {
        return VolumeInfo;
    }

    const auto& GetParentActorId() const
    {
        return ParentActorId;
    }

    const auto& GetFreshDeviceIds() const
    {
        return FreshDeviceIds;
    }

    const THashSet<TString>& GetOutdatedDeviceIds() const
    {
        return OutdatedDeviceIds;
    }

    bool GetLaggingDevicesAllowed() const
    {
        return LaggingDevicesAllowed;
    }

    auto GetMaxTimedOutDeviceStateDuration() const
    {
        return MaxTimedOutDeviceStateDuration;
    }

    auto IsMaxTimedOutDeviceStateDurationOverridden() const
    {
        return MaxTimedOutDeviceStateDurationOverridden;
    }

    auto GetUseSimpleMigrationBandwidthLimiter() const
    {
        return UseSimpleMigrationBandwidthLimiter;
    }

public:
    TVector<TDeviceRequest> ToDeviceRequests(
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

    bool DevicesReadyForReading(const TBlockRange64 blockRange) const
    {
        return DevicesReadyForReading(blockRange, {});
    }

    bool DevicesReadyForReading(
        const TBlockRange64 blockRange,
        const THashSet<TString>& excludeAgentIds) const
    {
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

                return !Devices[i].GetDeviceUUID() ||
                       excludeAgentIds.contains(Devices[i].GetAgentId()) ||
                       FreshDeviceIds.contains(Devices[i].GetDeviceUUID()) ||
                       OutdatedDeviceIds.contains(Devices[i].GetDeviceUUID());
            });
        return result;
    }

    void AugmentErrorFlags(NCloud::NProto::TError& error) const
    {
        if (MuteIOErrors) {
            SetErrorProtoFlag(error, NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
        }
        if (error.GetCode() == E_IO_SILENT) {
            SetErrorProtoFlag(error, NCloud::NProto::EF_SILENT);
        }
    }

    NCloud::NProto::TError MakeError(
        ui32 code,
        TString message,
        ui32 flags = 0) const
    {
        auto error = NCloud::MakeError(code, std::move(message), flags);
        AugmentErrorFlags(error);
        return error;
    }

    NCloud::NProto::TError MakeIOError(TString message) const
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

    auto SplitBlockRangeByDevicesBorder(const TBlockRange64 blockRange) const
        -> TVector<TBlockRange64>
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

private:
    TBlockRange64 DeviceRange(ui32 i) const
    {
        return TBlockRange64::WithLength(
            BlockIndices[i],
            Devices[i].GetBlocksCount()
        );
    }

    using TDeviceRequestVisitor = std::function<bool(
        const ui32 deviceIndex,
        const ui32 relativeDeviceIndex,
        const TBlockRange64 requestRange,
        const TBlockRange64 relativeRange)>;

    bool VisitDeviceRequests(
        const TBlockRange64 blockRange,
        const TDeviceRequestVisitor& visitor) const
    {
        const auto f = UpperBound(
            BlockIndices.begin(),
            BlockIndices.end(),
            blockRange.Start
        );

        const auto l = UpperBound(
            BlockIndices.begin(),
            BlockIndices.end(),
            blockRange.End
        );

        const auto fi = std::distance(BlockIndices.begin(), f) - 1;
        const auto li = std::distance(BlockIndices.begin(), l) - 1;

        Y_ABORT_UNLESS(fi < Devices.size());
        Y_ABORT_UNLESS(li < Devices.size());

        for (ui32 i = fi, relativeDeviceIdx = 0; i <= li;
             ++i, ++relativeDeviceIdx)
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
};

}   // namespace NCloud::NBlockStore::NStorage
