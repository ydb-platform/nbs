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
    const TBlockRange64 BlockRange;
    const TBlockRange64 DeviceBlockRange;

    TDeviceRequest(
            const NProto::TDeviceConfig& device,
            const ui32 deviceIdx,
            const TBlockRange64& blockRange,
            const TBlockRange64& deviceBlockRange)
        : Device(device)
        , DeviceIdx(deviceIdx)
        , BlockRange(blockRange)
        , DeviceBlockRange(deviceBlockRange)
    {
    }
};

class TNonreplicatedPartitionConfig
{
public:
    struct TVolumeInfo
    {
        TInstant CreationTs;
        NProto::EStorageMediaKind MediaKind;
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
    // List of devices that have outdated data. Can only appear on mirror disks.
    const THashSet<TString> LaggingDeviceIds;
    const TDuration MaxTimedOutDeviceStateDuration;
    const bool MaxTimedOutDeviceStateDurationOverridden;
    const bool UseSimpleMigrationBandwidthLimiter;

    TVector<ui64> BlockIndices;

public:
    TNonreplicatedPartitionConfig(
            TDevices devices,
            NProto::EVolumeIOMode ioMode,
            TString name,
            ui32 blockSize,
            TVolumeInfo volumeInfo,
            NActors::TActorId parentActorId,
            bool muteIOErrors,
            THashSet<TString> freshDeviceIds,
            THashSet<TString> laggingDeviceIds,
            TDuration maxTimedOutDeviceStateDuration,
            bool maxTimedOutDeviceStateDurationOverridden,
            bool useSimpleMigrationBandwidthLimiter)
        : Devices(std::move(devices))
        , IOMode(ioMode)
        , Name(std::move(name))
        , BlockSize(blockSize)
        , VolumeInfo(volumeInfo)
        , ParentActorId(std::move(parentActorId))
        , MuteIOErrors(muteIOErrors)
        , FreshDeviceIds(std::move(freshDeviceIds))
        , LaggingDeviceIds(std::move(laggingDeviceIds))
        , MaxTimedOutDeviceStateDuration(maxTimedOutDeviceStateDuration)
        , MaxTimedOutDeviceStateDurationOverridden(maxTimedOutDeviceStateDurationOverridden)
        , UseSimpleMigrationBandwidthLimiter(useSimpleMigrationBandwidthLimiter)
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
        THashSet<TString> laggingDeviceIds;
        for (const auto& device: devices) {
            const auto& uuid = device.GetDeviceUUID();

            if (FreshDeviceIds.contains(uuid)) {
                freshDeviceIds.insert(uuid);
            }
            if (LaggingDeviceIds.contains(uuid)) {
                laggingDeviceIds.insert(uuid);
            }
        }

        return std::make_shared<TNonreplicatedPartitionConfig>(
            std::move(devices),
            IOMode,
            Name,
            BlockSize,
            VolumeInfo,
            ParentActorId,
            MuteIOErrors,
            std::move(freshDeviceIds),
            std::move(laggingDeviceIds),
            MaxTimedOutDeviceStateDuration,
            MaxTimedOutDeviceStateDurationOverridden,
            UseSimpleMigrationBandwidthLimiter
        );
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

    const THashSet<TString>& GetLaggingDeviceIds() const
    {
        return LaggingDeviceIds;
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
    TVector<TDeviceRequest> ToDeviceRequests(const TBlockRange64 blockRange) const
    {
        TVector<TDeviceRequest> res;
        VisitDeviceRequests(
            blockRange,
            [&] (
                const ui32 i,
                const TBlockRange64 requestRange,
                const TBlockRange64 relativeRange)
            {
                if (Devices[i].GetDeviceUUID()) {
                    res.emplace_back(
                        Devices[i],
                        i,
                        requestRange,
                        relativeRange);
                }

                return false;
            });

        return res;
    }

    bool DevicesReadyForReading(const TBlockRange64 blockRange) const
    {
        return VisitDeviceRequests(
            blockRange,
            [&] (
                const ui32 i,
                const TBlockRange64 requestRange,
                const TBlockRange64 relativeRange)
            {
                Y_UNUSED(requestRange);
                Y_UNUSED(relativeRange);

                return !Devices[i].GetDeviceUUID()
                    || FreshDeviceIds.contains(Devices[i].GetDeviceUUID())
                    || LaggingDeviceIds.contains(Devices[i].GetDeviceUUID());
            });
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
        const TBlockRange64 requestRange,
        const TBlockRange64 relativeRange
    )>;

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

        for (ui32 i = fi; i <= li; ++i) {
            const auto subRange = DeviceRange(i).Intersect(blockRange);
            const auto interrupted = visitor(
                i,
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
