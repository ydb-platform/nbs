#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/hash_set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDevices = google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>;
using TMigrations = google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>;

struct TDeviceRequest
{
    const NProto::TDeviceConfig& Device;
    // Index of the device in the partition config.
    const ui32 DeviceIdx;
    // Index of the device in original request. E.g. when the request is split
    // between two devices, this will be 0 for the first deviceRequest and 1 for
    // the second.
    const ui32 RelativeDeviceIdx;
    // Request block range.
    const TBlockRange64 BlockRange;
    // Block range that is relative to the device borders.
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
    const TDevices Devices;
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
    const TVector<ui64> BlockIndices;
    const bool CanReadFromAllDevices = false;

public:
    explicit TNonreplicatedPartitionConfig(
        TNonreplicatedPartitionConfigInitParams params);

    TNonreplicatedPartitionConfigPtr Fork(TDevices devices) const;

    const auto& GetDevices() const
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

    const TString& GetName() const
    {
        return Name;
    }

    ui64 GetBlockCount() const
    {
        return BlockIndices.back() + Devices.rbegin()->GetBlocksCount();
    }

    ui32 GetBlockSize() const
    {
        return BlockSize;
    }

    const TVolumeInfo& GetVolumeInfo() const
    {
        return VolumeInfo;
    }

    NActors::TActorId GetParentActorId() const
    {
        return ParentActorId;
    }

    const THashSet<TString>& GetFreshDeviceIds() const
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

    TDuration GetMaxTimedOutDeviceStateDuration() const
    {
        return MaxTimedOutDeviceStateDuration;
    }

    bool IsMaxTimedOutDeviceStateDurationOverridden() const
    {
        return MaxTimedOutDeviceStateDurationOverridden;
    }

    bool GetUseSimpleMigrationBandwidthLimiter() const
    {
        return UseSimpleMigrationBandwidthLimiter;
    }

    TVector<TDeviceRequest> ToDeviceRequests(
        const TBlockRange64 blockRange) const;

    bool DevicesReadyForReading(const TBlockRange64 blockRange) const;
    bool DevicesReadyForReading(
        const TBlockRange64 blockRange,
        const THashMap<TString, NProto::TLaggingAgent>& laggingAgents) const;

    void AugmentErrorFlags(NCloud::NProto::TError& error) const;

    NCloud::NProto::TError
    MakeError(ui32 code, TString message, ui32 flags = 0) const;

    NCloud::NProto::TError MakeIOError(TString message) const;

    TVector<TBlockRange64> SplitBlockRangeByDevicesBorder(
        const TBlockRange64 blockRange) const;

private:
    using TDeviceRequestVisitor = std::function<bool(
        const ui32 deviceIndex,
        const ui32 relativeDeviceIndex,
        const TBlockRange64 requestRange,
        const TBlockRange64 relativeRange)>;

    TBlockRange64 DeviceRange(ui32 i) const;

    bool VisitDeviceRequests(
        const TBlockRange64 blockRange,
        const TDeviceRequestVisitor& visitor) const;
};

}   // namespace NCloud::NBlockStore::NStorage
