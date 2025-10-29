#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <ydb/core/tablet/tablet_counters.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvServicePrivate
{
    //
    // VolumeTabletStatus notification
    //

    struct TVolumeTabletStatus
    {
        const ui64 TabletId = 0;
        const NProto::TVolume VolumeInfo;
        const NActors::TActorId VolumeActor;
        const NProto::TError Error;

        TVolumeTabletStatus(
                ui64 tabletId,
                const NProto::TVolume& volumeInfo,
                const NActors::TActorId& volumeActor)
            : TabletId(tabletId)
            , VolumeInfo(volumeInfo)
            , VolumeActor(volumeActor)
        {}

        TVolumeTabletStatus(const NProto::TError& error)
            : Error(error)
        {}
    };

    //
    // StartVolumeActorStopped notification
    //

    struct TStartVolumeActorStopped
    {
        const NProto::TError Error;

        TStartVolumeActorStopped(const NProto::TError& error)
            : Error(error)
        {}
    };

    //
    // InactiveClientsTimeout notification
    //

    struct TInactiveClientsTimeout
    {
    };

    //
    // StartVolume request
    //

    struct TStartVolumeRequest
    {
        const ui64 TabletId = 0;

        TStartVolumeRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    //
    // StartVolume response
    //

    struct TStartVolumeResponse
    {
        const NProto::TVolume VolumeInfo;

        TStartVolumeResponse() = default;

        TStartVolumeResponse(NProto::TVolume volumeInfo)
            : VolumeInfo(std::move(volumeInfo))
        {}
    };

    //
    // StopVolume request
    //

    struct TStopVolumeRequest
    {
    };

    //
    // StopVolume response
    //

    struct TStopVolumeResponse
    {
    };

    //
    // MountRequestProcessed notification
    //

    struct TMountRequestProcessed
    {
        const NProto::TVolume Volume;
        const ui64 MountStartTick;
        const NProto::TMountVolumeRequest Request;

        const TRequestInfoPtr RequestInfo;
        const ui64 VolumeTabletId;

        const bool HadLocalStart;
        const NProto::EVolumeBinding BindingType;
        const NProto::EPreemptionSource PreemptionSource;
        const bool VolumeSessionRestartRequired;

        TMountRequestProcessed(
                NProto::TVolume volume,
                NProto::TMountVolumeRequest request,
                ui64 mountStartTick,
                TRequestInfoPtr requestInfo,
                ui64 volumeTabletId,
                bool hadLocalStart,
                NProto::EVolumeBinding bindingType,
                NProto::EPreemptionSource preemptionSource,
                bool volumeSessionRestartRequired)
            : Volume(std::move(volume))
            , MountStartTick(mountStartTick)
            , Request(std::move(request))
            , RequestInfo(std::move(requestInfo))
            , VolumeTabletId(volumeTabletId)
            , HadLocalStart(hadLocalStart)
            , BindingType(bindingType)
            , PreemptionSource(preemptionSource)
            , VolumeSessionRestartRequired(volumeSessionRestartRequired)
        {}
    };

    //
    // UnmountRequestProcessed notification
    //

    struct TUnmountRequestProcessed
    {
        const TString DiskId;
        const TString ClientId;
        const NActors::TActorId RequestSender;
        const NProto::EControlRequestSource Source;
        const bool VolumeSessionRestartRequired;

        TUnmountRequestProcessed(
                TString diskId,
                TString clientId,
                const NActors::TActorId& requestSender,
                NProto::EControlRequestSource source,
                bool volumeSessionRestartRequired)
            : DiskId(std::move(diskId))
            , ClientId(std::move(clientId))
            , RequestSender(requestSender)
            , Source(source)
            , VolumeSessionRestartRequired(volumeSessionRestartRequired)
        {}
    };

    //
    // UploadDisksStatsCompleted notification
    //

    struct TSessionActorDied
    {
        TString DiskId;
    };

    //
    // VolumePipeReset notification
    //

    struct TVolumePipeReset
    {
        ui64 ResetTick = 0;

        explicit TVolumePipeReset(ui64 tick)
            : ResetTick(tick)
        {}
    };

    //
    // ResetPipeClient notification
    //

    struct TResetPipeClient
    {
    };


    //
    // InternalMountVolume request
    //

    struct TInternalMountVolumeRequest
    {
        NProto::TMountVolumeRequest Record;
        NProto::EVolumeBinding BindingType = NProto::BINDING_NOT_SET;
        NProto::EPreemptionSource PreemptionSource = NProto::SOURCE_NONE;

        TInternalMountVolumeRequest() = default;

        explicit TInternalMountVolumeRequest(
                NProto::TMountVolumeRequest record)
            : Record(std::move(record))
        {}
    };

    //
    // InternalMountVolume response
    //

    struct TInternalMountVolumeResponse
    {
        NProto::TMountVolumeResponse Record;
    };

    //
    // Create encryption key
    //

    struct TCreateEncryptionKeyResponse
    {
        NProto::TKmsKey KmsKey;
    };

    //
    // List mounted volumes
    //

    struct TListMountedVolumesRequest
    {
    };

    struct TListMountedVolumesResponse
    {
        struct TMountedVolumeInfo
        {
            TString DiskId;
            NActors::TActorId VolumeActor;
        };
        TVector<TMountedVolumeInfo> MountedVolumes;
    };

    //
    // UpdateManuallyPreemptedVolume notification
    //

    struct TUpdateManuallyPreemptedVolume
    {
        const TString DiskId;
        const NProto::EPreemptionSource PreemptionSource;

        TUpdateManuallyPreemptedVolume(
                const TString& diskId,
                NProto::EPreemptionSource preemptionSource)
            : DiskId(diskId)
            , PreemptionSource(preemptionSource)
        {}
    };

    //
    // SyncManuallyPreemptedVolumesComplete notification
    //

    struct TSyncManuallyPreemptedVolumesComplete
    {
    };

    //
    // Ping notification
    //

    struct TSelfPing
    {
        const ui64 StartCycles;

        explicit TSelfPing(ui64 startCycles)
            : StartCycles(startCycles)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::SERVICE_START,

        EvVolumeTabletStatus,
        EvStartVolumeActorStopped,
        EvInactiveClientsTimeout,
        EvStartVolumeRequest,
        EvStartVolumeResponse,
        EvStopVolumeRequest,
        EvStopVolumeResponse,
        EvMountRequestProcessed,
        EvUnmountRequestProcessed,
        EvSessionActorDied,
        EvVolumePipeReset,
        EvResetPipeClient,
        EvInternalMountVolumeRequest,
        EvInternalMountVolumeResponse,
        EvUpdateManuallyPreemptedVolume,
        EvSyncManuallyPreemptedVolumesComplete,
        EvSelfPing,
        EvCreateEncryptionKeyResponse,
        EvListMountedVolumesRequest,
        EvListMountedVolumesResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::SERVICE_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::SERVICE_END");


    using TEvVolumeTabletStatus = TRequestEvent<
        TVolumeTabletStatus,
        EvVolumeTabletStatus
    >;

    using TEvStartVolumeActorStopped = TRequestEvent<
        TStartVolumeActorStopped,
        EvStartVolumeActorStopped
    >;

    using TEvInactiveClientsTimeout = TRequestEvent<
        TInactiveClientsTimeout,
        EvInactiveClientsTimeout
    >;

    using TEvStartVolumeRequest = TRequestEvent<
        TStartVolumeRequest,
        EvStartVolumeRequest
    >;

    using TEvStartVolumeResponse = TResponseEvent<
        TStartVolumeResponse,
        EvStartVolumeResponse
    >;

    using TEvStopVolumeRequest = TRequestEvent<
        TStopVolumeRequest,
        EvStopVolumeRequest
    >;

    using TEvStopVolumeResponse = TResponseEvent<
        TStopVolumeResponse,
        EvStopVolumeResponse
    >;

    using TEvMountRequestProcessed = TResponseEvent<
        TMountRequestProcessed,
        EvMountRequestProcessed
    >;

    using TEvUnmountRequestProcessed = TResponseEvent<
        TUnmountRequestProcessed,
        EvUnmountRequestProcessed
    >;

    using TEvSessionActorDied = TResponseEvent<
        TSessionActorDied,
        EvSessionActorDied
    >;

    using TEvVolumePipeReset = TRequestEvent<
        TVolumePipeReset,
        EvVolumePipeReset
    >;

    using TEvResetPipeClient = TRequestEvent<
        TResetPipeClient,
        EvResetPipeClient
    >;

    using TEvInternalMountVolumeRequest = TRequestEvent<
        TInternalMountVolumeRequest,
        EvInternalMountVolumeRequest
    >;

    using TEvInternalMountVolumeResponse = TResponseEvent<
        TInternalMountVolumeResponse,
        EvInternalMountVolumeResponse
    >;

    using TEvUpdateManuallyPreemptedVolume = TRequestEvent<
        TUpdateManuallyPreemptedVolume,
        EvUpdateManuallyPreemptedVolume
    >;

    using TEvSyncManuallyPreemptedVolumesComplete = TRequestEvent<
        TSyncManuallyPreemptedVolumesComplete,
        EvSyncManuallyPreemptedVolumesComplete
    >;

    using TEvSelfPing = TRequestEvent<TSelfPing, EvSelfPing>;

    using TEvCreateEncryptionKeyResponse = TResponseEvent<
        TCreateEncryptionKeyResponse,
        EvCreateEncryptionKeyResponse>;

    using TEvListMountedVolumesRequest = TResponseEvent<
        TListMountedVolumesRequest,
        EvListMountedVolumesRequest>;

    using TEvListMountedVolumesResponse = TResponseEvent<
        TListMountedVolumesResponse,
        EvListMountedVolumesResponse>;
};

}   // namespace NCloud::NBlockStore::NStorage
