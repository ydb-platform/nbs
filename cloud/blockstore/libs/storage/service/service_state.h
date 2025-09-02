#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/service.h>

#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/ptr.h>
#include <util/generic/queue.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TClientInfo
    : public TIntrusiveListItem<TClientInfo>
{
    TString ClientId;
    TString DiskId;
    NProto::EVolumeAccessMode VolumeAccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
    NProto::EVolumeMountMode VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;
    ui64 MountSeqNumber = 0;
    ui32 MountFlags = 0;
    TInstant LastActivityTime;
    ui64 LastMountTick = 0;
    TString ClientVersionInfo;
    ui64 FillSeqNumber = 0;
    ui64 FillGeneration = 0;

    // IPC info
    NProto::EClientIpcType IpcType = NProto::IPC_GRPC;

    TClientInfo(
        TString clientId,
        TString diskId);
};

using TClientInfoList = TIntrusiveListWithAutoDelete<TClientInfo, TDelete>;

////////////////////////////////////////////////////////////////////////////////

struct TSharedServiceCounters
    : TAtomicRefCount<TSharedServiceCounters>
{
    const TStorageConfigPtr Config;

    TAtomic LocalVolumeCount = 0;

    TSharedServiceCounters(TStorageConfigPtr config)
        : Config(std::move(config))
    {}

    bool TryAcquireLocalVolume();
    void ReleaseLocalVolume();
};

using TSharedServiceCountersPtr = TIntrusivePtr<TSharedServiceCounters>;

////////////////////////////////////////////////////////////////////////////////

struct TVolumeInfo
{
    enum EState
    {
        INITIAL,
        STARTED,
        FAILED,
        STOPPING
    };

    static const auto MAX_VOLUME_BINDING = NProto::BINDING_REMOTE;

    const TString DiskId;

    ui64 TabletId = 0;
    TString TabletHost;
    TMaybe<NProto::TVolume> VolumeInfo;
    const TString SessionId;

    NActors::TActorId VolumeSessionActor;
    NActors::TActorId VolumeClientActor;

    EState State = INITIAL;
    NActors::TActorId VolumeActor;
    NProto::TError Error;

    TClientInfoList ClientInfos;
    THashMap<TString, TClientInfo*> ClientInfosByClientId;

    NProto::EVolumeBinding BindingType = NProto::BINDING_REMOTE;
    NProto::EPreemptionSource PreemptionSource = NProto::SOURCE_NONE;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;

    TDuration InitialAddClientTimeout;

    bool SharedCountersLockAcquired = false;

    bool RebindingIsInflight = false;

    bool SyncManuallyPreemptedVolumesRequired = false;

    bool TabletReportedLocalMount = false;

    TVolumeInfo(TString diskId);

    //
    // Client info
    //

    TClientInfo* GetClientInfo(const TString& clientId) const;
    TClientInfo* GetReadWriteAccessClientInfo() const;
    TClientInfo* GetLocalMountClientInfo() const;
    TClientInfo* AddClientInfo(const TString& clientId);
    void RemoveClientInfo(TClientInfo* info);
    void RemoveClientInfo(const TString& clientId);
    void SetTabletReportedLocalMount(bool value);
    bool GetTabletReportedLocalMount() const
    {
        return TabletReportedLocalMount;
    }

    bool IsMounted() const;
    bool IsReadWriteMounted() const;
    bool IsLocallyMounted() const;
    bool IsDiskRegistryVolume() const;

    //
    // Status
    //

    void SetStarted(
        ui64 tabletId,
        const NProto::TVolume& volumeInfo,
        const NActors::TActorId& volumeActor);

    void SetFailed(const NProto::TError& error);

    TString GetStatus() const;

    // Binding

    NProto::EVolumeBinding OnMountStarted(
        TSharedServiceCounters& sharedCounters,
        NProto::EPreemptionSource preemptionSource,
        NProto::EVolumeBinding bindingMode,
        NProto::EVolumeMountMode clientMode,
        bool applyLocalVolumesLimit);

    void OnMountCancelled(
        TSharedServiceCounters& sharedCounters,
        NProto::EPreemptionSource preemptionSource);

    void OnMountFinished(
        TSharedServiceCounters& sharedCounters,
        NProto::EPreemptionSource preemptionSource,
        NProto::EVolumeBinding bindingType,
        const NProto::TError& error);

    void OnClientRemoved(TSharedServiceCounters& sharedCounters);

    NProto::EVolumeBinding CalcVolumeBinding(
        NProto::EPreemptionSource preemptionSource,
        NProto::EVolumeBinding explicitBindingType,
        NProto::EVolumeMountMode clientMode) const;

    void UpdatePreemptionSource(NProto::EPreemptionSource candidate);

    bool ShouldSyncManuallyPreemptedVolumes() const
    {
        return SyncManuallyPreemptedVolumesRequired;
    }

private:
    void UpdateSyncManuallyPreemptedVolumes(NProto::EPreemptionSource oldSource)
    {
        bool checkForManual =
            oldSource == NProto::SOURCE_MANUAL ||
            PreemptionSource == NProto::SOURCE_MANUAL;
        SyncManuallyPreemptedVolumesRequired =
            oldSource != PreemptionSource && checkForManual;
    }
};

using TVolumeInfoPtr = std::shared_ptr<TVolumeInfo>;

////////////////////////////////////////////////////////////////////////////////

class TServiceState
{
private:
    using TVolumesMap = THashMap<TString, TVolumeInfoPtr>;

    const TManuallyPreemptedVolumesPtr ManuallyPreemptedVolumes;
    TVolumesMap VolumesById;

    bool IsManuallyPreemptedVolumesTrackingDisabled = false;

public:
    TServiceState(TManuallyPreemptedVolumesPtr preemptedVolumes);

    void RemoveVolume(TVolumeInfoPtr volume);
    TVolumeInfoPtr GetVolume(const TString& diskId) const;
    TVolumeInfoPtr GetOrAddVolume(const TString& diskId);

    const TVolumesMap& GetVolumes() const
    {
        return VolumesById;
    }

    TVolumesMap& GetVolumes()
    {
        return VolumesById;
    }

    TManuallyPreemptedVolumesPtr GetManuallyPreemptedVolumes()
    {
        return ManuallyPreemptedVolumes;
    }

    void SetIsManuallyPreemptedVolumesTrackingDisabled(bool disable)
    {
        IsManuallyPreemptedVolumesTrackingDisabled = disable;
    }

    bool GetIsManuallyPreemptedVolumesTrackingDisabled()
    {
        return IsManuallyPreemptedVolumesTrackingDisabled;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
