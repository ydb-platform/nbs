#pragma once

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/interconnect/types.h>

namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices {

struct TAgentAcquireDevicesCachedRequest
{
    TString AgentId;
    NProto::TAcquireDevicesRequest Request;
    TInstant RequestTime;
};

struct TAgentReleaseDevicesCachedRequest
{
    TString AgentId;
    NProto::TReleaseDevicesRequest Request;
};

struct TDevicesAcquireFinished
{
    TString DiskId;
    TString ClientId;
    TVector<TAgentAcquireDevicesCachedRequest> SentRequests;
    TVector<NProto::TDeviceConfig> Devices;
    NProto::TError Error;

    TDevicesAcquireFinished(
            TString diskId,
            TString clientId,
            TVector<TAgentAcquireDevicesCachedRequest> sentRequests,
            TVector<NProto::TDeviceConfig> devices,
            NProto::TError error)
        : DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , SentRequests(std::move(sentRequests))
        , Devices(std::move(devices))
        , Error(std::move(error))
    {}
};

struct TDevicesReleaseFinished
{
    TString DiskId;
    TString ClientId;
    TVector<TAgentReleaseDevicesCachedRequest> SentRequests;
    NProto::TError Error;

    TDevicesReleaseFinished(
            TString diskId,
            TString clientId,
            TVector<TAgentReleaseDevicesCachedRequest> sentRequests,
            NProto::TError error)
        : DiskId(std::move(diskId))
        , ClientId(std::move(clientId))
        , SentRequests(std::move(sentRequests))
        , Error(std::move(error))
    {}
};

enum EEvents
{
    EvBegin,

    EvDevicesAcquireFinished,
    EvDevicesReleaseFinished,

    EvEnd
};

using TEvDevicesAcquireFinished =
    TRequestEvent<TDevicesAcquireFinished, EvDevicesAcquireFinished>;

using TEvDevicesReleaseFinished =
    TRequestEvent<TDevicesReleaseFinished, EvDevicesReleaseFinished>;

struct TAcquireReleaseDevicesInfo
{
    TVector<NProto::TDeviceConfig> Devices;
    TString DiskId;
    TString ClientId;
    std::optional<NProto::EVolumeAccessMode>
        AccessMode;                       // Only AcquireDevicesActor need it.
    std::optional<ui64> MountSeqNumber;   // Only AcquireDevicesActor need it.
    ui32 VolumeGeneration;
    TDuration RequestTimeout;
    bool MuteIOErrors;
};

TActorId CreateAcquireDevicesActor(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TAcquireReleaseDevicesInfo acquireDevicesInfo,
    NActors::NLog::EComponent component);

TActorId CreateReleaseDevicesActor(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TAcquireReleaseDevicesInfo releaseDevicesInfo,
    NActors::NLog::EComponent component);

}   // namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices
