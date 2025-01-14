
#pragma once

#include <contrib/ydb/library/actors/interconnect/types.h>

#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <contrib/ydb/library/actors/core/actor.h>

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

TActorId AcquireDevices(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TVector<NProto::TDeviceConfig> devices,
    TString diskId,
    TString clientId,
    NProto::EVolumeAccessMode accessMode,
    ui64 mountSeqNumber,
    ui32 volumeGeneration,
    TDuration requestTimeout,
    bool muteIOErrors,
    NActors::NLog::EComponent component);

TActorId ReleaseDevices(
    const NActors::TActorContext& ctx,
    const TActorId& owner,
    TString diskId,
    TString clientId,
    ui32 volumeGeneration,
    TDuration requestTimeout,
    TVector<NProto::TDeviceConfig> devices,
    bool muteIOErrors,
    NActors::NLog::EComponent component);

}   // namespace NCloud::NBlockStore::NStorage::NAcquireReleaseDevices
