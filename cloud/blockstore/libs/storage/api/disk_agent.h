#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_AGENT_REQUESTS_LOCAL(xxx, ...)                         \
    xxx(WaitReady,          __VA_ARGS__)                                       \
// BLOCKSTORE_DISK_AGENT_REQUESTS_LOCAL

#define BLOCKSTORE_DISK_AGENT_REQUESTS_PROTO(xxx, ...)                         \
    xxx(AcquireDevices,           __VA_ARGS__)                                 \
    xxx(ReleaseDevices,           __VA_ARGS__)                                 \
    xxx(ReadDeviceBlocks,         __VA_ARGS__)                                 \
    xxx(WriteDeviceBlocks,        __VA_ARGS__)                                 \
    xxx(ZeroDeviceBlocks,         __VA_ARGS__)                                 \
    xxx(SecureEraseDevice,        __VA_ARGS__)                                 \
    xxx(ChecksumDeviceBlocks,     __VA_ARGS__)                                 \
    xxx(DisableConcreteAgent,     __VA_ARGS__)                                 \
    xxx(EnableAgentDevice,        __VA_ARGS__)                                 \
    xxx(PartiallySuspendAgent,    __VA_ARGS__)                                 \
    xxx(DirectCopyBlocks,         __VA_ARGS__)                                 \

// BLOCKSTORE_DISK_AGENT_REQUESTS_PROTO

#define BLOCKSTORE_DISK_AGENT_REQUESTS(xxx, ...)                               \
    BLOCKSTORE_DISK_AGENT_REQUESTS_LOCAL(xxx, __VA_ARGS__)                     \
    BLOCKSTORE_DISK_AGENT_REQUESTS_PROTO(xxx, __VA_ARGS__)                     \
// BLOCKSTORE_DISK_AGENT_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskAgent
{
    //
    // WaitReady
    //

    struct TWaitReadyRequest
    {
    };

    struct TWaitReadyResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::DISK_AGENT_START,

        EvWaitReadyRequest = EvBegin + 1,
        EvWaitReadyResponse = EvBegin + 2,

        EvAcquireDevicesRequest = EvBegin + 3,
        EvAcquireDevicesResponse = EvBegin + 4,

        EvReleaseDevicesRequest = EvBegin + 5,
        EvReleaseDevicesResponse = EvBegin + 6,

        EvReadDeviceBlocksRequest = EvBegin + 7,
        EvReadDeviceBlocksResponse = EvBegin + 8,

        EvWriteDeviceBlocksRequest = EvBegin + 9,
        EvWriteDeviceBlocksResponse = EvBegin + 10,

        EvZeroDeviceBlocksRequest = EvBegin + 11,
        EvZeroDeviceBlocksResponse = EvBegin + 12,

        EvSecureEraseDeviceRequest = EvBegin + 13,
        EvSecureEraseDeviceResponse = EvBegin + 14,

        EvChecksumDeviceBlocksRequest = EvBegin + 15,
        EvChecksumDeviceBlocksResponse = EvBegin + 16,

        EvDisableConcreteAgentRequest = EvBegin + 17,
        EvDisableConcreteAgentResponse = EvBegin + 18,

        EvEnableAgentDeviceRequest = EvBegin + 19,
        EvEnableAgentDeviceResponse = EvBegin + 20,

        EvPartiallySuspendAgentRequest = EvBegin + 21,
        EvPartiallySuspendAgentResponse = EvBegin + 22,

        EvDirectCopyBlocksRequest = EvBegin + 23,
        EvDirectCopyBlocksResponse = EvBegin + 24,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::DISK_AGENT_END,
        "EvEnd expected to be < TBlockStoreEvents::DISK_AGENT_END");

    BLOCKSTORE_DISK_AGENT_REQUESTS_LOCAL(BLOCKSTORE_DECLARE_EVENTS)
    BLOCKSTORE_DISK_AGENT_REQUESTS_PROTO(BLOCKSTORE_DECLARE_PROTO_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeDiskAgentServiceId(ui32 nodeId);

}   // namespace NCloud::NBlockStore::NStorage
