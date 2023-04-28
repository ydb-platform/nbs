#pragma once

#include "public.h"

#include "storage_with_stats.h"

#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(xxx, ...)                       \
    xxx(RegisterAgent,              __VA_ARGS__)                               \
    xxx(CollectStats,               __VA_ARGS__)                               \
// BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskAgentPrivate
{
    //
    // InitAgent
    //

    struct TInitAgentCompleted
    {
        TVector<NProto::TDeviceConfig> Configs;
        TVector<TString> Errors;

        TInitAgentCompleted() = default;

        TInitAgentCompleted(
                TVector<NProto::TDeviceConfig> configs,
                TVector<TString> errors)
            : Configs(std::move(configs))
            , Errors(std::move(errors))
        {}
    };

    //
    // RegisterAgent
    //

    struct TRegisterAgentRequest
    {};

    struct TRegisterAgentResponse
    {};

    //
    // CollectStats
    //

    struct TCollectStatsRequest
    {};

    struct TCollectStatsResponse
    {
        NProto::TAgentStats Stats;

        TCollectStatsResponse() = default;

        explicit TCollectStatsResponse(
                NProto::TAgentStats stats)
            : Stats(std::move(stats))
        {}
    };

    //
    // SecureErase
    //

    struct TSecureEraseCompleted
    {
        TString DeviceId;

        TSecureEraseCompleted() = default;

        explicit TSecureEraseCompleted(TString deviceId)
            : DeviceId(std::move(deviceId))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::DISK_AGENT_START,

        BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvInitAgentCompleted,
        EvSecureEraseCompleted,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStorePrivateEvents::DISK_AGENT_END,
        "EvEnd expected to be < TBlockStorePrivateEvents::DISK_AGENT_END");

    BLOCKSTORE_DISK_AGENT_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)

    using TEvInitAgentCompleted = TResponseEvent<
        TInitAgentCompleted,
        EvInitAgentCompleted>;

    using TEvSecureEraseCompleted = TResponseEvent<
        TSecureEraseCompleted,
        EvSecureEraseCompleted>;
};

}   // namespace NCloud::NBlockStore::NStorage
