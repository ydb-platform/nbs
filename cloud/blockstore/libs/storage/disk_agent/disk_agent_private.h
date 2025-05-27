#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

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
        TVector<TString> ConfigMismatchErrors;
        TVector<TString> DevicesWithSuspendedIO;

        TInitAgentCompleted() = default;

        TInitAgentCompleted(
                TVector<NProto::TDeviceConfig> configs,
                TVector<TString> errors,
                TVector<TString> configMismatchErrors,
                TVector<TString> devicesWithSuspendedIO)
            : Configs(std::move(configs))
            , Errors(std::move(errors))
            , ConfigMismatchErrors(std::move(configMismatchErrors))
            , DevicesWithSuspendedIO(std::move(devicesWithSuspendedIO))
        {}
    };

    //
    // RegisterAgent
    //

    struct TRegisterAgentRequest
    {};

    struct TRegisterAgentResponse
    {
        TVector<TString> DevicesToDisableIO;
    };

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
    // TWriteOrZeroCompleted
    //

    struct TWriteOrZeroCompleted
    {
        ui64 RequestId = 0;
        TBlockRange64 Range;
        TString DeviceUUID;
        bool Success = false;

        TWriteOrZeroCompleted(
            ui64 requestId,
            TBlockRange64 range,
            TString deviceUUID,
            bool success)
            : RequestId(requestId)
            , Range(range)
            , DeviceUUID(std::move(deviceUUID))
            , Success(success)
        {}
    };

    //
    // TReportDelayedDiskAgentConfigMismatch
    //

    struct TReportDelayedDiskAgentConfigMismatch
    {
        TString ErrorText;

        explicit TReportDelayedDiskAgentConfigMismatch(TString errorText)
            : ErrorText(std::move(errorText))
        {}
    };

    //
    // UpdateSessionCache
    //

    struct TUpdateSessionCacheRequest
    {
        TVector<NProto::TDiskAgentDeviceSession> Sessions;

        TUpdateSessionCacheRequest() = default;
        explicit TUpdateSessionCacheRequest(
                TVector<NProto::TDiskAgentDeviceSession> sessions)
            : Sessions(std::move(sessions))
        {}
    };

    struct TUpdateSessionCacheResponse
    {};


    struct TCancelSuspensionRequest
    {};

    //
    // ParsedWriteDeviceBlocksRequest
    //

    struct TParsedWriteDeviceBlocksRequest
    {
        NProto::TWriteDeviceBlocksRequest Record;
        TStorageBuffer Storage;
        ui64 StorageSize = 0;
    };

    // The response for TWriteDeviceBlocksRequest that should be executed on
    // multiple DiskAgents (contains replication targets). The
    // TMultiAgentWriteDeviceBlocksResponse is not transmitted through the actor
    // system.
    struct TMultiAgentWriteDeviceBlocksResponse
    {
        NProto::TError Error;
        TVector<NProto::TError> ReplicationResponses;

        TMultiAgentWriteDeviceBlocksResponse() = default;

        TMultiAgentWriteDeviceBlocksResponse(TErrorResponse&& error)
            : Error(std::move(error))
        {}

        static bool HasError()
        {
            return true;
        }

        const NProto::TError& GetError() const
        {
            return Error;
        }
    };

    //
    // MultiAgentWriteDeviceBlocksRequest
    //

    struct TMultiAgentWriteDeviceBlocksRequest
    {
        NProto::TWriteDeviceBlocksRequest Record;

        NThreading::TPromise<TMultiAgentWriteDeviceBlocksResponse>
            ResponsePromise;
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
        EvWriteOrZeroCompleted,
        EvReportDelayedDiskAgentConfigMismatch,
        EvCancelSuspensionRequest,

        EvParsedReadDeviceBlocksRequest,
        EvParsedWriteDeviceBlocksRequest,
        EvParsedZeroDeviceBlocksRequest,

        EvMultiAgentWriteDeviceBlocksRequest,

        BLOCKSTORE_DECLARE_EVENT_IDS(UpdateSessionCache)

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

    using TEvWriteOrZeroCompleted = TResponseEvent<
        TWriteOrZeroCompleted,
        EvWriteOrZeroCompleted>;

    using TEvReportDelayedDiskAgentConfigMismatch = TResponseEvent<
        TReportDelayedDiskAgentConfigMismatch,
        EvReportDelayedDiskAgentConfigMismatch>;

    using TEvCancelSuspensionRequest = TRequestEvent<
        TCancelSuspensionRequest,
        EvCancelSuspensionRequest>;

    using TEvParsedWriteDeviceBlocksRequest = TRequestEvent<
        TParsedWriteDeviceBlocksRequest,
        EvParsedWriteDeviceBlocksRequest>;

    using TEvMultiAgentWriteDeviceBlocksRequest = TRequestEvent<
        TMultiAgentWriteDeviceBlocksRequest,
        EvMultiAgentWriteDeviceBlocksRequest>;

    BLOCKSTORE_DECLARE_EVENTS(UpdateSessionCache)
};

// IMultiAgentWriteHandler interface defines a method to perform write blocks
// using multiple agents, returning a future with the response.
class IMultiAgentWriteHandler
{
public:
    virtual ~IMultiAgentWriteHandler() = default;

    virtual NThreading::TFuture<
        TEvDiskAgentPrivate::TMultiAgentWriteDeviceBlocksResponse>
    PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) = 0;
};

using IMultiAgentWriteHandlerPtr = std::shared_ptr<IMultiAgentWriteHandler>;

}   // namespace NCloud::NBlockStore::NStorage
