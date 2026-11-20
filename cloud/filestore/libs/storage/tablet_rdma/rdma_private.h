#pragma once

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_RDMA_TABLET_REQUESTS(xxx, ...)                               \
    xxx(DescribeData,               __VA_ARGS__)                               \
    xxx(GenerateBlobIds,            __VA_ARGS__)                               \
    xxx(AddData,                    __VA_ARGS__)                               \
    xxx(ConfirmAddData,             __VA_ARGS__)                               \
    xxx(CancelAddData,              __VA_ARGS__)                               \
    xxx(GetNodeAttrBatch,           __VA_ARGS__)                               \
    xxx(ListNodesInternal,          __VA_ARGS__)                               \
// FILESTORE_RDMA_TABLET_REQUESTS

// The requests allowed over RDMA; the rest, CreateSession included, stay on
// the pipe.
#define FILESTORE_RDMA_REQUESTS(xxx)                                           \
    FILESTORE_SERVICE_REQUESTS(xxx, TEvService)                                \
    FILESTORE_RDMA_TABLET_REQUESTS(xxx, TEvIndexTablet)                        \
// FILESTORE_RDMA_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaTabletPrivate
{
    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::RDMA_START,

        EvTrackRequest = EvBegin + 1,
        EvTabletGone,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TFileStoreEventsPrivate::RDMA_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::RDMA_END");

    // Precedes the request that the RDMA request handler sends to the tablet
    // with the same cookie; Promise gets the tablet's typed response.
    struct TEvTrackRequest
        : public NActors::TEventLocal<TEvTrackRequest, EvTrackRequest>
    {
        const NActors::TActorId TabletActorId;
        NThreading::TPromise<TTabletTransportResponse> Promise;

        TEvTrackRequest(
                const NActors::TActorId& tabletActorId,
                NThreading::TPromise<TTabletTransportResponse> promise)
            : TabletActorId(tabletActorId)
            , Promise(std::move(promise))
        {}

        ~TEvTrackRequest()
        {
            // the event was dropped before the forwarder took the promise
            if (Promise.Initialized() && !Promise.HasValue()) {
                ReportTabletRdmaCritEvent(
                    TStringBuilder()
                    << "RDMA request was dropped before the forwarder took it"
                    << ": tablet " << TabletActorId.ToString());
                Promise.SetValue(TTabletTransportResponse(
                    MakeError(E_REJECTED, "rdma forwarder unavailable")));
            }
        }
    };

    // The tablet actor left the RDMA tablet registry.
    struct TEvTabletGone
        : public NActors::TEventLocal<TEvTabletGone, EvTabletGone>
    {
        const NActors::TActorId TabletActorId;

        explicit TEvTabletGone(const NActors::TActorId& tabletActorId)
            : TabletActorId(tabletActorId)
        {}
    };
};

}   // namespace NCloud::NFileStore::NStorage
