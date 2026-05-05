#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/probes.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_IMPLEMENT_PRIVATE_REQUEST(name)                              \
    void TStorageServiceActor::Handle##name(                                   \
        const TEvIndexTablet::TEv##name##Request::TPtr& ev,                    \
        const TActorContext& ctx)                                              \
    {                                                                          \
        auto* msg = ev->Get();                                                 \
                                                                               \
        LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,                          \
            "#%lu forward " #name,                                             \
            msg->CallContext->RequestId);                                      \
                                                                               \
        auto [cookie, inflight] = CreateInFlightRequest(                       \
            TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),            \
            NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,                  \
            StatsRegistry->GetRequestStats(),                                  \
            ctx.Now());                                                        \
                                                                               \
        InitProfileLogRequestInfo(                                             \
            inflight->AccessProfileLogRequest(), msg->Record);                 \
                                                                               \
        auto event = std::make_unique<IEventHandle>(                           \
            MakeIndexTabletProxyServiceId(),                                   \
            SelfId(),                                                          \
            ev->ReleaseBase().Release(),                                       \
            0,                                                                 \
            cookie,                                                            \
            nullptr);                                                          \
                                                                               \
        ctx.Send(event.release());                                             \
    }                                                                          \
                                                                               \
    void TStorageServiceActor::Handle##name##Response(                         \
        const TEvIndexTablet::TEv##name##Response::TPtr& ev,                   \
        const TActorContext& ctx)                                              \
    {                                                                          \
        CompleteRequest<TEvIndexTablet::T##name##Method>(ctx, ev);             \
    }                                                                          \
// FILESTORE_IMPLEMENT_PRIVATE_REQUEST

FILESTORE_IMPLEMENT_PRIVATE_REQUEST(UnsafeCreateNode)
FILESTORE_IMPLEMENT_PRIVATE_REQUEST(UnsafeDeleteNode)
FILESTORE_IMPLEMENT_PRIVATE_REQUEST(UnsafeCreateNodeRef)
FILESTORE_IMPLEMENT_PRIVATE_REQUEST(UnsafeDeleteNodeRef)

#undef FILESTORE_IMPLEMENT_PRIVATE_REQUEST

////////////////////////////////////////////////////////////////////////////////

// Explicit instantiations of CompleteRequest for the private tablet methods.
template void TStorageServiceActor::CompleteRequest<
    TEvIndexTablet::TUnsafeCreateNodeMethod>(
    const TActorContext& ctx,
    const TEvIndexTablet::TEvUnsafeCreateNodeResponse::TPtr& ev);

template void TStorageServiceActor::CompleteRequest<
    TEvIndexTablet::TUnsafeDeleteNodeMethod>(
    const TActorContext& ctx,
    const TEvIndexTablet::TEvUnsafeDeleteNodeResponse::TPtr& ev);

template void TStorageServiceActor::CompleteRequest<
    TEvIndexTablet::TUnsafeCreateNodeRefMethod>(
    const TActorContext& ctx,
    const TEvIndexTablet::TEvUnsafeCreateNodeRefResponse::TPtr& ev);

template void TStorageServiceActor::CompleteRequest<
    TEvIndexTablet::TUnsafeDeleteNodeRefMethod>(
    const TActorContext& ctx,
    const TEvIndexTablet::TEvUnsafeDeleteNodeRefResponse::TPtr& ev);

}   // namespace NCloud::NFileStore::NStorage
