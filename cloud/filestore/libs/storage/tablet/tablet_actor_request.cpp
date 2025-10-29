#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <ydb/library/actors/core/log.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
TSession* TIndexTabletActor::AcceptRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx,
    const std::function<NProto::TError(
        const typename TMethod::TRequest::ProtoRecordType&)>& validator,
    bool validateSession)
{
    auto* msg = ev->Get();
    auto& request = msg->Record;

    msg->CallContext->RequestId = GetRequestId(request);
    if (!msg->CallContext->LWOrbit.HasShuttles()) {
        msg->CallContext->SetRequestStartedCycles(GetCycleCount());
        TraceSerializer->HandleTraceRequest(
            request.GetHeaders().GetInternal().GetTrace(),
            msg->CallContext->LWOrbit);
    }

    Metrics.BusyIdleCalc.OnRequestStarted();

    FILESTORE_TRACK(
        RequestReceived_Tablet,
        msg->CallContext,
        TMethod::Name,
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        TMethod::Name,
        DumpMessage(request).c_str());

    NProto::TError error;
    if (validator) {
        error = validator(request);
    }

    if (FAILED(error.GetCode())) {
        auto response = std::make_unique<typename TMethod::TResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
        return nullptr;
    }

    if (!validateSession) {
        return nullptr;
    }

    auto* session = FindSession(
         GetClientId(request),
         GetSessionId(request),
         GetSessionSeqNo(request));

    if (!session) {
        auto response = std::make_unique<typename TMethod::TResponse>(
            ErrorInvalidSession(
                GetClientId(request),
                GetSessionId(request),
                GetSessionSeqNo(request)));
        NCloud::Reply(ctx, *ev, std::move(response));
        return nullptr;
    }

    return session;
}

template <typename TMethod>
void TIndexTabletActor::CompleteResponse(
    typename TMethod::TResponse::ProtoRecordType& response,
    const TCallContextPtr& callContext,
    const NActors::TActorContext& ctx)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s %s: #%lu completed (%s)",
        LogTag.c_str(),
        TMethod::Name,
        callContext->RequestId,
        FormatError(response.GetError()).c_str());

    if (HasError(response.GetError())) {
        auto* e = response.MutableError();
        e->SetMessage(TStringBuilder()
            << e->GetMessage() << ", request-id: " << callContext->RequestId);
    }

    FILESTORE_TRACK(
        ResponseSent_Tablet,
        callContext,
        TMethod::Name);

    BuildTraceInfo(TraceSerializer, callContext, response);
    BuildThrottlerInfo(*callContext, response);

    Metrics.BusyIdleCalc.OnRequestCompleted();
}

#define FILESTORE_GENERATE_IMPL(name, ns)                                             \
template TSession* TIndexTabletActor::AcceptRequest<ns::T##name##Method>(             \
    const ns::T##name##Method::TRequest::TPtr& ev,                                    \
    const TActorContext& ctx,                                                         \
    const std::function<NProto::TError(                                               \
        const typename ns::T##name##Method::TRequest::ProtoRecordType&)>& validator,  \
    bool validateSession);                                                            \
                                                                                      \
template void TIndexTabletActor::CompleteResponse<ns::T##name##Method>(               \
    ns::TEv##name##Response::ProtoRecordType& response,                               \
    const TCallContextPtr& callContext,                                               \
    const NActors::TActorContext& ctx);                                               \
// FILESTORE_IMPL_VALIDATE

FILESTORE_REMOTE_SERVICE(FILESTORE_GENERATE_IMPL, TEvService)
FILESTORE_GENERATE_IMPL(DescribeData, TEvIndexTablet)
FILESTORE_GENERATE_IMPL(DescribeSessions, TEvIndexTablet)
FILESTORE_GENERATE_IMPL(GenerateBlobIds, TEvIndexTablet)
FILESTORE_GENERATE_IMPL(AddData, TEvIndexTablet)
FILESTORE_GENERATE_IMPL(GetNodeAttrBatch, TEvIndexTablet)
FILESTORE_GENERATE_IMPL(RenameNodeInDestination, TEvIndexTablet)

#undef FILESTORE_GENERATE_IMPL

}   // namespace NCloud::NFileStore::NStorage
