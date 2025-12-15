#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/verify.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

LWTRACE_USING(FILESTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoResponse>
void CalculateResponseChecksums(
    const TChecksumCalcInfo::TIovecs& iovecs,
    const TProtoResponse& response,
    ui32 blockSize,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    Y_UNUSED(iovecs);
    Y_UNUSED(response);
    Y_UNUSED(blockSize);
    Y_UNUSED(profileLogRequest);
}

void CalculateResponseChecksums(
    const TChecksumCalcInfo::TIovecs& iovecs,
    const NProto::TReadDataResponse& response,
    ui32 blockSize,
    NProto::TProfileLogRequestInfo& profileLogRequest)
{
    CalculateReadDataResponseChecksums(
        iovecs,
        response,
        blockSize,
        profileLogRequest);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

template<typename TMethod>
void TStorageServiceActor::CompleteRequest(
    const TActorContext& ctx,
    const typename TMethod::TResponse::TPtr& ev)
{
    auto* msg = ev->Get();

    auto request = FindInFlightRequest(ev->Cookie);
    if (!request) {
        LOG_CRIT(ctx, TFileStoreComponents::SERVICE,
            "failed to complete %s: invalid cookie (%d)",
            TMethod::Name,
            ev->Cookie);
        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "#%lu completed %s (%s)",
        request->CallContext->RequestId,
        TMethod::Name,
        FormatError(msg->Record.GetError()).c_str());

    const auto& checksumCalcInfo = request->GetChecksumCalcInfo();
    if (checksumCalcInfo.BlockChecksumsEnabled) {
        CalculateResponseChecksums(
            checksumCalcInfo.Iovecs,
            msg->Record,
            checksumCalcInfo.BlockSize,
            request->ProfileLogRequest);
    }

    FinalizeProfileLogRequestInfo(request->ProfileLogRequest, msg->Record);
    HandleTraceInfo(TraceSerializer, request->CallContext, msg->Record);
    HandleThrottlerInfo(*request->CallContext, msg->Record);

    STORAGE_VERIFY_C(
        ev->HasEvent(),
        TWellKnownEntityTypes::FILESYSTEM,
        request->CallContext->FileSystemId,
        "unexpected missing event before forwarding");
    TAutoPtr<IEventHandle> event = new IEventHandle(
        request->Sender,
        ev->Sender,
        ev->ReleaseBase().Release(),
        ev->Flags,
        request->Cookie,
        // undeliveredRequestActor
        nullptr);

    FILESTORE_TRACK(
        ResponseSent_Service,
        request->CallContext,
        TMethod::Name);

    const auto& error = msg->Record.GetError();
    request->Complete(ctx.Now(), error);
    ctx.Send(event);
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_IMPLEMENT_RESPONSE(name, ns)                                 \
    void TStorageServiceActor::Handle##name(                                   \
        const ns::TEv##name##Response::TPtr& ev,                               \
        const TActorContext& ctx)                                              \
    {                                                                          \
        CompleteRequest<ns::T##name##Method>(ctx, ev);                         \
    }

    FILESTORE_REMOTE_SERVICE(FILESTORE_IMPLEMENT_RESPONSE, TEvService)

#undef FILESTORE_IMPLEMENT_RESPONSE


}   // namespace NCloud::NFileStore::NStorage
