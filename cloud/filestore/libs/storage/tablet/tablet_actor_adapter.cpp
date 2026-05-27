#include "tablet_actor.h"

#include <cloud/storage/core/libs/common/future_helper.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void UpdateAdapterMetrics(TTabletMetrics& m, ui64 requestBytes, TDuration d)
{
    Y_UNUSED(m, requestBytes, d);
}

#define ADAPTER_METRICS_REQUESTS_PUBLIC(xxx, ...)                              \
    xxx(ReadData,                                       __VA_ARGS__)           \
    xxx(WriteData,                                      __VA_ARGS__)           \
    xxx(GetNodeAttr,                                    __VA_ARGS__)           \
    xxx(CreateHandle,                                   __VA_ARGS__)           \
    xxx(DestroyHandle,                                  __VA_ARGS__)           \
    xxx(CreateNode,                                     __VA_ARGS__)           \
    xxx(UnlinkNode,                                     __VA_ARGS__)           \
    xxx(GetNodeXAttr,                                   __VA_ARGS__)           \
// ADAPTER_METRICS_REQUESTS_PUBLIC

#define DECLARE_UPDATE_ADAPTER_METRICS(name, ns)                               \
template <>                                                                    \
void UpdateAdapterMetrics<ns::T##name##Method>(                                \
    TTabletMetrics& m,                                                         \
    ui64 requestBytes,                                                         \
    TDuration d)                                                               \
{                                                                              \
    m.name.Update(1, requestBytes, d);                                         \
}                                                                              \
// DECLARE_UPDATE_ADAPTER_METRICS

ADAPTER_METRICS_REQUESTS_PUBLIC(DECLARE_UPDATE_ADAPTER_METRICS, TEvService)

#undef DECLARE_UPDATE_ADAPTER_METRICS

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR(name, ns)                 \
void TIndexTabletActor::HandleAdapter##name(                                   \
    const ns::TEv##name##Request::TPtr& ev,                                    \
    const NActors::TActorContext& ctx)                                         \
{                                                                              \
    auto* msg = ev->Get();                                                     \
    LOG_TRACE(ctx, TFileStoreComponents::TABLET,                               \
        "%s " Y_STRINGIZE(name) " request %s",                                 \
        LogTag.c_str(),                                                        \
        msg->Record.ShortUtf8DebugString().Quote().c_str());                   \
    using TMethod = ns::T##name##Method;                                       \
    const bool accepted = AcceptRequestNoSession<TMethod>(                     \
        ev,                                                                    \
        ctx,                                                                   \
        [] (const TMethod::TRequest::ProtoRecordType&) {                       \
            return MakeError(S_OK);                                            \
        });                                                                    \
    if (!accepted) {                                                           \
        return;                                                                \
    }                                                                          \
                                                                               \
    TInstant startedTs = ctx.Now();                                            \
    const ui64 requestBytes = CalculateByteCount(msg->Record);                 \
    auto sender = ev->Sender;                                                  \
    ui64 cookie = ev->Cookie;                                                  \
    auto* ass = ctx.ActorSystem();                                             \
    auto callContext = msg->CallContext;                                       \
    FastShard->name(std::move(msg->Record)).Subscribe(                         \
        [=] (const auto& f) {                                                  \
            /* TODO(#5894): ensure that tablet actor is still alive */         \
            auto response = std::make_unique<ns::TEv##name##Response>(         \
                UnsafeExtractValue(f));                                        \
                                                                               \
            bool builtTraceInfo = false;                                       \
            CompleteResponse<TMethod>(                                         \
                response->Record,                                              \
                callContext,                                                   \
                &builtTraceInfo);                                              \
                                                                               \
            LOG_DEBUG(*ass, TFileStoreComponents::TABLET,                      \
                "%s %s: #%lu completed (%s), trace-info: %d",                  \
                LogTag.c_str(),                                                \
                TMethod::Name,                                                 \
                callContext->RequestId,                                        \
                FormatError(response->Record.GetError()).c_str(),              \
                builtTraceInfo);                                               \
            LOG_TRACE(*ass, TFileStoreComponents::TABLET,                      \
                "%s " Y_STRINGIZE(name) " response %s",                        \
                LogTag.c_str(),                                                \
                response->Record.ShortUtf8DebugString().Quote().c_str());      \
                                                                               \
            ass->Send(sender, response.release(), 0 /* flags */, cookie);      \
                                                                               \
            UpdateAdapterMetrics<TMethod>(                                     \
                Metrics,                                                       \
                requestBytes,                                                  \
                ass->Timestamp() - startedTs);                                 \
        });                                                                    \
}                                                                              \
// FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR

FILESTORE_SERVICE_ADAPTER_REQUESTS(
    FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR,
    TEvService)
FILESTORE_TABLET_ADAPTER_REQUESTS(
    FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR,
    TEvIndexTablet)

#undef FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR

}   // namespace NCloud::NFileStore::NStorage
