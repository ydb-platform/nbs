#include "tablet_actor.h"

#include <cloud/storage/core/libs/common/future_helper.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_IMPLEMENT_ADAPTER_REQUEST_IN_ACTOR(name, ns)                 \
void TIndexTabletActor::HandleAdapter##name(                                   \
    const ns::TEv##name##Request::TPtr& ev,                                    \
    const NActors::TActorContext& ctx)                                         \
{                                                                              \
    auto* msg = ev->Get();                                                     \
    auto sender = ev->Sender;                                                  \
    ui64 cookie = ev->Cookie;                                                  \
    auto* ass = ctx.ActorSystem();                                             \
    FastShard->name(std::move(msg->Record)).Subscribe(                         \
        [=] (const auto& f) {                                                  \
            auto response = std::make_unique<ns::TEv##name##Response>(         \
                UnsafeExtractValue(f));                                        \
            ass->Send(sender, response.release(), 0 /* flags */, cookie);      \
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
