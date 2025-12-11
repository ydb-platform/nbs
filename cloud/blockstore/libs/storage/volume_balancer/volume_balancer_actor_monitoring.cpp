#include "volume_balancer_actor.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TVolumeBalancerActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Request;

    TString uri{request.GetUri()};
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::VOLUME_BALANCER,
        "HTTP request: %s",
        uri.c_str());

    TStringStream out;

    if (State) {
        State->RenderHtml(out, ctx.Now());
    }

    NCloud::Reply(ctx, *ev, std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

}   // namespace NCloud::NBlockStore::NStorage
