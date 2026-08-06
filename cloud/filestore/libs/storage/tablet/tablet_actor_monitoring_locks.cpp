#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/simple_template.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/str.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NActors::NMon;

namespace {

////////////////////////////////////////////////////////////////////////////////

void DumpLocksPage(IOutputStream& out, ui64 tabletId)
{
    OutputTemplate(NResource::Find("html/locks-main.html"), {
        {"STYLE", NResource::Find("css/locks.css")},
        {"JS", NResource::Find("js/locks.js")},
        {"TABLET_ID", ToString(tabletId)}}, out);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleHttpInfo_Locks(
    const NActors::TActorContext& ctx,
    const TCgiParameters& params,
    TRequestInfoPtr requestInfo)
{
    if (params.Get("getContent") != "1") {
        TStringStream out;
        DumpLocksPage(out, TabletID());

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<NMon::TEvRemoteHttpInfoRes>(std::move(out.Str())));
        return;
    }

    TStringStream out;
    NJsonWriter::TBuf writer(NJsonWriter::HEM_DONT_ESCAPE_HTML, &out);
    writer.BeginObject();
    writer.WriteKey("locks");
    writer.BeginList();
    VisitNodeRefLocks([&] (const TNodeRefKey& x) {
        writer.BeginObject();
        writer.WriteKey("parentNodeId");
        writer.WriteString(ToString(x.ParentNodeId));
        writer.WriteKey("name");
        writer.WriteString(x.Name);
        writer.EndObject();
    });
    writer.EndList();
    writer.EndObject();

    NCloud::Reply(
        ctx,
        *requestInfo,
        std::make_unique<NMon::TEvRemoteJsonInfoRes>(std::move(out.Str())));
}

}   // namespace NCloud::NFileStore::NStorage
