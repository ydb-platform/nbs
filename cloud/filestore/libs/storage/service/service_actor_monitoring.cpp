#include "service_actor.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

void BuildSearchButton(IOutputStream& out)
{
    out <<
        "<form method=\"GET\" id=\"fsSearch\" name=\"fsSearch\">\n"
        "Filesystem: <input type=\"text\" id=\"Filesystem\" name=\"Filesystem\"/>\n"
        "<input class=\"btn btn-primary\" type=\"submit\" value=\"Search\"/>\n"
        "<input type='hidden' name='action' value='search'/>"
        "</form>\n";
}

////////////////////////////////////////////////////////////////////////////////

void DumpFsLink(IOutputStream& out, const ui64 tabletId, const TString& fsId)
{
    out << "<a href='../tablets?TabletID=" << tabletId << "'>"
        << fsId << "</a>";
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleHttpInfo(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Request;
    TString uri{request.GetUri()};
    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "HTTP request: %s", uri.c_str());

    const auto& params = (request.GetMethod() != HTTP_METHOD_POST) ?
        request.GetParams() : request.GetPostParams();

    const auto& filesystemId = params.Get("Filesystem");
    const auto& action = params.Get("action");

    if (filesystemId && (action == "search")) {
        HandleHttpInfo_Search(ev, filesystemId, ctx);
        return;
    }

    TStringStream out;
    if (State) {
        HTML(out) {
            TAG(TH3) { out << "Search Filesystem by id"; }
            BuildSearchButton(out);

            TAG(TH3) { out << "Local Sessions"; }
            RenderSessions(out);

            TAG(TH3) { out << "Local Filesystems"; }
            RenderLocalFileStores(out);

            TAG(TH3) { out << "Config"; }
            StorageConfig->DumpHtml(out);
        }
    } else {
        out << "State not ready yet" << Endl;
    }

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

void TStorageServiceActor::RenderSessions(IOutputStream& out)
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "ClientId"; }
                    TABLEH() { out << "FileSystemId"; }
                    TABLEH() { out << "SessionId"; }
                }
            }

            State->VisitSessions([&] (const TSessionInfo& session) {
                TABLER() {
                    TABLED() { out << session.ClientId; }
                    TABLED() {
                        DumpFsLink(
                            out,
                            session.TabletId,
                            session.FileStore.GetFileSystemId()
                        );
                    }
                    TABLED() { out << session.SessionId; }
                }
            });
        }
    }
}

void TStorageServiceActor::RenderLocalFileStores(IOutputStream& out)
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "FileStore"; }
                    TABLEH() { out << "Tablet"; }
                    TABLEH() { out << "Size"; }
                    TABLEH() { out << "Media kind"; }
                }
            }

            for (const auto& [_, info]: State->GetLocalFileStores()) {
                TABLER() {
                    TABLED() {
                        out << "<a href='../tablets?TabletID="
                            << info.TabletId
                            << "'>"
                            << info.FileStoreId
                            << "</a>";
                    }
                    TABLED() {
                        out << "<a href='../tablets?TabletID="
                            << info.TabletId
                            << "'>"
                            << info.TabletId
                            << "</a>";
                    }
                    TABLED() {
                        out << FormatByteSize(
                            info.Config.GetBlocksCount() * info.Config.GetBlockSize());
                    }
                    TABLED() {
                        out << MediaKindToString(
                            static_cast<NProto::EStorageMediaKind>(
                                info.Config.GetStorageMediaKind()));
                    }
                }
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
