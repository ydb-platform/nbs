#include "service_actor.h"

#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/media.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/stream/str.h>

#include <cloud/storage/core/libs/xsl_render/xsl_render.h>

namespace {
    const char* xslTemplate =
    {
        #include "xsl_templates/service_actor_monitoring.xsl"
    };
};

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

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

    NXml::TDocument data("root", NXml::TDocument::RootName);
    
    auto root = data.Root();

    if (State) {
        root.AddChild("has_data", " ");

        RenderSessions(root);

        RenderLocalFileStores(root);

        if (StorageConfig) {
            auto configNode = root.AddChild("config_table", " ");
            StorageConfig->DumpXml(configNode);
        }
    }

    NCloud::NStorage::NXSLRender::NXSLRender(xslTemplate, data, out);

    NCloud::Reply(
        ctx,
        *ev,
        std::make_unique<NMon::TEvHttpInfoRes>(out.Str()));
}

void TStorageServiceActor::RenderSessions(NXml::TNode& root)
{
    auto sessions = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("sessions", " "));
    State->VisitSessions([&] (const TSessionInfo& session) {
        sessions.AddFieldIn("cd", " ")
            ("client_id", session.ClientId)
            ("tablet_id", session.TabletId)
            ("fs_id", session.FileStore.GetFileSystemId())
            ("session_id", session.SessionId);
    });
}

void TStorageServiceActor::RenderLocalFileStores(NXml::TNode& root)
{
    auto localFs = NCloud::NStorage::NTNodeWrapper::TFieldAdder(root.AddChild("local_filesystems", " "));
    for (const auto& [_, info]: State->GetLocalFileStores()) {
        localFs.AddFieldIn("cd", " ")
            ("tablet_id", info.TabletId)
            ("fs_id", info.FileStoreId)
            ("size", FormatByteSize(
                info.Config.GetBlocksCount() * info.Config.GetBlockSize()))
            ("media_kind", MediaKindToString(
                static_cast<NProto::EStorageMediaKind>(info.Config.GetStorageMediaKind())));
    }
}

}   // namespace NCloud::NFileStore::NStorage
