#include "service_actor.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/storage/core/libs/xsl_render/xsl_render.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/resource/resource.h>

#include <util/stream/str.h>
#include <util/string/cast.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

namespace {

class THttpFindFileSystemActor final
    : public TActorBootstrapped<THttpFindFileSystemActor>
{
private:
    const TActorId ActorID;
    const TString FileSystemId;

public:
    THttpFindFileSystemActor(
            const TActorId& actorID,
            TString fileSystemId)
        : ActorID(actorID)
        , FileSystemId(std::move(fileSystemId))
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(FileSystemId));

        Become(&TThis::StateWork);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvSSProxy::TEvDescribeFileStoreResponse, HandleDescribeResponse);

            default:
                HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE);
                break;
        }
    }

    void HandleDescribeResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto& error = msg->GetError();

        TString out;

        if (FAILED(error.GetCode())) {
            Notify(ctx, HandleError(ctx, error, msg->Path));
        } else {
            const auto& pathDescr = msg->PathDescription;
            const auto& FileSystemTabletID =
                pathDescr.GetFileStoreDescription().GetIndexTabletId();
            Notify(ctx, BuildHtmlResponse(FileSystemTabletID, msg->Path));
        }

        Die(ctx);
    }

    void Notify(const TActorContext& ctx, const TString& html)
    {
        auto response = std::make_unique<NMon::TEvHttpInfoRes>(html);
        NCloud::Send(ctx, ActorID, std::move(response));
    }

    TString HandleError(
        const TActorContext& ctx,
        const NProto::TError& error,
        const TString& path)
    {
        TStringStream out;
        out << "Could not resolve filesystem path " << path.Quote()
            << ": " << FormatError(error);

        LOG_ERROR(ctx, TFileStoreComponents::SERVICE, out.Str());
        return out.Str();
    }

    TString BuildHtmlResponse(ui64 tabletId, const TString& path)
    {
        using namespace NCloud::NStorage::NXSLRender;

        TStringStream out;

        NXml::TDocument data("root", NXml::TDocument::RootName);
    
        auto root = data.Root();
        root.AddChild("path", path);
        root.AddChild("tablet_id", tabletId);

        NXSLRender(NResource::Find("xslt/filestore/storage/service/search").c_str(), data, out);

        return out.Str();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleHttpInfo_Search(
    const NMon::TEvHttpInfo::TPtr& ev,
    const TString& FileSystemId,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TFileStoreComponents::SERVICE,
        "Search FileSystem for id: %s",
        FileSystemId.Quote().data());

    NCloud::Register<THttpFindFileSystemActor>(
        ctx,
        ev->Sender,
        FileSystemId);
}

}   // namespace NCloud::NFileStore::NStorage
