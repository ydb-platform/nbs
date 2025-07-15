#include "config_printer.h"

#include <cloud/blockstore/libs/kikimr/components.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/protos/console_config.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NActors;

namespace {
using namespace NKikimr::NConsole;

class TConfigPrinterActor: public TActorBootstrapped<TConfigPrinterActor>
{
public:
    TConfigPrinterActor() = default;

    void Bootstrap(const TActorContext& ctx)
    {
        SendSubscriptionRequest(ctx);
        Become(&TConfigPrinterActor::StateWork);
    }

    void SendSubscriptionRequest(const TActorContext& ctx)
    {
        auto* req = new TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest;
        auto blockstoreConfig = static_cast<ui32>(
            NKikimrConsole::TConfigItem::BlockstoreConfigItem);
        req->ConfigItemKinds = {
            blockstoreConfig,
        };
        ctx.Send(MakeConfigsDispatcherID(ctx.SelfID.NodeId()), req);
    }

    void Handle(
        TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& /*unused*/,
        const TActorContext& ctx)
    {
        LOG_INFO(ctx, TBlockStoreComponents::SERVICE, __PRETTY_FUNCTION__);
    }

    void Handle(
        TEvConsole::TEvConfigNotificationRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto& rec = ev->Get()->Record;

        TString message;
        google::protobuf::util::MessageToJsonString(rec.GetConfig().GetBlockstoreConfig(), &message);
        LOG_INFO(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Got config from Config Dispatcher!\n%s",
            message.c_str());

        auto* resp = new TEvConsole::TEvConfigNotificationResponse;
        resp->Record.SetSubscriptionId(rec.GetSubscriptionId());
        resp->Record.MutableConfigId()->CopyFrom(rec.GetConfigId());
        ctx.Send(ev->Sender, resp, 0, ev->Cookie);
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse,
                Handle);
            HFunc(TEvConsole::TEvConfigNotificationRequest, Handle);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateConfigPrinter()
{
    return std::make_unique<TConfigPrinterActor>();
}

}   // namespace NCloud::NBlockStore::NStorage
