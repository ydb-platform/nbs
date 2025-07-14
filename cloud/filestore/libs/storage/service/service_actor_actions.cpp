#include "service_actor.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto ActionNotFoundResponse()
{
    return std::make_unique<TEvService::TEvExecuteActionResponse>(
        MakeError(E_ARGUMENT, "No suitable action found"));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleExecuteAction(
    const TEvService::TEvExecuteActionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();
    auto& request = msg->Record;
    auto& action = *request.MutableAction();
    action.to_lower();
    auto& input = *request.MutableInput();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    using TActorBuilderMethod =
        NActors::IActorPtr (TStorageServiceActor::*)(TRequestInfoPtr, TString);
    static const THashMap<TString, TActorBuilderMethod> actions = {
        {
            "draintablets",
            &TStorageServiceActor::CreateDrainTabletActionActor
        },
        {
            "getstorageconfigfields",
            &TStorageServiceActor::CreateGetStorageConfigFieldsActionActor
        },
        {
            "changestorageconfig",
            &TStorageServiceActor::CreateChangeStorageConfigActionActor
        },
        {
            "describesessions",
            &TStorageServiceActor::CreateDescribeSessionsActionActor
        },
        {
            "forcedoperation",
            &TStorageServiceActor::CreateForcedOperationActionActor
        },
        {
            "forcedoperationstatus",
            &TStorageServiceActor::CreateForcedOperationStatusActionActor
        },
        {
            "reassigntablet",
            &TStorageServiceActor::CreateReassignTabletActionActor
        },
        {
            "configureshards",
            &TStorageServiceActor::CreateConfigureShardsActionActor
        },
        {
            "configureasshard",
            &TStorageServiceActor::CreateConfigureAsShardActionActor
        },
        {
            "getstorageconfig",
            &TStorageServiceActor::CreateGetStorageConfigActionActor
        },
        {
            "writecompactionmap",
            &TStorageServiceActor::CreateWriteCompactionMapActionActor
        },
        {
            "unsafedeletenode",
            &TStorageServiceActor::CreateUnsafeDeleteNodeActionActor
        },
        {
            "unsafeupdatenode",
            &TStorageServiceActor::CreateUnsafeUpdateNodeActionActor
        },
        {
            "unsafegetnode",
            &TStorageServiceActor::CreateUnsafeGetNodeActionActor
        },
        {
            "getstoragestats",
            &TStorageServiceActor::CreateGetStorageStatsActionActor
        },
        {
            "listlocalfilestores",
            &TStorageServiceActor::CreateListLocalFileStoresActionActor,
        },
        {
            "restarttablet",
            &TStorageServiceActor::CreateRestartTabletActionActor
        },
        {
            "getfilesystemtopology",
            &TStorageServiceActor::CreateGetFileSystemTopologyActionActor
        },
        {
            "readnoderefs",
            &TStorageServiceActor::CreateReadNodeRefsActionActor
        },
    };

    auto it = actions.find(action);
    if (it == actions.end()) {
        NCloud::Reply(ctx, *requestInfo, ActionNotFoundResponse());
        return;
    }
    auto actorBuilder = it->second;
    auto actor = std::invoke(actorBuilder, this, requestInfo, std::move(input));
    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
