#include "write_back_cache_actor.h"

namespace NCloud::NFileStore {

using namespace NActors;

using namespace NCloud::NFileStore::NStorage;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheActor::TWriteBackCacheActor(TString filePath)
    : TActor<TThis>(&TThis::StateWork)
    , FilePath(std::move(filePath))
{}

void TWriteBackCacheActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO(svartmetal): implement me.
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleDestroyHandle(
    const TEvService::TEvDestroyHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO(svartmetal): implement me.
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO(svartmetal): implement me.
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleWriteData(
    const TEvService::TEvWriteDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    // TODO(svartmetal): implement me.
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

STFUNC(TWriteBackCacheActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvService::TEvCreateHandleRequest, HandleCreateHandle);
        HFunc(TEvService::TEvDestroyHandleRequest, HandleDestroyHandle);
        HFunc(TEvService::TEvReadDataRequest, HandleReadData);
        HFunc(TEvService::TEvWriteDataRequest, HandleWriteData);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_PROXY);
            break;
    }
}

}   // namespace NCloud::NFileStore
