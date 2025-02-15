#include "write_back_cache_actor.h"

#include <cloud/filestore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

namespace NCloud::NFileStore {

using namespace NActors;

using namespace NCloud::NFileStore::NStorage;

////////////////////////////////////////////////////////////////////////////////

TWriteBackCacheActor::TWriteBackCacheActor()
    : TActor<TThis>(&TThis::StateWork)
{}

void TWriteBackCacheActor::HandleCreateHandle(
    const TEvService::TEvCreateHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleDestroyHandle(
    const TEvService::TEvDestroyHandleRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ctx.Send(ev->Forward(MakeStorageServiceId()));
}

void TWriteBackCacheActor::HandleWriteData(
    const TEvService::TEvWriteDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
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
