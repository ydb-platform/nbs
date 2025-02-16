#pragma once

#include <cloud/filestore/libs/storage/api/service.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TWriteBackCacheActor final
    : public NActors::TActor<TWriteBackCacheActor>
{
public:
    static constexpr const char ActorName[] =
        "NCloud::NFileStore::TWriteBackCacheActor";

private:
    TString FilePath;

public:
    TWriteBackCacheActor(TString filePath);

private:
    void HandleCreateHandle(
        const NStorage::TEvService::TEvCreateHandleRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleDestroyHandle(
        const NStorage::TEvService::TEvDestroyHandleRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadData(
        const NStorage::TEvService::TEvReadDataRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteData(
        const NStorage::TEvService::TEvWriteDataRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    STFUNC(StateWork);
};

}   // namespace NCloud::NFileStore
