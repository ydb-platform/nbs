#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

class TCheckRangeActor: public TActorBootstrapped<TCheckRangeActor>
{
protected:
    const TActorId Tablet;
    const ui64 StartIndex;
    const ui64 BlocksCount;
    const TRequestInfoPtr RequestInfo;

public:
    TCheckRangeActor(
        const TActorId& tablet,
        ui64 startIndex,
        ui64 blocksCount,
        TRequestInfoPtr&& requestInfo);

    void Bootstrap(const TActorContext& ctx);

private:
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& status,
        const NProto::TError& error = {});

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    virtual void SendReadBlocksRequest(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

std::optional<NProto::TError> ValidateBlocksCount(
    ui64 blocksCount,
    ui64 bytesPerStripe,
    ui64 blockSize,
    ui64 checkRangeMaxRangeSize);

}   // namespace NCloud::NBlockStore::NStorage
