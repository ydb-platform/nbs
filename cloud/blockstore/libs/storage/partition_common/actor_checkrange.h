#pragma once

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

class TCheckRangeActor: public NActors::TActorBootstrapped<TCheckRangeActor>
{
protected:
    const NActors::TActorId Partition;
    const NProto::TCheckRangeRequest Request;
    const TRequestInfoPtr RequestInfo;
    const ui64 BlockSize;
    TGuardedBuffer<TString> Buffer;
    TGuardedSgList SgList;

public:
    TCheckRangeActor(
        const NActors::TActorId& partition,
        NProto::TCheckRangeRequest&& request,
        TRequestInfoPtr requestInfo,
        ui64 blockSize);

    void Bootstrap(const NActors::TActorContext& ctx);

protected:
    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        std::unique_ptr<TEvVolume::TEvCheckRangeResponse>);

    virtual void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    virtual void SendReadBlocksRequest(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

NProto::TError ValidateBlocksCount(
    ui64 blocksCount,
    ui64 bytesPerStripe,
    ui64 blockSize,
    ui64 checkRangeMaxRangeSize);

}   // namespace NCloud::NBlockStore::NStorage
