#pragma once

#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCopyRangeActor final
    : public NActors::TActorBootstrapped<TCopyRangeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TBlockRange64 Range;
    const NActors::TActorId Source;
    const NActors::TActorId Target;
    const TString WriterSessionId;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    TGuardedBuffer<TString> Buffer;
    TGuardedSgList SgList;
    NProto::TError Error;

    TInstant ReadStartTs;
    TDuration ReadDuration;
    TInstant WriteStartTs;
    TDuration WriteDuration;
    TVector<IProfileLog::TBlockInfo> AffectedBlockInfos;

public:
    TCopyRangeActor(
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TBlockRange64 range,
        NActors::TActorId source,
        NActors::TActorId target,
        TString writerSessionId,
        IBlockDigestGeneratorPtr blockDigestGenerator);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void ReadBlocks(const NActors::TActorContext& ctx);
    void WriteBlocks(const NActors::TActorContext& ctx);
    void Done(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleReadResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadUndelivery(
        const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteResponse(
        const TEvService::TEvWriteBlocksLocalResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteUndelivery(
        const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
