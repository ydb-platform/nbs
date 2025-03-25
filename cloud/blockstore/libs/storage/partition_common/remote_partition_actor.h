#pragma once

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/rdma/iface/config.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

class TRemotePartitionActor
    : public NActors::TActorBootstrapped<TRemotePartitionActor>
{
    const TString Host;
    const NRdma::TRdmaConfigPtr Config;
    const size_t BlockSize;
    const NRdma::IClientPtr RdmaClient;

    NThreading::TFuture<NRdma::IClientEndpointPtr> FutureEndpoint;
    NRdma::IClientEndpointPtr Endpoint;

public:
    TRemotePartitionActor(
        TString remoteHost,
        NRdma::TRdmaConfigPtr config,
        NRdma::IClientPtr rdmaClient,
        size_t blockSize);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    template <typename TMethod>
    TResultOrError<NRdma::TClientRequestPtr> InitRequest(
        TMethod::TRequest::ProtoRecordType& proto,
        const TSgList& data,
        size_t additionalSpaceForResponseData,
        int msgId,
        NRdma::IClientHandlerPtr handler);

    NRdma::IClientEndpointPtr GetEndpoint();

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);

    template <typename TMethod>
    void HandleReadMethod(
        const TMethod::TRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ReadBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocks, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(WriteBlocksLocal, TEvService);
    BLOCKSTORE_IMPLEMENT_REQUEST(ZeroBlocks, TEvService);
};

}   // namespace NCloud::NBlockStore::NStorage
