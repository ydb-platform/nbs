#pragma once

#include <cloud/blockstore/libs/rdma/client.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaClientTest: NRdma::IClient
{
    struct TEndpointInfo
    {
        NRdma::IClientEndpointPtr Endpoint;
        NThreading::TPromise<NRdma::IClientEndpointPtr> Promise;
    };

    THashMap<TString, TEndpointInfo> Endpoints;

    NThreading::TFuture<NRdma::IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        NRdma::IClientHandlerPtr handler) override;

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void InjectErrors(
        NProto::TError allocationError,
        NProto::TError rdmaResponseError,
        NProto::TError responseError);
    ui32 InitAllEndpoints();
    ui32 InitAllEndpointsWithError();
};

}   // namespace NCloud::NBlockStore::NStorage
