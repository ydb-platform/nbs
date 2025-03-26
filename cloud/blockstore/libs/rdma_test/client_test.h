#pragma once

#include <cloud/blockstore/libs/rdma/iface/client.h>
#include <cloud/blockstore/libs/rdma/iface/protobuf.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaClientTest: NRdma::IClient
{
    struct TRdmaEndpointImpl;

    using TMessageObserver =
        std::function<void(NRdma::TProtoMessageSerializer::TParseResult&)>;

    struct TEndpointInfo
    {
        std::shared_ptr<TRdmaEndpointImpl> Endpoint;
        NThreading::TPromise<NRdma::IClientEndpointPtr> Promise;
    };

    THashMap<TString, TEndpointInfo> Endpoints;
    TMessageObserver MessageObserver;

    NThreading::TFuture<NRdma::IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) override;

    void Start() override
    {
    }

    void Stop() override
    {
    }

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }

    bool IsAlignedDataEnabled() const override
    {
        return false;
    }

    void InjectErrors(
        NProto::TError allocationError,
        NProto::TError rdmaResponseError,
        NProto::TError responseError);
    ui32 InitAllEndpoints();
    ui32 InitAllEndpointsWithError();

    void InjectWaitForFuture(const NThreading::TFuture<void>& waitForFuture);

    void SetMessageObserver(TMessageObserver messageObserver);
};

}   // namespace NCloud::NBlockStore::NStorage
