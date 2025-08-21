#pragma once

#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRdmaAsyncTestEndpoint;

class TRdmaAsyncTestServer: public NRdma::IServer
{
private:
    THashMap<TString, std::shared_ptr<TRdmaAsyncTestEndpoint> > Endpoints;

public:
    NRdma::IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        NRdma::IServerHandlerPtr handler) override;

    NRdma::IServerEndpointPtr StartEndpointOnInterface(
        TString interface,
        ui32 port,
        NRdma::IServerHandlerPtr handler) override;

    void Start() override
    {}

    void Stop() override
    {}

    void DumpHtml(IOutputStream& out) const override
    {
        Y_UNUSED(out);
    }

    NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> Run(
        TString host,
        ui32 port,
        NProto::TWriteDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TReadDeviceBlocksResponse> Run(
        TString host,
        ui32 port,
        NProto::TReadDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TChecksumDeviceBlocksResponse> Run(
        TString host,
        ui32 port,
        NProto::TChecksumDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TZeroDeviceBlocksResponse> Run(
        TString host,
        ui32 port,
        NProto::TZeroDeviceBlocksRequest request);
};

}   // namespace NCloud::NBlockStore::NStorage
