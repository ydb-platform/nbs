#pragma once

#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TRdmaServerTest: NRdma::IServer
{
    struct TEndpointInfo
    {
        NRdma::IServerEndpointPtr Endpoint;
    };

    THashMap<TString, TEndpointInfo> Endpoints;

    NRdma::IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        NRdma::IServerHandlerPtr handler) override;

    NProto::TChecksumDeviceBlocksResponse ProcessRequest(
        TString host,
        ui32 port,
        NProto::TChecksumDeviceBlocksRequest request);
    NProto::TReadDeviceBlocksResponse ProcessRequest(
        TString host,
        ui32 port,
        NProto::TReadDeviceBlocksRequest request);
    NProto::TWriteDeviceBlocksResponse ProcessRequest(
        TString host,
        ui32 port,
        NProto::TWriteDeviceBlocksRequest request);
    NProto::TZeroDeviceBlocksResponse ProcessRequest(
        TString host,
        ui32 port,
        NProto::TZeroDeviceBlocksRequest request);

    void Start() override
    {
    }

    void Stop() override
    {
    }
};

}   // namespace NCloud::NBlockStore::NStorage
