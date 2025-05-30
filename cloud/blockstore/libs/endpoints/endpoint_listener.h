#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/endpoints.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/blockstore/libs/client/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointListener
{
    virtual ~IEndpointListener() = default;

    virtual NThreading::TFuture<NProto::TError> StartEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) = 0;

    virtual NThreading::TFuture<NProto::TError> AlterEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) = 0;

    virtual NThreading::TFuture<NProto::TError> StopEndpoint(
        const TString& socketPath) = 0;

    virtual NProto::TError RefreshEndpoint(
        const TString& socketPath,
        const NProto::TVolume& volume) = 0;

    virtual NThreading::TFuture<NProto::TError> SwitchEndpoint(
        const NProto::TStartEndpointRequest& request,
        const NProto::TVolume& volume,
        NClient::ISessionPtr session) = 0;

    virtual NProto::TError CancelEndpointInFlightRequests(
        const TString& socketPath) = 0;
};

}   // namespace NCloud::NBlockStore::NServer
