#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/ping.pb.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct TPingResponseInfo
{
    TString Host;
    ui16 Port;
    NProto::TPingResponse Record;
    TDuration Time;
};

////////////////////////////////////////////////////////////////////////////////

struct IPingClient: IStartable
{
    virtual ~IPingClient() = default;

    virtual NThreading::TFuture<TPingResponseInfo>
    Ping(TString host, ui16 port, TDuration timeout) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IPingClientPtr CreateSecurePingClient(const TString& rootCertFile);
IPingClientPtr CreateInsecurePingClient();

}   // namespace NCloud::NBlockStore::NDiscovery
