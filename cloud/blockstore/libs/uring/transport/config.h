#pragma once

#include "public.h"

#include <cloud/storage/core/protos/uring.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct TTransportConfig
{
    ui32 IoThreads = 2;
    ui32 QueueSize = 4096;
    ui32 MaxInflightRequests = 32;
    ui64 MaxInflightBytes = 256ull << 20;
    ui32 MaxProtoSize = 64u << 10;
    ui32 MaxDataSize = 4u << 20;
    ui32 SendWindowBytes = 1u << 20;
    EOobMode OobMode = EOobMode::Auto;
    ui32 OobThreshold = 128u << 10;
    ESendZeroCopyMode SendZeroCopyMode = ESendZeroCopyMode::Auto;
    ui32 SendZeroCopyThreshold = 64u << 10;
    bool DirectSgIoEnabled = true;
    TDuration TcpUserTimeout = TDuration::Seconds(30);
    TDuration KeepAliveIdle = TDuration::Seconds(5);
    TDuration KeepAliveInterval = TDuration::Seconds(5);
    ui32 KeepAliveCount = 3;

    void DumpHtml(IOutputStream& out) const;
};

struct TClientConfig: TTransportConfig
{
    TDuration MaxResponseDelay = TDuration::Seconds(30);
    TDuration MaxReconnectDelay = TDuration::Seconds(30);

    void DumpHtml(IOutputStream& out) const;
};

struct TServerConfig: TTransportConfig
{
    ui32 Backlog = 128;
    TDuration DataTokenTtl = TDuration::Seconds(10);

    void DumpHtml(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

TClientConfig CreateClientConfig(const NProto::TUringClient& config);
TServerConfig CreateServerConfig(const NProto::TUringServer& config);

inline TClientConfigPtr CreateClientConfigPtr(
    const NProto::TUringClient& config)
{
    return std::make_shared<TClientConfig>(CreateClientConfig(config));
}

inline TServerConfigPtr CreateServerConfigPtr(
    const NProto::TUringServer& config)
{
    return std::make_shared<TServerConfig>(CreateServerConfig(config));
}

}   // namespace NCloud::NBlockStore::NUring
