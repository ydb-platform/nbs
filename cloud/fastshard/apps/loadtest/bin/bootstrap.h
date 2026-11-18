#pragma once

#include "private.h"

#include <cloud/fastshard/libs/sn/client/durable.h>
#include <cloud/fastshard/libs/sn/iface/storage_node.h>

namespace NCloud::NFastShard::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
private:
    const TOptionsPtr Options;

    // Client to the storage node under test; it retries failed requests.
    TDurableClientMetricsPtr ClientMetrics;
    IStorageNodePtr Client;

    // The silk runtime is up, so Stop has to destroy it.
    bool RuntimeStarted = false;

public:
    explicit TBootstrap(TOptionsPtr options);
    ~TBootstrap();

    void Init();
    void Start();
    void Stop();

    TOptionsPtr GetOptions()
    {
        return Options;
    }

    IStorageNodePtr GetClient()
    {
        return Client;
    }

    TDurableClientMetricsPtr GetClientMetrics()
    {
        return ClientMetrics;
    }
};

}   // namespace NCloud::NFastShard::NLoadTest
