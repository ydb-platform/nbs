#pragma once

#include "public.h"

#include <cloud/blockstore/config/rdma.pb.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

class TRdmaConfig
{
private:
    const NProto::TRdmaConfig Config;

public:
    explicit TRdmaConfig(NProto::TRdmaConfig config = {});

    auto GetClientEnabled() const
    {
        return Config.GetClientEnabled();
    }

    auto GetClient() const
    {
        return Config.GetClient();
    }

    auto GetServerEnabled() const
    {
        return Config.GetServerEnabled();
    }

    auto GetServer() const
    {
        return Config.GetServer();
    }
};

}   // namespace NCloud::NBlockStore::NRdma
