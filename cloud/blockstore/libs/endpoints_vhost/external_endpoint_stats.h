#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>

#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/json/json_value.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TEndpointStats
{
    TString ClientId;
    TString DiskId;

    IServerStatsPtr ServerStats;

    void Update(const NJson::TJsonValue& stats);
};

}   // namespace NCloud::NBlockStore::NServer
