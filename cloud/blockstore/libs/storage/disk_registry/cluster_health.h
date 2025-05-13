#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

class TDiskRegistryState;

////////////////////////////////////////////////////////////////////////////////

class TClusterHealth
{
private:
    const TDiskRegistryState* const Owner;
    THashSet<TString> Agents;
    THashSet<TString> DisconnectedAgents;
    THashSet<TString> UnavailableAgents;

public:
    explicit TClusterHealth(TDiskRegistryState* owner);
    ~TClusterHealth();

    void OnAgentDisconnected(const TString& agentId);
    void OnAgentConnected(const TString& agentId);
    void OnAgentStateChanged(const TString& agentId);

    [[nodiscard]] double GetDisconnectedAgentsRatio() const;

    [[nodiscard]] const THashSet<TString>& GetAgents() const {
        return Agents;
    }

    [[nodiscard]] const THashSet<TString>& GetDisconnectedAgents() const {
        return DisconnectedAgents;
    }

    [[nodiscard]] const THashSet<TString>& GetUnavailableAgents() const {
        return UnavailableAgents;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
