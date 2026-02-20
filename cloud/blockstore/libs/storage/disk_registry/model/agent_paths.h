#pragma once

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TAgentsPaths
{
    using TPathsByAgentId = THashMap<TString, THashSet<TString>>;

private:
    TPathsByAgentId PathsToAttach;

public:
    void AddPathToAttach(const TString& agentId, const TString& path);
    void DeletePathToAttach(const TString& agentId, const TString& path);
    void DeleteAgent(const TString& agentId);
    [[nodiscard]] const TPathsByAgentId& GetPathsToAttach() const;
};

}   // namespace NCloud::NBlockStore::NStorage
