#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TClusterNode
{
private:
    ui32 Flags = 0;
    THashMap<TString, TString> Clients;

public:
    bool AddClient(const NProto::TClusterClient& client);
    bool RemoveClient(const TString& clientId);

    void ListClients(NProto::TListClusterClientsResponse& response) const;

    ui32 GetFlags() const
    {
        return Flags;
    }

    void SetFlags(ui32 flags)
    {
        Flags |= flags;
    }

    void ClearFlags(ui32 flags)
    {
        Flags &= ~flags;
    }

    size_t NumClients() const
    {
        return Clients.size();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCluster
{
private:
    THashMap<TString, TClusterNode> Nodes;

public:
    bool AddNode(const TString& nodeId);
    bool RemoveNode(const TString& nodeId);
    TClusterNode* FindNode(const TString& nodeId);

    void ListNodes(NProto::TListClusterNodesResponse& response) const;
};

}   // namespace NCloud::NFileStore
