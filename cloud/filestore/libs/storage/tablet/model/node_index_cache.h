#pragma once

#include "public.h"

#include "node_ref.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

#include <util/generic/hash.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TNodeIndexCacheStats
{
    ui64 NodeCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeIndexCache
{
private:
    THashMap<ui64, TNodeRefKey> KeyByNodeId;
    THashMap<TNodeRefKey, NProto::TNodeAttr, TNodeRefKeyHash>
        AttrByParentNodeId;

    ui32 MaxNodes = 0;

public:
    explicit TNodeIndexCache(IAllocator* allocator);

    [[nodiscard]] TNodeIndexCacheStats GetStats() const;

    void Reset(ui32 maxNodes);

    void InvalidateCache(ui64 parentNodeId, const TString& name);

    void InvalidateCache(ui64 nodeId);

    void RegisterGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        const NProto::TNodeAttr& response);

    bool TryFillGetNodeAttrResult(
        ui64 parentNodeId,
        const TString& name,
        NProto::TNodeAttr* response);
};

}   // namespace NCloud::NFileStore::NStorage
