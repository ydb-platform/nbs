#include "node_index_cache.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TNodeIndexCache::TNodeIndexCache(IAllocator* allocator)
{
    Y_UNUSED(allocator);
}

TNodeIndexCacheStats TNodeIndexCache::GetStats() const
{
    TNodeIndexCacheStats stats;
    stats.NodeCount = AttrByParentNodeId.size();
    return stats;
}

void TNodeIndexCache::Reset(ui32 maxNodes)
{
    KeyByNodeId.clear();
    AttrByParentNodeId.clear();
    MaxNodes = maxNodes;
}

void TNodeIndexCache::InvalidateCache(ui64 parentNodeId, const TString& name)
{
    auto it = AttrByParentNodeId.find(TNodeIndexCacheKey(parentNodeId, name));
    if (it != AttrByParentNodeId.end()) {
        KeyByNodeId.erase(it->second.GetId());
        AttrByParentNodeId.erase(it);
    }
}

void TNodeIndexCache::InvalidateCache(ui64 nodeId)
{
    auto key = KeyByNodeId.find(nodeId);
    if (key != KeyByNodeId.end()) {
        AttrByParentNodeId.erase(key->second);
        KeyByNodeId.erase(nodeId);
    }
}

void TNodeIndexCache::RegisterGetNodeAttrResult(
    ui64 parentNodeId,
    const TString& name,
    const NProto::TNodeAttr& response)
{
    if (MaxNodes == 0) {
        return;
    }
    if (AttrByParentNodeId.size() == MaxNodes) {
        KeyByNodeId.clear();
        AttrByParentNodeId.clear();
    }

    auto key = TNodeIndexCacheKey(parentNodeId, name);
    AttrByParentNodeId[key] = response;
    KeyByNodeId.emplace(response.GetId(), key);
}

bool TNodeIndexCache::TryFillGetNodeAttrResult(
    ui64 parentNodeId,
    const TString& name,
    NProto::TNodeAttr* response)
{
    auto it = AttrByParentNodeId.find(TNodeIndexCacheKey(parentNodeId, name));
    if (it == AttrByParentNodeId.end()) {
        return false;
    }
    response->CopyFrom(it->second);
    return true;
}

}   // namespace NCloud::NFileStore::NStorage
