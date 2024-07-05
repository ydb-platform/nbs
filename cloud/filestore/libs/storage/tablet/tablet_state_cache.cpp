#include "tablet_state_cache.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryIndexState::ReadNode(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    Y_UNUSED(nodeId, commitId, node);
    return false;
}

//
// Nodes_Ver
//

bool TInMemoryIndexState::ReadNodeVer(
    ui64 nodeId,
    ui64 commitId,
    TMaybe<TNode>& node)
{
    Y_UNUSED(nodeId, commitId, node);
    return false;
}

//
// NodeAttrs
//

bool TInMemoryIndexState::ReadNodeAttr(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    Y_UNUSED(nodeId, commitId, name, attr);
    return false;
}

bool TInMemoryIndexState::ReadNodeAttrs(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    Y_UNUSED(nodeId, commitId, attrs);
    return false;
}

//
// NodeAttrs_Ver
//

bool TInMemoryIndexState::ReadNodeAttrVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeAttr>& attr)
{
    Y_UNUSED(nodeId, commitId, name, attr);
    return false;
}

bool TInMemoryIndexState::ReadNodeAttrVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeAttr>& attrs)
{
    Y_UNUSED(nodeId, commitId, attrs);
    return false;
}

//
// NodeRefs
//

bool TInMemoryIndexState::ReadNodeRef(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    Y_UNUSED(nodeId, commitId, name, ref);
    return false;
}

bool TInMemoryIndexState::ReadNodeRefs(
    ui64 nodeId,
    ui64 commitId,
    const TString& cookie,
    TVector<TNodeRef>& refs,
    ui32 maxBytes,
    TString* next)
{
    Y_UNUSED(nodeId, commitId, cookie, refs, maxBytes, next);
    return false;
}

bool TInMemoryIndexState::PrechargeNodeRefs(
    ui64 nodeId,
    const TString& cookie,
    ui32 bytesToPrecharge)
{
    Y_UNUSED(nodeId, cookie, bytesToPrecharge);
    return false;
}

//
// NodeRefs_Ver
//

bool TInMemoryIndexState::ReadNodeRefVer(
    ui64 nodeId,
    ui64 commitId,
    const TString& name,
    TMaybe<TNodeRef>& ref)
{
    Y_UNUSED(nodeId, commitId, name, ref);
    return false;
}

bool TInMemoryIndexState::ReadNodeRefVers(
    ui64 nodeId,
    ui64 commitId,
    TVector<TNodeRef>& refs)
{
    Y_UNUSED(nodeId, commitId, refs);
    return false;
}

//
// CheckpointNodes
//

bool TInMemoryIndexState::ReadCheckpointNodes(
    ui64 checkpointId,
    TVector<ui64>& nodes,
    size_t maxCount)
{
    Y_UNUSED(checkpointId, nodes, maxCount);
    return false;
}

}   // namespace NCloud::NFileStore::NStorage
