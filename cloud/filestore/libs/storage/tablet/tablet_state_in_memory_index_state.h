#pragma once
#include "public.h"

#include "tablet_state_iface.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief Stores the state of the index tables in memory. Can be used to perform
 * read-only operations.
 */
class TInMemoryIndexState : public IIndexTabletDatabase
{
public:
    TInMemoryIndexState() = default;

    //
    // Nodes
    //

    bool ReadNode(ui64 nodeId, ui64 commitId, TMaybe<IIndexTabletDatabase::TNode>& node) override;

    //
    // Nodes_Ver
    //

    bool ReadNodeVer(ui64 nodeId, ui64 commitId, TMaybe<IIndexTabletDatabase::TNode>& node) override;

    //
    // NodeAttrs
    //

    bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeAttr>& attr) override;

    bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeAttr>& attrs) override;

    //
    // NodeAttrs_Ver
    //

    bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeAttr>& attr) override;

    bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeAttr>& attrs) override;

    //
    // NodeRefs
    //

    bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeRef>& ref) override;

    bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<IIndexTabletDatabase::TNodeRef>& refs,
        ui32 maxBytes,
        TString* next = nullptr) override;

    bool PrechargeNodeRefs(
        ui64 nodeId,
        const TString& cookie,
        ui32 bytesToPrecharge) override;

    //
    // NodeRefs_Ver
    //

    bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<IIndexTabletDatabase::TNodeRef>& ref) override;

    bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<IIndexTabletDatabase::TNodeRef>& refs) override;

    //
    // CheckpointNodes
    //

    bool ReadCheckpointNodes(
        ui64 checkpointId,
        TVector<ui64>& nodes,
        size_t maxCount) override;
};

}   // namespace NCloud::NFileStore::NStorage
