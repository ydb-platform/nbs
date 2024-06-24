#pragma once
#include "public.h"

#include <cloud/filestore/libs/storage/tablet/tablet_database.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Stores a subset of the actual index state for caching purposes.
class TInMemoryIndexState: public IIndexTabletDatabase
{
public:
    TInMemoryIndexState() = default;

    //
    // Nodes
    //

    bool ReadNode(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) override;

    //
    // NodeAttrs
    //

    bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;

    bool ReadNodeAttrs(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeRefs
    //

    bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;

    bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next) override;

    //
    // Nodes_Ver
    //

    bool ReadNodeVer(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) override;

    //
    // NodeAttrs_Ver
    //

    bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) override;

    bool ReadNodeAttrVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeAttr>& attrs) override;

    //
    // NodeRefs_Ver
    //

    bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) override;

    bool ReadNodeRefVers(
        ui64 nodeId,
        ui64 commitId,
        TVector<TNodeRef>& refs) override;
};

}   // namespace NCloud::NFileStore::NStorage
