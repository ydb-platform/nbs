#include "helpers.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// This interface contains a subset of the methods that can be performed over
// the localDB tables. Throughout the code two implementations are supported:
// one for the localDB access, and one for the in-memory cache.
class IIndexState
{
public:
    struct TNode
    {
        ui64 NodeId;
        NProto::TNode Attrs;
        ui64 MinCommitId;
        ui64 MaxCommitId;
    };

    struct TNodeRef
    {
        ui64 NodeId;
        TString Name;
        ui64 ChildNodeId;
        TString FollowerId;
        TString FollowerName;
        ui64 MinCommitId;
        ui64 MaxCommitId;
    };

    struct TNodeAttr
    {
        ui64 NodeId;
        TString Name;
        TString Value;
        ui64 MinCommitId;
        ui64 MaxCommitId;
        ui64 Version;
    };

public:
    virtual ~IIndexState() = default;

    //
    // Nodes
    //

    virtual bool ReadNode(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) = 0;

    //
    // NodeAttrs
    //

    virtual bool ReadNodeAttr(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool
    ReadNodeAttrs(ui64 nodeId, ui64 commitId, TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeRefs
    //

    virtual bool ReadNodeRef(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool ReadNodeRefs(
        ui64 nodeId,
        ui64 commitId,
        const TString& cookie,
        TVector<TNodeRef>& refs,
        ui32 maxBytes,
        TString* next) = 0;

    //
    // Nodes_Ver
    //

    virtual bool
    ReadNodeVer(ui64 nodeId, ui64 commitId, TMaybe<TNode>& node) = 0;

    //
    // NodeAttrs_Ver
    //

    virtual bool ReadNodeAttrVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeAttr>& attr) = 0;

    virtual bool
    ReadNodeAttrVers(ui64 nodeId, ui64 commitId, TVector<TNodeAttr>& attrs) = 0;

    //
    // NodeRefs_Ver
    //

    virtual bool ReadNodeRefVer(
        ui64 nodeId,
        ui64 commitId,
        const TString& name,
        TMaybe<TNodeRef>& ref) = 0;

    virtual bool
    ReadNodeRefVers(ui64 nodeId, ui64 commitId, TVector<TNodeRef>& refs) = 0;
};

}   // namespace NCloud::NFileStore::NStorage
