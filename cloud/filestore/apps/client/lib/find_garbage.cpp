#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNode
{
    ui64 Id = 0;
    TString Name;
    TString FollowerId;
    TString FollowerName;
};

////////////////////////////////////////////////////////////////////////////////

class TFindGarbageCommand final
    : public TFileStoreCommand
{
private:
    TVector<TString> Shards;

public:
    TFindGarbageCommand()
    {
        Opts.AddLongOption("shard")
            .RequiredArgument("STR")
            .AppendTo(&Shards);
    }

    void FetchAll(
        const TString& fsId,
        ui64 parentId,
        TVector<TNode>* nodes)
    {
        auto request = CreateRequest<NProto::TListNodesRequest>();
        request->SetNodeId(parentId);
        request->MutableHeaders()->SetDisableMultiTabletForwarding(true);
        // TODO: traverse all pages
        // TODO: async listing

        auto response = WaitFor(Client->ListNodes(
            PrepareCallContext(),
            std::move(request)));
        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (node.GetType() == NProto::E_DIRECTORY_NODE) {
                FetchAll(fsId, node.GetId(), nodes);
            } else if (node.GetType() == NProto::E_REGULAR_NODE) {
                nodes->push_back({
                    node.GetId(),
                    name,
                    node.GetFollowerFileSystemId(),
                    node.GetFollowerNodeName(),
                });
            }

            // TODO: support hardlinks
        }
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        TMap<TString, TVector<TNode>> shard2Nodes;
        for (const auto& shard: Shards) {
            FetchAll(shard, RootNodeId, &shard2Nodes[shard]);
        }

        TVector<TNode> leaderNodes;
        FetchAll(FileSystemId, RootNodeId, &leaderNodes);

        THashSet<TString> followerNames;
        for (const auto& node: leaderNodes) {
            if (!node.FollowerId) {
                continue;
            }

            followerNames.insert(node.FollowerName);
        }

        for (const auto& [shard, nodes]: shard2Nodes) {
            for (const auto& node: nodes) {
                if (!followerNames.contains(node.Name)) {
                    // TODO GetNodeAttr in shard to check whether this node
                    // still exists
                    Cout << shard << "\t" << node.Name << "\n";
                }
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewFindGarbageCommand()
{
    return std::make_shared<TFindGarbageCommand>();
}

}   // namespace NCloud::NFileStore::NClient
