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
    TString FollowerFileSystemId;
    TString FollowerNodeName;
};

////////////////////////////////////////////////////////////////////////////////

class TFindGarbageCommand final
    : public TFileStoreCommand
{
private:
    TVector<TString> Shards;
    ui32 PageSize = 0;

public:
    TFindGarbageCommand()
    {
        Opts.AddLongOption("shard")
            .RequiredArgument("STR")
            .AppendTo(&Shards);

        Opts.AddLongOption("page-size")
            .RequiredArgument("STR")
            .StoreResult(&PageSize);
    }

    NProto::TListNodesResponse ListAll(const TString& fsId, ui64 parentId)
    {
        NProto::TListNodesResponse fullResult;
        TString cookie;
        do {
            auto request = CreateRequest<NProto::TListNodesRequest>();
            request->SetFileSystemId(fsId);
            request->SetNodeId(parentId);
            request->MutableHeaders()->SetDisableMultiTabletForwarding(true);
            if (PageSize) {
                request->SetMaxBytes(PageSize);
            }
            request->SetCookie(cookie);
            // TODO: async listing

            auto response = WaitFor(Client->ListNodes(
                PrepareCallContext(),
                std::move(request)));

            Y_ENSURE_EX(
                !HasError(response.GetError()),
                yexception() << "ListNodes error: "
                    << FormatError(response.GetError()));

            Y_ENSURE_EX(
                response.NamesSize() == response.NodesSize(),
                yexception() << "invalid ListNodes response: "
                    << response.DebugString().Quote());

            for (ui32 i = 0; i < response.NamesSize(); ++i) {
                fullResult.AddNames(*response.MutableNames(i));
                *fullResult.AddNodes() = std::move(*response.MutableNodes(i));
            }

            cookie = response.GetCookie();
        } while (cookie);

        return fullResult;
    }

    void FetchAll(
        const TString& fsId,
        ui64 parentId,
        TVector<TNode>* nodes)
    {
        // TODO: async listing
        auto response = ListAll(fsId, parentId);

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (node.GetType() == NProto::E_DIRECTORY_NODE) {
                FetchAll(fsId, node.GetId(), nodes);
            } else {
                nodes->push_back({
                    node.GetId(),
                    name,
                    node.GetFollowerFileSystemId(),
                    node.GetFollowerNodeName(),
                });
            }
        }
    }

    bool Exists(const TString& fsId, ui64 parentId, const TString& name)
    {
        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetFileSystemId(fsId);
        request->SetNodeId(parentId);
        request->SetName(name);

        auto response = WaitFor(Client->GetNodeAttr(
            PrepareCallContext(),
            std::move(request)));

        if (response.GetError().GetCode() == E_FS_NOENT) {
            return false;
        }

        Y_ENSURE_EX(
            !HasError(response.GetError()),
            yexception() << "GetNodeAttr error: "
                << FormatError(response.GetError()));

        return true;
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
            if (!node.FollowerFileSystemId) {
                continue;
            }

            followerNames.insert(node.FollowerNodeName);
        }

        for (const auto& [shard, nodes]: shard2Nodes) {
            for (const auto& node: nodes) {
                if (!followerNames.contains(node.Name)) {
                    if (Exists(shard, RootNodeId, node.Name)) {
                        Cout << shard << "\t" << node.Name << "\n";
                    }
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
