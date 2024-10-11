#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/format.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNode
{
    ui64 Id = 0;
    TString Name;
    TString ShardFileSystemId;
    TString ShardNodeName;
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
            .RequiredArgument("NUM")
            .StoreResult(&PageSize);
    }

    NProto::TListNodesResponse ListAll(
        ISession& session,
        const TString& fsId,
        ui64 parentId)
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

            auto response = WaitFor(session.ListNodes(
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
        ISession& session,
        const TString& fsId,
        ui64 parentId,
        TVector<TNode>* nodes)
    {
        // TODO: async listing
        auto response = ListAll(session, fsId, parentId);

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (node.GetType() == NProto::E_DIRECTORY_NODE) {
                FetchAll(session, fsId, node.GetId(), nodes);
            } else {
                nodes->push_back({
                    node.GetId(),
                    name,
                    node.GetShardFileSystemId(),
                    node.GetShardNodeName(),
                });
            }
        }
    }

    TMaybe<NProto::TNodeAttr> Stat(
        ISession& session,
        const TString& fsId,
        ui64 parentId,
        const TString& name)
    {
        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetFileSystemId(fsId);
        request->SetNodeId(parentId);
        request->SetName(name);

        auto response = WaitFor(session.GetNodeAttr(
            PrepareCallContext(),
            std::move(request)));

        if (response.GetError().GetCode() == E_FS_NOENT) {
            return {};
        }

        Y_ENSURE_EX(
            !HasError(response.GetError()),
            yexception() << "GetNodeAttr error: "
                << FormatError(response.GetError()));

        return std::move(*response.MutableNode());
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();
        TMap<TString, TVector<TNode>> shard2Nodes;
        for (const auto& shard: Shards) {
            auto shardSessionGuard =
                CreateCustomSession(shard, shard + "::" + ClientId);
            auto& shardSession = shardSessionGuard.AccessSession();
            FetchAll(shardSession, shard, RootNodeId, &shard2Nodes[shard]);
        }

        TVector<TNode> leaderNodes;
        FetchAll(session, FileSystemId, RootNodeId, &leaderNodes);

        THashSet<TString> shardNames;
        for (const auto& node: leaderNodes) {
            if (!node.ShardFileSystemId) {
                continue;
            }

            shardNames.insert(node.ShardNodeName);
        }

        struct TResult
        {
            TString Shard;
            TString Name;
            ui64 Size = 0;

            bool operator<(const TResult& rhs) const
            {
                const auto s = Max<ui64>() - Size;
                const auto rs = Max<ui64>() - rhs.Size;
                return std::tie(Shard, s, Name)
                    < std::tie(rhs.Shard, rs, rhs.Name);
            }
        };

        TVector<TResult> results;

        for (const auto& [shard, nodes]: shard2Nodes) {
            auto shardSessionGuard =
                CreateCustomSession(shard, shard + "::" + ClientId);
            auto& shardSession = shardSessionGuard.AccessSession();
            for (const auto& node: nodes) {
                if (!shardNames.contains(node.Name)) {
                    auto stat =
                        Stat(shardSession, shard, RootNodeId, node.Name);

                    if (stat) {
                        results.push_back({shard, node.Name, stat->GetSize()});
                    }
                }
            }
        }

        Sort(results.begin(), results.end());
        for (const auto& result: results) {
            Cout << result.Shard
                << "\t" << result.Name
                << "\t" << FormatByteSize(result.Size)
                << " (" << result.Size << ")"
                << "\n";
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
