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
    ui64 ParentId = 0;
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
    bool FindOrphansInShards = true;
    bool FindOrphansInLeader = false;

public:
    TFindGarbageCommand()
    {
        Opts.AddLongOption("shard")
            .RequiredArgument("STR")
            .AppendTo(&Shards);

        Opts.AddLongOption("page-size")
            .RequiredArgument("NUM")
            .StoreResult(&PageSize)
            .DefaultValue(0);

        Opts.AddLongOption("find-in-shards")
            .RequiredArgument("BOOL")
            .StoreResult(&FindOrphansInShards)
            .DefaultValue(true);

        Opts.AddLongOption("find-in-leader")
            .RequiredArgument("BOOL")
            .StoreResult(&FindOrphansInLeader)
            .DefaultValue(false);
    }

    void FetchAll(
        ISession& session,
        const TString& fsId,
        ui64 parentId,
        TVector<TNode>* nodes)
    {
        // TODO: async listing
        auto response = ListAll(session, fsId, parentId, true, PageSize);

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (node.GetType() == NProto::E_DIRECTORY_NODE) {
                FetchAll(session, fsId, node.GetId(), nodes);
            } else {
                nodes->push_back({
                    node.GetId(),
                    parentId,
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
            auto& shardNodes = shard2Nodes[shard];
            STORAGE_INFO("Fetching nodes for shard " << shard);
            FetchAll(shardSession, shard, RootNodeId, &shardNodes);
            STORAGE_INFO("Fetched " << shardNodes.size() << " nodes");
        }

        STORAGE_INFO("Fetching nodes for leader");
        TVector<TNode> leaderNodes;
        FetchAll(session, FileSystemId, RootNodeId, &leaderNodes);
        STORAGE_INFO("Fetched " << leaderNodes.size() << " nodes");

        THashSet<TString> shardNodeNames;
        for (const auto& node: leaderNodes) {
            if (!node.ShardFileSystemId) {
                continue;
            }

            shardNodeNames.insert(node.ShardNodeName);
        }

        if (FindOrphansInShards) {
            struct TResult
            {
                TString Shard;
                TString NodeName;
                ui64 Size = 0;

                bool operator<(const TResult& rhs) const
                {
                    const auto s = Max<ui64>() - Size;
                    const auto rs = Max<ui64>() - rhs.Size;
                    return std::tie(Shard, s, NodeName) <
                           std::tie(rhs.Shard, rs, rhs.NodeName);
                }
            };

            TVector<TResult> results;

            for (const auto& [shard, nodes]: shard2Nodes) {
                auto shardSessionGuard =
                    CreateCustomSession(shard, shard + "::" + ClientId);
                auto& shardSession = shardSessionGuard.AccessSession();
                for (const auto& node: nodes) {
                    if (!shardNodeNames.contains(node.Name)) {
                        STORAGE_INFO(
                            "Node " << node.Name << " found in shard " << shard
                                    << " but not in leader, calling stat");
                        auto stat =
                            Stat(shardSession, shard, RootNodeId, node.Name);
                        STORAGE_INFO("Stat done");

                        if (stat) {
                            results.push_back(
                                {shard, node.Name, stat->GetSize()});
                        }
                    }
                }
            }

            Sort(results.begin(), results.end());
            for (const auto& result: results) {
                Cout << result.Shard << "\t" << result.NodeName << "\t"
                     << FormatByteSize(result.Size) << " (" << result.Size
                     << ")"
                     << "\n";
            }
        }

        if (FindOrphansInLeader) {
            // Find nodes that are present in leader but not in shards

            TVector<TNode> results;
            THashMap<TString, THashSet<TString>> shard2NodeNames;
            for (const auto& [shard, nodes]: shard2Nodes) {
                for (const auto& node: nodes) {
                    shard2NodeNames[shard].emplace(node.Name);
                }
            }

            THashMap<TString, TVector<TNode>> shard2LeaderNodes;
            for (const auto& node: leaderNodes) {
                if (node.ShardFileSystemId) {
                    shard2LeaderNodes[node.ShardFileSystemId].push_back(node);
                }
            }

            for (const auto& [shard, nodes]: shard2LeaderNodes) {
                auto shardSessionGuard =
                    CreateCustomSession(shard, shard + "::" + ClientId);
                auto& shardSession = shardSessionGuard.AccessSession();

                for (const auto& node: nodes) {
                    if (!shard2NodeNames.contains(node.ShardFileSystemId) ||
                        !shard2NodeNames[node.ShardFileSystemId].contains(
                            node.ShardNodeName))
                    {
                        STORAGE_INFO(
                            "Node "
                            << node.Name << " found in leader in directory "
                            << node.ParentId << " but shard "
                            << node.ShardFileSystemId << " does not contain "
                            << node.ShardNodeName << ". Calling stat");

                        auto stat = Stat(
                            shardSession,
                            node.ShardFileSystemId,
                            RootNodeId,
                            node.ShardNodeName);
                        if (!stat) {
                            results.push_back(node);
                        }
                    }
                }
            }

            Sort(
                results.begin(),
                results.end(),
                [](const auto& lhs, const auto& rhs)
                {
                    return std::tie(lhs.ParentId, lhs.Name) <
                           std::tie(rhs.ParentId, rhs.Name);
                });

            for (const auto& result: results) {
                Cout << result.ParentId << "\t" << result.Name << "\t"
                     << result.ShardFileSystemId << "\t" << result.ShardNodeName
                     << "\n";
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
