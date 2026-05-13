#include "command.h"

#include <cloud/filestore/public/api/protos/node.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TUnsafeCreateNodeCommand final
    : public TFileStoreCommand
{
private:
    ui64 NodeId = 0;
    ui32 Type = NProto::E_REGULAR_NODE;
    ui32 Mode = 0;
    ui32 Uid = 0;
    ui32 Gid = 0;
    ui64 ATime = 0;
    ui64 MTime = 0;
    ui64 CTime = 0;
    ui64 Size = 0;
    ui32 Links = 0;
    ui64 DevId = 0;
    TString ShardId;
    TString ShardNodeName;

public:
    TUnsafeCreateNodeCommand()
    {
        Opts.AddLongOption("node-id")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&NodeId);

        Opts.AddLongOption("type")
            .RequiredArgument("NUM")
            .DefaultValue(static_cast<ui32>(NProto::E_REGULAR_NODE))
            .StoreResult(&Type);

        Opts.AddLongOption("mode")
            .RequiredArgument("NUM")
            .StoreResult(&Mode);

        Opts.AddLongOption("uid")
            .RequiredArgument("NUM")
            .StoreResult(&Uid);

        Opts.AddLongOption("gid")
            .RequiredArgument("NUM")
            .StoreResult(&Gid);

        Opts.AddLongOption("atime")
            .RequiredArgument("NUM")
            .StoreResult(&ATime);

        Opts.AddLongOption("mtime")
            .RequiredArgument("NUM")
            .StoreResult(&MTime);

        Opts.AddLongOption("ctime")
            .RequiredArgument("NUM")
            .StoreResult(&CTime);

        Opts.AddLongOption("size")
            .RequiredArgument("NUM")
            .StoreResult(&Size);

        Opts.AddLongOption("links")
            .RequiredArgument("NUM")
            .StoreResult(&Links);

        Opts.AddLongOption("dev-id")
            .RequiredArgument("NUM")
            .StoreResult(&DevId);

        Opts.AddLongOption("shard-id")
            .RequiredArgument("STR")
            .StoreResult(&ShardId);

        Opts.AddLongOption("shard-node-name")
            .RequiredArgument("STR")
            .StoreResult(&ShardNodeName);
    }

    bool Execute() override
    {
        auto request = CreateRequest<NProto::TUnsafeCreateNodeRequest>();

        auto* node = request->MutableNode();
        node->SetId(NodeId);
        node->SetType(Type);
        node->SetMode(Mode);
        node->SetUid(Uid);
        node->SetGid(Gid);
        node->SetATime(ATime);
        node->SetMTime(MTime);
        node->SetCTime(CTime);
        node->SetSize(Size);
        node->SetLinks(Links);
        node->SetDevId(DevId);
        node->SetShardFileSystemId(ShardId);
        node->SetShardNodeName(ShardNodeName);

        auto response = WaitFor(Client->UnsafeCreateNode(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);
        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TUnsafeCreateNodeRefCommand final
    : public TFileStoreCommand
{
private:
    ui64 ParentId = 0;
    TString Name;
    ui64 ChildId = 0;
    TString ShardId;
    TString ShardNodeName;

public:
    TUnsafeCreateNodeRefCommand()
    {
        Opts.AddLongOption("parent-id")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&ParentId);

        Opts.AddLongOption("name")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&Name);

        Opts.AddLongOption("child-id")
            .RequiredArgument("NUM")
            .StoreResult(&ChildId);

        Opts.AddLongOption("shard-id")
            .RequiredArgument("STR")
            .StoreResult(&ShardId);

        Opts.AddLongOption("shard-node-name")
            .RequiredArgument("STR")
            .StoreResult(&ShardNodeName);
    }

    bool Execute() override
    {
        auto request = CreateRequest<NProto::TUnsafeCreateNodeRefRequest>();
        request->SetParentId(ParentId);
        request->SetName(Name);
        request->SetChildId(ChildId);
        request->SetShardId(ShardId);
        request->SetShardNodeName(ShardNodeName);

        auto response = WaitFor(Client->UnsafeCreateNodeRef(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewUnsafeCreateNodeCommand()
{
    return std::make_shared<TUnsafeCreateNodeCommand>();
}

TCommandPtr NewUnsafeCreateNodeRefCommand()
{
    return std::make_shared<TUnsafeCreateNodeRefCommand>();
}

}   // namespace NCloud::NFileStore::NClient
