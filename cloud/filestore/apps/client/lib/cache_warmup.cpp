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

class TCacheWarmup final
    : public TFileStoreCommand
{
private:
    ui32 Depth = 0;

public:
    TCacheWarmup()
    {
        Opts.AddLongOption("depth")
            .RequiredArgument("NUM")
            .DefaultValue(1)
            .StoreResult(&Depth);
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
            request->SetCookie(cookie);

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

    void StatAll(
        ISession& session,
        const TString& fsId,
        TString prefix,
        ui64 parentId,
        ui32 depth)
    {
        --depth;
        auto response = ListAll(session, fsId, parentId);

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (node.GetType() == NProto::E_DIRECTORY_NODE && depth) {
                StatAll(
                    session,
                    fsId,
                    prefix + "/" + name,
                    node.GetId(),
                    depth);
            } else {
                Cout << prefix << "\t" << name << "\t" << node.GetId() << Endl;
            }
        }
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();
        StatAll(session, FileSystemId, "/", RootNodeId, Depth);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCacheWarmupCommand()
{
    return std::make_shared<TCacheWarmupCommand>();
}

}   // namespace NCloud::NFileStore::NClient
