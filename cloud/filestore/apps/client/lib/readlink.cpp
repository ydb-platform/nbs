#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum class EModeType
{
    Unknown,
    Path,
    Node,
};

////////////////////////////////////////////////////////////////////////////////

class TReadLinkCommand final: public TFileStoreCommand
{
private:
    EModeType Mode = EModeType::Unknown;
    TString Path;
    ui64 NodeId = 0;

public:
    TReadLinkCommand()
    {
        const TString PathOptionName = "path";
        Opts.AddLongOption(PathOptionName)
            .RequiredArgument("PATH")
            .StoreResult(&Path)
            .Handler0([this] {
                Mode = EModeType::Path;
            });

        const TString NodeOptionName = "node";
        Opts.AddLongOption(NodeOptionName)
            .RequiredArgument("ID")
            .StoreResult(&NodeId)
            .Handler0([this] {
                Mode = EModeType::Node;
            });

        Opts.MutuallyExclusive(PathOptionName, NodeOptionName);
    }

    bool Execute() override
    {
        Y_ENSURE(Mode != EModeType::Unknown, "--path or --node must be specified");

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        ui64 nodeId;
        switch (Mode) {
            case EModeType::Path: {
                const auto resolved = ResolvePath(session, Path, false);
                nodeId = resolved.back().Node.GetId();
                break;
            }
            case EModeType::Node:
                nodeId = NodeId;
                break;
            case EModeType::Unknown:
                Y_ABORT("unreachable");
        }

        Cout << ReadLink(session, nodeId) << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadLinkCommand()
{
    return std::make_shared<TReadLinkCommand>();
}

}   // namespace NCloud::NFileStore::NClient
