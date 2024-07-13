#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStartEndpointCommand final
    : public TEndpointCommand
{
private:
    TString FileSystemId;
    TString SocketPath;
    TString ClientId;
    bool MountReadOnly = false;
    ui64 MountSeqNo = 0;
    bool Persistent = false;
    TString ServiceEndpoint;

public:
    TStartEndpointCommand()
    {
        Opts.AddLongOption("filesystem")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&FileSystemId);

        Opts.AddLongOption("socket-path")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&SocketPath);

        Opts.AddLongOption("client-id")
            .Optional()
            .RequiredArgument("STR")
            .StoreResult(&ClientId);

        Opts.AddLongOption("mount-readonly")
            .NoArgument()
            .SetFlag(&MountReadOnly);

        Opts.AddLongOption("mount-seqno")
            .RequiredArgument("NUM")
            .StoreResult(&MountSeqNo);

        Opts.AddLongOption("persistent")
            .NoArgument()
            .SetFlag(&Persistent);

        Opts.AddLongOption("service-endpoint")
            .Optional()
            .RequiredArgument("STR")
            .StoreResult(&ServiceEndpoint);

    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TStartEndpointRequest>();

        auto* config = request->MutableEndpoint();
        config->SetFileSystemId(FileSystemId);
        config->SetSocketPath(SocketPath);
        config->SetClientId(ClientId);
        config->SetReadOnly(MountReadOnly);
        config->SetMountSeqNumber(MountSeqNo);
        config->SetPersistent(Persistent);
        config->SetServiceEndpoint(ServiceEndpoint);

        auto response = WaitFor(
            Client->StartEndpoint(
                std::move(callContext),
                std::move(request)));

        if (HasError(response)) {
            ythrow TServiceError(response.GetError());
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStartEndpointCommand()
{
    return std::make_shared<TStartEndpointCommand>();
}

}   // namespace NCloud::NFileStore::NClient
