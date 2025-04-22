#include "command.h"

#include <cloud/filestore/public/api/protos/session.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

void Print(const NProto::TDestroySessionResponse& response, bool jsonOutput)
{
    if (jsonOutput) {
        Cout << response.AsJSON() << Endl;
    } else {
        Cout << response.DebugString() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TDestroySessionCommand final
    : public TFileStoreCommand
{
private:
    TString SessionId;
    ui64 SeqNo = 0;

public:
    TDestroySessionCommand()
    {
        Opts.AddLongOption("session-id")
            .RequiredArgument("SESSION_ID")
            .StoreResult(&SessionId)
            .Required();

        Opts.AddLongOption("client-id")
            .RequiredArgument("CLIENT_ID")
            .StoreResult(&ClientId)
            .Required();

        Opts.AddLongOption("seq-no")
            .RequiredArgument("SEQ_NO")
            .StoreResult(&SeqNo)
            .Required();
    }

    bool Execute() override
    {
        auto request = std::make_shared<NProto::TDestroySessionRequest>();
        request->SetFileSystemId(FileSystemId);
        request->MutableHeaders()->SetSessionId(SessionId);
        request->MutableHeaders()->SetClientId(ClientId);
        request->MutableHeaders()->SetSessionSeqNo(SeqNo);

        TCallContextPtr ctx = MakeIntrusive<TCallContext>(FileSystemId);
        auto response = WaitFor(Client->DestroySession(ctx, std::move(request)));
        CheckResponse(response);
        Print(response, JsonOutput);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDestroySessionCommand()
{
    return std::make_shared<TDestroySessionCommand>();
}

}   // namespace NCloud::NFileStore::NClient
