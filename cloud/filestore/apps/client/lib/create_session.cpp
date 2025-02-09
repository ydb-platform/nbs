#include "command.h"

#include <cloud/filestore/public/api/protos/session.pb.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

void Print(const NProto::TCreateSessionResponse& response, bool jsonOutput)
{
    if (jsonOutput) {
        Cout << response.AsJSON() << Endl;
    } else {
        Cout << response.DebugString() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCreateSessionCommand final
    : public TFileStoreCommand
{
private:
    TString SessionId;
    ui64 SeqNo = 0;

public:
    TCreateSessionCommand()
    {
        Opts.AddLongOption("session-id")
            .RequiredArgument("SESSION_ID")
            .StoreResult(&SessionId);

        Opts.AddLongOption("client-id")
            .RequiredArgument("CLIENT_ID")
            .StoreResult(&ClientId);

        Opts.AddLongOption("seq-no")
            .RequiredArgument("SEQ_NO")
            .StoreResult(&SeqNo);
    }

    bool Execute() override
    {
        auto request = std::make_shared<NProto::TCreateSessionRequest>();
        request->SetFileSystemId(FileSystemId);
        request->MutableHeaders()->SetSessionId(SessionId);
        request->MutableHeaders()->SetClientId(ClientId);
        request->MutableHeaders()->SetSessionSeqNo(SeqNo);

        TCallContextPtr ctx = MakeIntrusive<TCallContext>(FileSystemId);
        auto response = WaitFor(Client->CreateSession(ctx, std::move(request)));
        CheckResponse(response);
        Print(response, JsonOutput);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewCreateSessionCommand()
{
    return std::make_shared<TCreateSessionCommand>();
}

}   // namespace NCloud::NFileStore::NClient
