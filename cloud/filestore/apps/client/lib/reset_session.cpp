#include "command.h"

#include <cloud/filestore/public/api/protos/session.pb.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TResetSessionCommand final
    : public TFileStoreCommand
{
private:
    TString SessionId;
    ui64 SeqNo = 0;
    TString SessionState;

public:
    TResetSessionCommand()
    {
        Opts.AddLongOption("session-id")
            .RequiredArgument("SESSION_ID")
            .Required()
            .StoreResult(&SessionId);

        Opts.AddLongOption("client-id")
            .RequiredArgument("CLIENT_ID")
            .Required()
            .StoreResult(&ClientId);

        Opts.AddLongOption("seq-no")
            .RequiredArgument("SEQ_NO")
            .StoreResult(&SeqNo);

        Opts.AddLongOption("session-state")
            .RequiredArgument("BASE64_SESSION_STATE")
            .StoreResult(&SessionState);
    }

    bool Execute() override
    {
        auto request = CreateRequest<NProto::TResetSessionRequest>();
        auto& headers = *request->MutableHeaders();
        headers.SetSessionId(SessionId);
        headers.SetClientId(ClientId);
        headers.SetSessionSeqNo(SeqNo);
        if (SessionState) {
            request->SetSessionState(Base64Decode(SessionState));
        }

        auto response = WaitFor(Client->ResetSession(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewResetSessionCommand()
{
    return std::make_shared<TResetSessionCommand>();
}

}   // namespace NCloud::NFileStore::NClient
