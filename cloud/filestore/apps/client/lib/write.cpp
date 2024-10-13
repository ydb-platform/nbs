#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteCommand final
    : public TFileStoreCommand
{
private:
    TString Path;
    TString DataPath;
    ui64 Offset;

public:
    TWriteCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption("data")
            .RequiredArgument("PATH")
            .StoreResult(&DataPath);

        Opts.AddLongOption("offset")
            .RequiredArgument("INT")
            .DefaultValue(0)
            .StoreResult(&Offset);
    }

    bool Execute() override
    {
        TString data = TIFStream(DataPath).ReadAll();

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        const auto resolved = ResolvePath(session, Path, true);

        Y_ENSURE(
            resolved.back().Node.GetType() != NProto::E_DIRECTORY_NODE,
            "can't write to a directory node"
        );

        Y_ABORT_UNLESS(resolved.size() >= 2);

        const auto& parent = resolved[resolved.size() - 2];

        Y_ENSURE(
            parent.Node.GetType() != NProto::E_INVALID_NODE,
            TStringBuilder() << "target parent does not exist: " << parent.Name
        );

        static const int flags = ProtoFlag(NProto::TCreateHandleRequest::E_CREATE)
            | ProtoFlag(NProto::TCreateHandleRequest::E_READ)
            | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

        auto createRequest = CreateRequest<NProto::TCreateHandleRequest>();
        createRequest->SetNodeId(parent.Node.GetId());
        createRequest->SetName(ToString(resolved.back().Name));
        createRequest->SetFlags(flags);

        auto createResponse = WaitFor(session.CreateHandle(
            PrepareCallContext(),
            std::move(createRequest)));

        CheckResponse(createResponse);

        auto handle = createResponse.GetHandle();

        auto writeRequest = CreateRequest<NProto::TWriteDataRequest>();
        writeRequest->SetHandle(handle);
        writeRequest->SetOffset(Offset);
        *writeRequest->MutableBuffer() = data;

        auto writeResponse = WaitFor(session.WriteData(
            PrepareCallContext(),
            std::move(writeRequest)
        ));

        CheckResponse(writeResponse);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWriteCommand()
{
    return std::make_shared<TWriteCommand>();
}

}   // namespace NCloud::NFileStore::NClient
