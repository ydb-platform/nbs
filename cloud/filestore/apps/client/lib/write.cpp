#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>

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

class TWriteCommand final
    : public TFileStoreCommand
{
private:
    EModeType Mode = EModeType::Unknown;
    TString Path;
    ui64 NodeId = 0;
    TString DataPath;
    ui64 Offset;

public:
    TWriteCommand()
    {
        const TString PathOptionName = "path";
        Opts.AddLongOption(PathOptionName)
            .RequiredArgument("PATH")
            .StoreResult(&Path)
            .Handler0([this] { Mode = EModeType::Path; });

        const TString NodeOptionName = "node";
        Opts.AddLongOption(NodeOptionName)
            .RequiredArgument("ID")
            .StoreResult(&NodeId)
            .Handler0([this] { Mode = EModeType::Node; });

        Opts.MutuallyExclusive(PathOptionName, NodeOptionName);

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
        Y_ENSURE(
            Mode != EModeType::Unknown,
            "--path or --node must be specified");

        TString data = TIFStream(DataPath).ReadAll();

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto createRequest = CreateRequest<NProto::TCreateHandleRequest>();

        int flags = ProtoFlag(NProto::TCreateHandleRequest::E_READ) |
                    ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

        switch (Mode) {
            case EModeType::Path: {
                const auto resolved = ResolvePath(session, Path, true);

                Y_ENSURE(
                    resolved.back().Node.GetType() != NProto::E_DIRECTORY_NODE,
                    "can't write to a directory node");

                Y_ABORT_UNLESS(resolved.size() >= 2);

                const auto& parent = resolved[resolved.size() - 2];

                Y_ENSURE(
                    parent.Node.GetType() != NProto::E_INVALID_NODE,
                    TStringBuilder()
                        << "target parent does not exist: " << parent.Name);

                createRequest->SetNodeId(parent.Node.GetId());
                createRequest->SetName(ToString(resolved.back().Name));
                flags |= ProtoFlag(NProto::TCreateHandleRequest::E_CREATE);
                break;
            }
            case EModeType::Node:
                createRequest->SetNodeId(NodeId);
                break;
            case EModeType::Unknown:
                Y_ABORT("unreachable");
        }

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
