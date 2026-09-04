#include "command.h"

#include <util/generic/size_literals.h>
#include <util/system/align.h>

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

class TReadCommand final
    : public TFileStoreCommand
{
private:
    EModeType Mode = EModeType::Unknown;
    TString Path;
    ui64 NodeId = 0;
    ui64 Offset;
    ui64 Length;

public:
    TReadCommand()
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

        Opts.AddLongOption("offset")
            .RequiredArgument("INT")
            .DefaultValue(0)
            .StoreResult(&Offset);

        Opts.AddLongOption("length")
            .RequiredArgument("INT")
            .DefaultValue(0)
            .StoreResult(&Length);
    }

    bool Execute() override
    {
        Y_ENSURE(
            Mode != EModeType::Unknown,
            "--path or --node must be specified");

        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        NProto::TNodeAttr node;
        switch (Mode) {
            case EModeType::Path:
                node = ResolvePath(session, Path, false).back().Node;
                break;
            case EModeType::Node: {
                auto attrRequest = CreateRequest<NProto::TGetNodeAttrRequest>();
                attrRequest->SetNodeId(NodeId);

                auto attrResponse = WaitFor(session.GetNodeAttr(
                    PrepareCallContext(),
                    std::move(attrRequest)));

                CheckResponse(attrResponse);

                node = attrResponse.GetNode();
                break;
            }
            case EModeType::Unknown:
                Y_ABORT("unreachable");
        }

        Y_ENSURE(
            node.GetType() != NProto::E_DIRECTORY_NODE,
            "can't read a directory node");

        auto createRequest = CreateRequest<NProto::TCreateHandleRequest>();
        createRequest->SetNodeId(node.GetId());
        createRequest->SetFlags(
            ProtoFlag(NProto::TCreateHandleRequest::E_READ));

        auto createResponse = WaitFor(session.CreateHandle(
            PrepareCallContext(),
            std::move(createRequest)));

        CheckResponse(createResponse);

        auto handle = createResponse.GetHandle();

        if (!Length) {
            // TODO pass proper block size
            Length = AlignUp(node.GetSize() - Offset, 4_KB);
        }

        auto readRequest = CreateRequest<NProto::TReadDataRequest>();
        readRequest->SetHandle(handle);
        readRequest->SetOffset(Offset);
        readRequest->SetLength(Length);

        auto readResponse = WaitFor(session.ReadData(
            PrepareCallContext(),
            std::move(readRequest)
        ));

        CheckResponse(readResponse);

        Cout << readResponse.GetBuffer().substr(readResponse.GetBufferOffset());

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadCommand()
{
    return std::make_shared<TReadCommand>();
}

}   // namespace NCloud::NFileStore::NClient
