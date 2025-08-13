#include "command.h"

#include <util/generic/size_literals.h>
#include <util/system/align.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadCommand final
    : public TFileStoreCommand
{
private:
    TString Path;
    ui64 Offset;
    ui64 Length;

public:
    TReadCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

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
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        const auto resolved = ResolvePath(session, Path, false);

        Y_ENSURE(
            resolved.back().Node.GetType() != NProto::E_DIRECTORY_NODE,
            "can't read a directory node"
        );

        Y_ABORT_UNLESS(resolved.size() >= 2);

        const auto& parent = resolved[resolved.size() - 2];

        static const int flags = ProtoFlag(NProto::TCreateHandleRequest::E_READ);

        auto createRequest = CreateRequest<NProto::TCreateHandleRequest>();
        createRequest->SetNodeId(parent.Node.GetId());
        createRequest->SetName(ToString(resolved.back().Name));
        createRequest->SetFlags(flags);

        auto createResponse = WaitFor(session.CreateHandle(
            PrepareCallContext(),
            std::move(createRequest)));

        CheckResponse(createResponse);

        auto handle = createResponse.GetHandle();

        if (!Length) {
            // TODO pass proper block size
            Length = AlignUp(resolved.back().Node.GetSize() - Offset, 4_KB);
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

        const auto& buffer = readResponse.GetBuffer();
        const auto& bufferOffset = readResponse.GetBufferOffset();
        if (!buffer.empty()) {
            Y_ABORT_UNLESS(bufferOffset < buffer.size());
            Cout << buffer.substr(bufferOffset);
        }

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
