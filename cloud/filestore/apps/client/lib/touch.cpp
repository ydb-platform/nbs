#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTouchCommand final
    : public TFileStoreCommand
{
private:
    TString Path;

public:
    TTouchCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto resolved = ResolvePath(session, Path, true);

        if (resolved.size() == 1) {
            return true;
        }

        Y_ABORT_UNLESS(resolved.size() >= 2);

        const auto& node = resolved.back().Node;
        if (node.GetType() != NProto::E_INVALID_NODE) {
            auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
            auto* update = request->MutableUpdate();
            ui64 now = MicroSeconds();
            update->SetATime(now);
            update->SetMTime(now);
            request->SetNodeId(node.GetId());

            auto response = WaitFor(session.SetNodeAttr(
                PrepareCallContext(),
                std::move(request)));

            CheckResponse(response);
            return true;
        } else {
            const auto& parent = resolved[resolved.size() - 2];

            Y_ENSURE(
                parent.Node.GetType() == NProto::E_DIRECTORY_NODE,
                TStringBuilder() << "target parent is not a directory"
            );

            auto request = CreateRequest<NProto::TCreateNodeRequest>();
            request->SetNodeId(parent.Node.GetId());
            request->SetName(ToString(resolved.back().Name));
            request->MutableFile()->SetMode(0644);

            auto response = WaitFor(session.CreateNode(
                PrepareCallContext(),
                std::move(request)));

            CheckResponse(response);
            return true;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewTouchCommand()
{
    return std::make_shared<TTouchCommand>();
}

}   // namespace NCloud::NFileStore::NClient
