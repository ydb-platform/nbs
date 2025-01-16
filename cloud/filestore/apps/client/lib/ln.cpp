#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLnCommand final
    : public TFileStoreCommand
{
private:
    TString Path;
    TString SymLink;

public:
    TLnCommand()
    {
        Opts.AddLongOption("path")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&Path);

        Opts.AddLongOption("symlink")
            .Required()
            .RequiredArgument("PATH")
            .StoreResult(&SymLink);
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();

        auto resolved = ResolvePath(session, Path, true);

        Y_ENSURE(resolved.size() >= 2, "root node can't be ln target");

        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        request->SetNodeId(resolved[resolved.size() - 2].Node.GetId());
        request->SetName(TString(resolved.back().Name));
        request->MutableSymLink()->SetTargetPath(SymLink);

        auto response = WaitFor(session.CreateNode(
            PrepareCallContext(),
            std::move(request)));

        CheckResponse(response);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewLnCommand()
{
    return std::make_shared<TLnCommand>();
}

}   // namespace NCloud::NFileStore::NClient
