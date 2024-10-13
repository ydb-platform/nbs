#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

void Print(const NProto::TNodeAttr& nodeAttr, bool jsonOutput) {

    if (jsonOutput) {
        Cout << nodeAttr.AsJSON() << Endl;
    } else {
        Cout << nodeAttr.DebugString() << Endl;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TStatCommand final: public TFileStoreCommand
{
private:
    TString Path;

public:
    TStatCommand()
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

        const auto resolved = ResolvePath(session, Path, false);
        Y_ABORT_UNLESS(resolved.size() >= 2);

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(resolved[resolved.size() - 2].Node.GetId());
        request->SetName(ToString(resolved.back().Name));
        auto response = WaitFor(
            session.GetNodeAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);
        Print(response.GetNode(), JsonOutput);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewStatCommand()
{
    return std::make_shared<TStatCommand>();
}

}   // namespace NCloud::NFileStore::NClient
