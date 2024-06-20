#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

void PrintNodeAttributes(IOutputStream& out, const NProto::TNodeAttr& nodeAttr)
{
    out << "Node Id:   " << nodeAttr.GetId() << Endl;
    out << "Node Type: " << nodeAttr.GetType() << Endl;
    out << "Mode:      " << nodeAttr.GetMode() << Endl;
    out << "Uid:       " << nodeAttr.GetUid() << Endl;
    out << "Gid:       " << nodeAttr.GetGid() << Endl;
    out << "Access:    " << TInstant::FromValue(nodeAttr.GetATime()) << Endl;
    out << "Modify:    " << TInstant::FromValue(nodeAttr.GetMTime()) << Endl;
    out << "Change:    " << TInstant::FromValue(nodeAttr.GetCTime()) << Endl;
    out << "File Size: " << nodeAttr.GetSize() << Endl;
    out << "Links:     " << nodeAttr.GetLinks() << Endl;
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
        CreateSession();

        const auto resolved = ResolvePath(Path, false);
        Y_ABORT_UNLESS(resolved.size() >= 2);

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(resolved[resolved.size() - 2].Node.GetId());
        request->SetName(ToString(resolved.back().Name));
        auto response = WaitFor(
            Client->GetNodeAttr(PrepareCallContext(), std::move(request)));

        CheckResponse(response);

        PrintNodeAttributes(Cout, response.GetNode());

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
