#include "command.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/libs/common/format.h>

#include <fnmatch.h>

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool MatchesGlob(const TString& pattern, const TString& name)
{
    return pattern ? fnmatch(pattern.c_str(), name.c_str(), 0) == 0 : true;
}

////////////////////////////////////////////////////////////////////////////////

class TFindCommand final
    : public TFileStoreCommand
{
private:
    TString Glob;
    ui32 Depth = 0;

public:
    TFindCommand()
    {
        Opts.AddLongOption("glob")
            .RequiredArgument("GLOB")
            .StoreResult(&Glob);

        Opts.AddLongOption("depth")
            .RequiredArgument("NUM")
            .DefaultValue(1)
            .StoreResult(&Depth);
    }

    void StatAll(
        ISession& session,
        const TString& fsId,
        TString prefix,
        ui64 parentId,
        ui32 depth)
    {
        --depth;
        auto response = ListAll(session, fsId, parentId, false);

        // TODO: async

        for (ui32 i = 0; i < response.NodesSize(); ++i) {
            const auto& node = response.GetNodes(i);
            const auto& name = response.GetNames(i);
            if (MatchesGlob(Glob, name)) {
                Cout << prefix << "\t" << name << "\t" << node.GetId() << Endl;
            }

            if (node.GetType() == NProto::E_DIRECTORY_NODE && depth) {
                StatAll(
                    session,
                    fsId,
                    prefix + name + "/",
                    node.GetId(),
                    depth);
            }
        }
    }

    bool Execute() override
    {
        auto sessionGuard = CreateSession();
        auto& session = sessionGuard.AccessSession();
        StatAll(session, FileSystemId, "/", RootNodeId, Depth);

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewFindCommand()
{
    return std::make_shared<TFindCommand>();
}

}   // namespace NCloud::NFileStore::NClient
