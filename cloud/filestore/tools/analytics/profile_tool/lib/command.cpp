#include "command.h"

#include <util/folder/dirut.h>
#include <util/system/fs.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf ProfileLogLabel = "profile-log";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommand::TCommand()
{
    Opts.AddLongOption(
            ProfileLogLabel.data(),
            "Path to profile log")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&PathToProfileLog);
}

int TCommand::Run(int argc, const char** argv)
{
    OptsParseResult.ConstructInPlace(&Opts, argc, argv);
    if (!OptsParseResult.Defined()) {
        Cerr << "Failed to parse cmd parameters" << Endl;
        return 1;
    }

    if (!TCommand::Init(OptsParseResult.GetRef()) ||
        !Init(OptsParseResult.GetRef()))
    {
        return 1;
    }

    return Execute();
}

const NLastGetopt::TOpts& TCommand::GetOpts() const
{
    return Opts;
}

bool TCommand::Init(NLastGetopt::TOptsParseResultException& parseResult)
{
    Y_UNUSED(parseResult);

    try {
        PathToProfileLog = RealPath(PathToProfileLog);
    } catch (const TFileError& error) {
        Cerr << error.what() << Endl;
        return false;
    }

    return true;
}

}   // namespace NCloud::NFileStore::NProfileTool
