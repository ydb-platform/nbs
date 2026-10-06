#include "command.h"

#include "time_range.h"

#include <library/cpp/eventlog/dumper/evlogdump.h>

#include <util/folder/dirut.h>
#include <util/generic/algorithm.h>
#include <util/system/fs.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf ProfileLogLabel = "profile-log";

TProfileLogFile MakeProfileLogFile(TString path)
{
    path = RealPath(path);
    const auto endTime = GetProfileLogEndTime(path);
    return {std::move(path), endTime};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommand::TCommand()
{
    Opts.AddLongOption(
            ProfileLogLabel.data(),
            "Path to profile log (repeat for multiple files; "
            "processed oldest first by filename timestamp, falling back to "
            "mtime)")
        .Required()
        .RequiredArgument("STR")
        .Handler1T<TString>(
            [this](TString path)
            {
                ProfileLogFiles.push_back(MakeProfileLogFile(std::move(path)));
            });

    Opts.SetFreeArgDefaultTitle("PROFILE_LOG", "Additional profile log files");
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

int TCommand::ProcessProfileLogs(IEventProcessor& processor)
{
    const bool ignoreErrors = OptsParseResult.GetRef().Has("ignore-errors");
    for (const auto& file: ProfileLogFiles) {
        if (ProfileLogFiles.size() > 1) {
            Cerr << "Reading " << file.Path << " " << file.EndTime << "\n";
        }
        const auto result = ProcessProfileLog(file.Path, processor, ignoreErrors);
        if (result) {
            return result;
        }
    }
    return 0;
}

int TCommand::ProcessProfileLog(
    const TString& path,
    IEventProcessor& processor,
    bool ignoreErrors)
{
    int result = 1;
    try {
        const char* args[] = {"", path.c_str()};
        result = IterateEventLog(NEvClass::Factory(), &processor, 2, args);
    } catch (const yexception& error) {
        Cerr << "Error reading profile log " << path << ": " << error.what()
             << Endl;
        if (!ignoreErrors) {
            throw;
        }
    }
    if (result != 0 && ignoreErrors) {
        Cerr << "Skipping unreadable remainder of profile log: " << path
             << Endl;
        return 0;
    }
    return result;
}

bool TCommand::Init(NLastGetopt::TOptsParseResultException& parseResult)
{
    try {
        for (const auto& path: parseResult.GetFreeArgs()) {
            ProfileLogFiles.push_back(MakeProfileLogFile(path));
        }

        StableSort(
            ProfileLogFiles,
            [](const auto& lhs, const auto& rhs)
            { return lhs.EndTime < rhs.EndTime; });
    } catch (const TFileError& error) {
        Cerr << error.what() << Endl;
        return false;
    }

    return true;
}

}   // namespace NCloud::NFileStore::NProfileTool
