#include "command.h"

#include "time_range.h"

#include <library/cpp/eventlog/iterator.h>

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

void PrintProfileLogProgress(const TProfileLogFile& file)
{
    Cerr << "Reading " << file.Path;
    if (file.EndTime) {
        Cerr << " " << *file.EndTime;
    }
    Cerr << Endl;
}

////////////////////////////////////////////////////////////////////////////////

TCommand::TCommand()
{
    Opts.AddLongOption(
            ProfileLogLabel.data(),
            "Path to profile log (repeat for multiple files; "
            "processed oldest first by filename timestamp, falling back to "
            "mtime)")
        .RequiredArgument("STR")
        .AppendTo(&ProfileLogPaths);

    Opts.AddLongOption(
            "ignore-errors",
            "Report log read errors and continue with the next file")
        .NoArgument()
        .StoreTrue(&IgnoreErrors);
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
    for (const auto& file: ProfileLogFiles) {
        if (ProfileLogFiles.size() > 1) {
            PrintProfileLogProgress(file);
        }
        const auto result = ProcessProfileLog(file.Path, processor, IgnoreErrors);
        Cout.Flush();
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
    processor.SetOptions(TEvent::TOutputOptions{});
    const auto handleReadError = [&path, ignoreErrors](const yexception& error) {
        Cerr << "Error reading profile log " << path << ": " << error.what()
             << Endl;
        if (ignoreErrors) {
            Cerr << "Skipping unreadable remainder of profile log: " << path
                 << Endl;
            return 0;
        }
        return 1;
    };

    NEventLog::TOptions options;
    options.FileName = path;
    THolder<NEventLog::IIterator> iterator;
    try {
        iterator = NEventLog::CreateIterator(options, NEvClass::Factory());
    } catch (const yexception& error) {
        return handleReadError(error);
    }

    for (;;) {
        TConstEventPtr event;
        try {
            event = iterator->Next();
        } catch (const yexception& error) {
            return handleReadError(error);
        }
        // Processing and output failures must propagate even with ignore-errors.
        if (!event || !processor.CheckedProcessEvent(event.Get())) {
            return 0;
        }
    }
}

bool TCommand::Init(NLastGetopt::TOptsParseResultException& parseResult)
{
    try {
        for (const auto& path: parseResult.GetFreeArgs()) {
            ProfileLogPaths.push_back(path);
        }

        if (ProfileLogPaths.empty()) {
            ythrow NLastGetopt::TUsageException()
                << "At least one profile log is required; use --profile-log "
                   "or a positional path";
        }

        for (const auto& path: ProfileLogPaths) {
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
