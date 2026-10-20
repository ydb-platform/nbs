#include "command.h"

#include "common_filter_params.h"
#include "factory.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestCommand final: public TCommand
{
    const TCommonFilterParams CommonFilterParams{Opts};

public:
    TVector<TString> Inputs;

    bool Init(NLastGetopt::TOptsParseResultException& parseResult) override
    {
        Y_UNUSED(parseResult);
        return true;
    }

    int Execute() override
    {
        for (const auto& file: ProfileLogFiles) {
            Inputs.push_back(file.Path);
        }
        return 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCountingProcessor: public TProtobufEventProcessor
{
public:
    TVector<TString> FileSystems;
    bool FailProcessing = false;
    IOutputStream* Output = nullptr;

    void DoProcessEvent(const TEvent* event, IOutputStream*) override
    {
        if (const auto* record = dynamic_cast<const NProto::TProfileLogRecord*>(
                event->GetProto()))
        {
            FileSystems.push_back(record->GetFileSystemId());
            if (FailProcessing) {
                ythrow yexception() << "Processing failed";
            }
            if (Output) {
                *Output << record->GetFileSystemId();
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadWithoutFiltersCommand: public TCommand
{
public:
    TCountingProcessor Processor;

    bool Init(NLastGetopt::TOptsParseResultException&) override
    {
        return true;
    }

    int Execute() override
    {
        return ProcessProfileLogs(Processor);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadTestCommand: public TCommand
{
    const TCommonFilterParams CommonFilterParams{Opts};

public:
    TCountingProcessor Processor;
    size_t FilesSelected = 0;

    bool Init(NLastGetopt::TOptsParseResultException& parseResult) override
    {
        ProfileLogFiles = SelectProfileLogFiles(
            std::move(ProfileLogFiles),
            CommonFilterParams.GetSince(parseResult),
            CommonFilterParams.GetUntil(parseResult));
        return true;
    }

    int Execute() override
    {
        FilesSelected = ProfileLogFiles.size();
        return ProcessProfileLogs(Processor);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFailingOutput: public IOutputStream
{
    void DoWrite(const void*, size_t) override
    {
        ythrow TFileError() << "Output write failed";
    }
};

////////////////////////////////////////////////////////////////////////////////

void WriteProfileLog(const TString& path, ui64 timestamp = 1000)
{
    TEventLog log(path, 0);
    {
        TSelfFlushLogFrame frame(log);
        NProto::TProfileLogRecord record;
        record.SetFileSystemId(path);
        record.AddRequests()->SetTimestampMcs(TInstant::Seconds(timestamp).MicroSeconds());
        frame.LogEvent(record);
        frame.Flush();
    }
    log.CloseLog();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommandTest)
{
    Y_UNIT_TEST(ShouldRejectMissingInput)
    {
        const char* dumpArgs[] = {"dumpevents", "--fs-id", "x"};
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            GetCommand("dumpevents")->Run(3, dumpArgs),
            NLastGetopt::TUsageException,
            "At least one profile log is required");

        const char* findArgs[] = {
            "findbytesaccess", "--start", "0", "--count", "1"};
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            GetCommand("findbytesaccess")->Run(5, findArgs),
            NLastGetopt::TUsageException,
            "At least one profile log is required");

        TTempDir directory;
        const auto output = directory.Path() / "out";
        const char* maskArgs[] = {
            "masksensitivedata", "--out-profile-log", output.c_str()};
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            GetCommand("masksensitivedata")->Run(3, maskArgs),
            NLastGetopt::TUsageException,
            "At least one profile log is required");
        UNIT_ASSERT(!output.Exists());
    }

    Y_UNIT_TEST(ShouldAcceptPositionalInputWithoutProfileLogOption)
    {
        TTempFileHandle input;
        const char* args[] = {"test", input.Name().c_str()};
        TTestCommand command;
        UNIT_ASSERT_VALUES_EQUAL(command.Run(2, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(input.Name()));
    }

    Y_UNIT_TEST(ShouldReadAllFilesUnlessPruningRequested)
    {
        TTempDir directory;
        TTempFileHandle oldFile(
            directory.Name() + "/profile.log.1970-01-01T00:16");
        TTempFileHandle currentFile(directory.Name() + "/profile.log");
        WriteProfileLog(oldFile.Name());
        WriteProfileLog(currentFile.Name(), 2400);
        oldFile.Resize(oldFile.GetLength() - 1);

        for (const auto* name: {"dumpevents", "findbytesaccess"}) {
            TVector<const char*> args = {
                name,
                "--profile-log",
                oldFile.Name().c_str(),
                currentFile.Name().c_str(),
                "--since",
                "1970-01-01T00:38:21Z",
                "--until",
                "1970-01-01T00:41:40Z"};
            if (TStringBuf(name) == "findbytesaccess") {
                args.insert(args.end(), {"--start", "0", "--count", "1"});
            }
            // Default reading exposes even errors outside the estimated range.
            UNIT_ASSERT_VALUES_EQUAL(
                GetCommand(name)->Run(args.size(), args.data()),
                1);
            args.push_back("--prune-by-time");
            // Only explicit pruning skips the old, truncated log.
            UNIT_ASSERT_VALUES_EQUAL(
                GetCommand(name)->Run(args.size(), args.data()),
                0);
        }
    }

    Y_UNIT_TEST(ShouldProcessLogsWithoutCommonFilterParams)
    {
        TTempFileHandle input;
        WriteProfileLog(input.Name());
        const char* args[] = {
            "test",
            "--profile-log",
            input.Name().c_str(),
            "--ignore-errors"};
        for (const bool ignoreErrors: {false, true}) {
            TReadWithoutFiltersCommand command;
            UNIT_ASSERT_VALUES_EQUAL(
                command.Run(ignoreErrors ? 4 : 3, args),
                0);
            UNIT_ASSERT_VALUES_EQUAL(command.Processor.FileSystems.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                command.Processor.FileSystems[0],
                input.Name());
        }
    }

    Y_UNIT_TEST(ShouldPropagateProcessingFailures)
    {
        TTempFileHandle first;
        TTempFileHandle second;
        WriteProfileLog(first.Name());
        WriteProfileLog(second.Name());
        const char* args[] = {
            "test",
            "--profile-log",
            first.Name().c_str(),
            second.Name().c_str(),
            "--ignore-errors"};
        for (const bool ignoreErrors: {false, true}) {
            TReadTestCommand command;
            command.Processor.FailProcessing = true;
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                command.Run(ignoreErrors ? 5 : 4, args),
                yexception,
                "Processing failed");
            UNIT_ASSERT_VALUES_EQUAL(command.Processor.FileSystems.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(
                command.Processor.FileSystems[0],
                first.Name());
        }
    }

    Y_UNIT_TEST(ShouldPropagateOutputFailures)
    {
        TTempFileHandle input;
        WriteProfileLog(input.Name());
        const char* args[] = {
            "test",
            "--profile-log",
            input.Name().c_str(),
            "--ignore-errors"};
        for (const bool ignoreErrors: {false, true}) {
            TFailingOutput output;
            TReadTestCommand command;
            command.Processor.Output = &output;
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                command.Run(ignoreErrors ? 4 : 3, args),
                TFileError,
                "Output write failed");
        }
    }

    Y_UNIT_TEST(ShouldReadOnlyPreselectedFiles)
    {
        TTempDir directory;
        TTempFileHandle first(directory.Name() + "/log.1970-01-01T00:16");
        TTempFileHandle second(directory.Name() + "/log.1970-01-01T00:33");
        TTempFileHandle third;
        first.Write("not a log", 9);
        second.Write("not a log", 9);
        {
            TEventLog log(third.Name(), 0);
            TSelfFlushLogFrame frame(log);
            NProto::TProfileLogRecord record;
            record.SetFileSystemId("selected");
            record.AddRequests()->SetTimestampMcs(TInstant::Seconds(2400).MicroSeconds());
            frame.LogEvent(record);
            frame.Flush();
            log.CloseLog();
        }
        const char* args[] = {
            "test",
            "--profile-log",
            third.Name().c_str(),
            first.Name().c_str(),
            second.Name().c_str(),
            "--since",
            "1970-01-01T00:38:21Z",
            "--until",
            "1970-01-01T00:41:40Z",
            "--prune-by-time"};
        TReadTestCommand command;
        UNIT_ASSERT_VALUES_EQUAL(command.Run(10, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.FilesSelected, 1);
        UNIT_ASSERT_VALUES_EQUAL(command.Processor.FileSystems.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(command.Processor.FileSystems[0], "selected");
    }

    Y_UNIT_TEST(ShouldContinueAfterTruncatedFileOnlyWhenRequested)
    {
        TTempFileHandle broken;
        TTempFileHandle good;
        for (const auto* input: {&broken, &good}) {
            TEventLog log(input->Name(), 0);
            {
                TSelfFlushLogFrame frame(log);
                NProto::TProfileLogRecord record;
                record.SetFileSystemId(input->Name());
                record.AddRequests()->SetTimestampMcs(100);
                frame.LogEvent(record);
                frame.Flush();
            }
            log.CloseLog();
        }
        broken.Resize(broken.GetLength() - 1);

        const char* strictArgs[] = {
            "test",
            "--profile-log",
            broken.Name().c_str(),
            good.Name().c_str()};
        TReadTestCommand strict;
        UNIT_ASSERT_VALUES_EQUAL(strict.Run(4, strictArgs), 1);
        UNIT_ASSERT_VALUES_EQUAL(strict.FilesSelected, 2);
        UNIT_ASSERT(strict.Processor.FileSystems.empty());

        const char* tolerantArgs[] = {
            "test",
            "--ignore-errors",
            "--profile-log",
            broken.Name().c_str(),
            good.Name().c_str()};
        TReadTestCommand tolerant;
        UNIT_ASSERT_VALUES_EQUAL(tolerant.Run(5, tolerantArgs), 0);
        UNIT_ASSERT_VALUES_EQUAL(tolerant.FilesSelected, 2);
        UNIT_ASSERT_VALUES_EQUAL(tolerant.Processor.FileSystems.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            tolerant.Processor.FileSystems[0],
            good.Name());
    }

    Y_UNIT_TEST(ShouldAcceptSingleFile)
    {
        TTempFileHandle input;
        TTestCommand command;
        const char* args[] = {"test", "--profile-log", input.Name().c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(3, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(input.Name()));
    }

    Y_UNIT_TEST(ShouldAcceptRepeatedOptionsAndExpandedWildcard)
    {
        TTempFileHandle first;
        TTempFileHandle second;
        TTempFileHandle third;
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            first.Name().c_str(),
            "--profile-log",
            second.Name().c_str(),
            third.Name().c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(6, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(first.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[1], RealPath(second.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[2], RealPath(third.Name()));
    }

    Y_UNIT_TEST(ShouldProbeUndatedFilesOnlyWhenPruningRequested)
    {
        TTempFileHandle newer;
        TTempFileHandle older;
        WriteProfileLog(newer.Name(), 2000);
        WriteProfileLog(older.Name(), 1000);
        const char* args[] = {
            "test", "--profile-log", newer.Name().c_str(), older.Name().c_str(),
            "--prune-by-time"};
        TTestCommand unprobed;
        UNIT_ASSERT_VALUES_EQUAL(unprobed.Run(4, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(unprobed.Inputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(unprobed.Inputs[0], RealPath(newer.Name()));
        UNIT_ASSERT_VALUES_EQUAL(unprobed.Inputs[1], RealPath(older.Name()));
        TTestCommand probed;
        UNIT_ASSERT_VALUES_EQUAL(probed.Run(5, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(probed.Inputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(probed.Inputs[0], RealPath(older.Name()));
        UNIT_ASSERT_VALUES_EQUAL(probed.Inputs[1], RealPath(newer.Name()));
    }

    Y_UNIT_TEST(ShouldSortByFilenameTimeWithRecordFallback)
    {
        TTempDir directory;
        const TString base = directory.Name() + "/nfs-vhost-profile.log";
        TTempFileHandle current(base);
        TTempFileHandle newer(base + ".2026-09-22T15:22");
        TTempFileHandle oldest(base + ".2026-09-21T15:42");
        TTempFileHandle newest(base + ".2026-09-22T15:32");
        WriteProfileLog(
            current.Name(),
            TInstant::ParseIso8601("2026-09-22T15:27:00Z").Seconds());
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            current.Name().c_str(),
            newer.Name().c_str(),
            oldest.Name().c_str(),
            newest.Name().c_str(),
            "--prune-by-time"};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(7, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(oldest.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[1], RealPath(newer.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[2], RealPath(current.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[3], RealPath(newest.Name()));
    }

    Y_UNIT_TEST(ShouldKeepInputOrderForEqualFilenameTimes)
    {
        TTempDir directory;
        TTempFileHandle first(directory.Name() + "/first.2026-09-26T10:25");
        TTempFileHandle second(directory.Name() + "/second.2026-09-26T10:25");
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            first.Name().c_str(),
            second.Name().c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(4, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(first.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[1], RealPath(second.Name()));
    }

    Y_UNIT_TEST(ShouldDeferPathValidationUntilInit)
    {
        TTempDir directory;
        const TString missing = directory.Name() + "/missing";
        const char* args[] = {"test", "--profile-log", missing.c_str()};
        TTestCommand command;
        const NLastGetopt::TOptsParseResultException parsed(
            &command.GetOpts(), 3, args);
        UNIT_ASSERT(parsed.Has("profile-log"));
    }

    Y_UNIT_TEST(ShouldValidateAllFilesBeforeExecuting)
    {
        TTempFileHandle input;
        const TString missing = input.Name() + ".missing";
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            input.Name().c_str(),
            missing.c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(4, args), 1);
        UNIT_ASSERT(command.Inputs.empty());

        TTestCommand optionCommand;
        const char* optionArgs[] = {
            "test",
            "--profile-log",
            input.Name().c_str(),
            "--profile-log",
            missing.c_str()};
        UNIT_ASSERT_VALUES_EQUAL(optionCommand.Run(5, optionArgs), 1);
        UNIT_ASSERT(optionCommand.Inputs.empty());
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
