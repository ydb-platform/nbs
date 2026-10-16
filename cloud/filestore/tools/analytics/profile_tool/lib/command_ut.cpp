#include "command.h"

#include "common_filter_params.h"
#include "factory.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>

#include <library/cpp/eventlog/eventlog.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/system/tempfile.h>

#include <chrono>
#include <filesystem>

namespace NCloud::NFileStore::NProfileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

void SetModificationTime(const TString& path, i64 seconds, i64 nanoseconds = 0)
{
    std::filesystem::last_write_time(
        path.c_str(),
        std::filesystem::file_time_type{} + std::chrono::seconds(seconds) +
            std::chrono::nanoseconds(nanoseconds));
}

////////////////////////////////////////////////////////////////////////////////

class TTestCommand final: public TCommand
{
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

void WriteProfileLog(const TString& path)
{
    TEventLog log(path, 0);
    {
        TSelfFlushLogFrame frame(log);
        NProto::TProfileLogRecord record;
        record.SetFileSystemId(path);
        frame.LogEvent(record);
        frame.Flush();
    }
    log.CloseLog();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommandTest)
{
    Y_UNIT_TEST(ShouldBypassFilePruningWhenAllFilesRequested)
    {
        TTempDir directory;
        TTempFileHandle oldFile(
            directory.Name() + "/profile.log.1970-01-01T00:16");
        TTempFileHandle currentFile(directory.Name() + "/profile.log");
        WriteProfileLog(oldFile.Name());
        WriteProfileLog(currentFile.Name());
        oldFile.Resize(oldFile.GetLength() - 1);
        SetModificationTime(currentFile.Name(), 3000);

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
            // The old, truncated log is outside the estimated time range.
            UNIT_ASSERT_VALUES_EQUAL(
                GetCommand(name)->Run(args.size(), args.data()),
                0);
            args.push_back("--all-files");
            // Reading it now must expose its error; pruning would hide it.
            UNIT_ASSERT_VALUES_EQUAL(
                GetCommand(name)->Run(args.size(), args.data()),
                1);
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
        SetModificationTime(first.Name(), 100);
        SetModificationTime(second.Name(), 200);
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
        TTempFileHandle first;
        TTempFileHandle second;
        TTempFileHandle third;
        first.Write("not a log", 9);
        second.Write("not a log", 9);
        {
            TEventLog log(third.Name(), 0);
            TSelfFlushLogFrame frame(log);
            NProto::TProfileLogRecord record;
            record.SetFileSystemId("selected");
            frame.LogEvent(record);
            frame.Flush();
            log.CloseLog();
        }
        SetModificationTime(first.Name(), 1000);
        SetModificationTime(second.Name(), 2000);
        SetModificationTime(third.Name(), 3000);
        const char* args[] = {
            "test",
            "--profile-log",
            third.Name().c_str(),
            first.Name().c_str(),
            second.Name().c_str(),
            "--since",
            "1970-01-01T00:38:21Z",
            "--until",
            "1970-01-01T00:41:40Z"};
        TReadTestCommand command;
        UNIT_ASSERT_VALUES_EQUAL(command.Run(9, args), 0);
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
        SetModificationTime(broken.Name(), 100);
        SetModificationTime(good.Name(), 200);

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
        SetModificationTime(first.Name(), 100);
        SetModificationTime(second.Name(), 100);
        SetModificationTime(third.Name(), 100);
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

    Y_UNIT_TEST(ShouldSortByFilenameTimeWithMtimeFallback)
    {
        TTempDir directory;
        const TString base = directory.Name() + "/nfs-vhost-profile.log";
        TTempFileHandle current(base);
        TTempFileHandle newer(base + ".2026-09-22T15:22");
        TTempFileHandle oldest(base + ".2026-09-21T15:42");
        TTempFileHandle newest(base + ".2026-09-22T15:32");
        SetModificationTime(
            current.Name(),
            TInstant::ParseIso8601("2026-09-22T15:27:00Z").Seconds());
        // Mtimes deliberately disagree with the dated filenames.
        SetModificationTime(newer.Name(), 200);
        SetModificationTime(oldest.Name(), 300);
        SetModificationTime(newest.Name(), 100);
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            current.Name().c_str(),
            newer.Name().c_str(),
            oldest.Name().c_str(),
            newest.Name().c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(6, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(oldest.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[1], RealPath(newer.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[2], RealPath(current.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[3], RealPath(newest.Name()));
    }

    Y_UNIT_TEST(ShouldKeepInputOrderWithinSameMicrosecond)
    {
        TTempDir directory;
        TTempFileHandle newest(directory.Name() + "/a");
        TTempFileHandle oldest(directory.Name() + "/z");
        TTempFileHandle middle(directory.Name() + "/m");
        SetModificationTime(newest.Name(), 100, 300);
        SetModificationTime(oldest.Name(), 100, 100);
        SetModificationTime(middle.Name(), 100, 200);
        TTestCommand command;
        const char* args[] = {
            "test",
            "--profile-log",
            newest.Name().c_str(),
            "--profile-log",
            oldest.Name().c_str(),
            "--profile-log",
            middle.Name().c_str()};
        UNIT_ASSERT_VALUES_EQUAL(command.Run(7, args), 0);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[0], RealPath(newest.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[1], RealPath(oldest.Name()));
        UNIT_ASSERT_VALUES_EQUAL(command.Inputs[2], RealPath(middle.Name()));
    }

    Y_UNIT_TEST(ShouldKeepInputOrderForEqualFilenameTimes)
    {
        TTempDir directory;
        TTempFileHandle first(directory.Name() + "/first.2026-09-26T10:25");
        TTempFileHandle second(directory.Name() + "/second.2026-09-26T10:25");
        SetModificationTime(first.Name(), 300);
        SetModificationTime(second.Name(), 100);
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
        UNIT_ASSERT_EXCEPTION(
            optionCommand.Run(5, optionArgs),
            NLastGetopt::TUsageException);
        UNIT_ASSERT(optionCommand.Inputs.empty());
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
