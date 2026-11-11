#include "multi_client.h"

#include "dir_state.h"
#include "sync.h"

#include <library/cpp/json/writer/json.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MaxReportedErrors = 100;

TString InstanceDirName(ui32 instanceId)
{
    return TStringBuilder() << "instance_" << instanceId;
}

NProto::TError MakeSystemError(const TString& what, const TFsPath& path)
{
    const int error = LastSystemError();
    return MakeError(
        E_IO,
        TStringBuilder() << what << " " << path.GetPath().Quote() << ": "
            << LastSystemErrorText(error));
}

TFileAttrs ToFileAttrs(const TFileStat& st)
{
    return TFileAttrs{
        .Ino = st.INode,
        .Mode = st.Mode,
        .Size = st.Size,
        .Links = st.NLinks,
        .MTimeNs = static_cast<ui64>(st.MTime) * 1'000'000'000 + st.MTimeNSec,
        .CTimeNs = static_cast<ui64>(st.CTime) * 1'000'000'000 + st.CTimeNSec,
    };
}

TFileStat LStat(const TFsPath& path)
{
    return TFileStat(path, true /* nofollow */);
}

////////////////////////////////////////////////////////////////////////////////

class TMultiClient
{
private:
    TLog& Log;
    const TOptions& Options;
    const TFsPath TestDir;
    const TFsPath OwnDir;

    TDirState State;
    ui64 NameCounter = 0;

    ui32 RoundsDone = 0;
    ui64 CheckedFiles = 0;
    ui64 CheckedAbsentNames = 0;
    ui64 ErrorCount = 0;
    TVector<TString> ErrorSamples;
    TString FatalError;

public:
    TMultiClient(TLog& log, const TOptions& options)
        : Log(log)
        , Options(options)
        , TestDir(options.TestDir)
        , OwnDir(TestDir / InstanceDirName(options.InstanceId))
    {
        State.InstanceId = Options.InstanceId;
    }

    int Run()
    {
        auto error = RunRounds();
        if (HasError(error)) {
            FatalError = FormatError(error);
            STORAGE_ERROR("Fatal error: " << FatalError);
        }

        WriteReport();
        return FatalError || ErrorCount ? 1 : 0;
    }

private:
    NProto::TError RunRounds()
    {
        MakePathIfNotExist(OwnDir.c_str());

        auto [client, error] = TSyncClient::Connect(
            Options.SyncHost,
            Options.SyncPort,
            Options.InstanceId,
            Options.SyncTimeout);
        if (HasError(error)) {
            return error;
        }

        for (ui32 round = 0; round < Options.Rounds; ++round) {
            State.Round = round;

            //
            // Phase 1: modify the own directory and broadcast its expected
            // state.
            //

            if (auto modifyError = Modify(); HasError(modifyError)) {
                return modifyError;
            }

            auto [states, exchangeError] =
                client->Exchange(round, "state", State.ToJson());
            if (HasError(exchangeError)) {
                return exchangeError;
            }

            State.RenamedFrom.clear();
            State.Unlinked.clear();

            //
            // All the instances have finished their modifications before the
            // barrier. The delay lets the guest-side attr and entry caches
            // (whose timeouts are expected to be much shorter) expire.
            //

            Sleep(Options.CheckDelay);

            //
            // Phase 2: check the other instances' directories. The second
            // barrier prevents the instances from starting the next round's
            // modifications while the others are still checking.
            //

            const ui64 errorsBefore = ErrorCount;
            for (const auto& json: states) {
                auto [state, parseError] = TDirState::FromJson(json);
                if (HasError(parseError)) {
                    return parseError;
                }
                if (state.InstanceId != Options.InstanceId) {
                    Check(state);
                }
            }

            NJson::TJsonValue checked(NJson::JSON_MAP);
            checked["errors"] = ErrorCount - errorsBefore;
            auto [results, checkedError] =
                client->Exchange(round, "checked", checked);
            if (HasError(checkedError)) {
                return checkedError;
            }

            for (ui32 i = 0; i < results.size(); ++i) {
                const ui64 errors = results[i]["errors"].GetUIntegerSafe(0);
                if (errors) {
                    STORAGE_WARN("Round " << round << ": instance " << i
                        << " found " << errors << " errors");
                }
            }

            ++RoundsDone;
            STORAGE_INFO("Round " << round << " done, files: "
                << State.Files.size() << ", errors: " << ErrorCount);
        }

        return client->Finish();
    }

    TString NewName()
    {
        return TStringBuilder() << "f_" << NameCounter++;
    }

    NProto::TError Modify()
    {
        //
        // Creates new files, then unlinks, renames and overwrites random
        // files (including the new ones). The names are never reused, so a
        // renamed-from or unlinked name never exists again, and a rename never
        // replaces an existing file. An overwrite doesn't change the file
        // size - only its MTime and CTime change. The creations, unlinks and
        // renames change the directory MTime and CTime.
        //

        for (ui32 i = 0; i < Options.FilesPerRound; ++i) {
            if (auto error = CreateFile(NewName()); HasError(error)) {
                return error;
            }
        }

        TVector<TString> names;
        for (const auto& [name, _]: State.Files) {
            names.push_back(name);
        }

        for (const auto& name: names) {
            const ui32 dice = RandomNumber<ui32>(100);
            if (dice < Options.UnlinkPercentage) {
                const auto path = OwnDir / name;
                if (!NFs::Remove(path)) {
                    return MakeSystemError("unlink", path);
                }
                State.Files.erase(name);
                State.Unlinked.push_back(name);
            } else if (dice < Options.UnlinkPercentage +
                    Options.RenamePercentage)
            {
                const auto newName = NewName();
                const auto path = OwnDir / name;
                const auto newPath = OwnDir / newName;
                if (!NFs::Rename(path, newPath)) {
                    return MakeSystemError("rename", path);
                }
                State.Files.erase(name);
                State.Files[newName] = {};
                State.RenamedFrom.push_back(name);
            } else if (dice < Options.UnlinkPercentage +
                    Options.RenamePercentage + Options.OverwritePercentage)
            {
                if (auto error = OverwriteFile(name); HasError(error)) {
                    return error;
                }
            }
        }

        //
        // The expected attrs are the ones observed by this instance after
        // its own modifications.
        //

        for (auto& [name, attrs]: State.Files) {
            const auto path = OwnDir / name;
            const auto st = LStat(path);
            if (st.IsNull()) {
                return MakeSystemError("lstat", path);
            }
            attrs = ToFileAttrs(st);
        }

        const auto st = LStat(OwnDir);
        if (st.IsNull()) {
            return MakeSystemError("lstat", OwnDir);
        }
        State.DirAttrs = ToFileAttrs(st);

        return {};
    }

    NProto::TError OverwriteFile(const TString& name)
    {
        const auto path = OwnDir / name;
        TFileHandle file(path, OpenExisting | WrOnly);
        if (!file.IsOpen()) {
            return MakeSystemError("open", path);
        }

        //
        // The files are never empty, see CreateFile.
        //

        const i64 size = file.GetLength();
        if (size <= 0) {
            return MakeSystemError("fstat", path);
        }

        const TString data(
            1 + RandomNumber<ui64>(size),
            'A' + RandomNumber<ui32>(26));
        if (file.Pwrite(data.data(), data.size(), 0) !=
                static_cast<i32>(data.size()))
        {
            return MakeSystemError("pwrite", path);
        }

        if (!file.Close()) {
            return MakeSystemError("close", path);
        }

        return {};
    }

    NProto::TError CreateFile(const TString& name)
    {
        const auto path = OwnDir / name;
        TFileHandle file(path, CreateNew | WrOnly | Seq);
        if (!file.IsOpen()) {
            return MakeSystemError("open", path);
        }

        //
        // The files are never empty so that they can be overwritten without
        // changing their size.
        //

        const TString data(
            1 + RandomNumber<ui32>(Max(Options.FileSize, 1U)),
            'a' + NameCounter % 26);
        size_t written = 0;
        while (written < data.size()) {
            const i32 res =
                file.Write(data.data() + written, data.size() - written);
            if (res < 0) {
                return MakeSystemError("write", path);
            }
            written += res;
        }

        if (!file.Close()) {
            return MakeSystemError("close", path);
        }

        State.Files[name] = {};
        return {};
    }

    void ReportError(const TDirState& state, const TString& message)
    {
        const TString error = TStringBuilder() << "round " << state.Round
            << ", instance " << state.InstanceId << ": " << message;
        STORAGE_ERROR(error);
        ++ErrorCount;
        if (ErrorSamples.size() < MaxReportedErrors) {
            ErrorSamples.push_back(error);
        }
    }

    void Check(const TDirState& state)
    {
        const auto dir = TestDir / InstanceDirName(state.InstanceId);

        //
        // The directory attrs (MTime and CTime in the first place) should be
        // the ones observed by the owner.
        //

        const auto dirStat = LStat(dir);
        if (dirStat.IsNull()) {
            ReportError(state, FormatError(MakeSystemError("lstat", dir)));
        } else if (const auto actual = ToFileAttrs(dirStat);
                   actual != state.DirAttrs)
        {
            ReportError(state, TStringBuilder() << "directory: expected "
                << FormatAttrs(state.DirAttrs)
                << ", actual " << FormatAttrs(actual));
        }

        //
        // The listing should contain exactly the expected names.
        // TFsPath::ListNames reports errors via exceptions.
        //

        TVector<TString> names;
        try {
            dir.ListNames(names);
        } catch (...) {
            ReportError(state, TStringBuilder() << "failed to list "
                << dir.GetPath().Quote() << ": " << CurrentExceptionMessage());
            return;
        }
        const THashSet<TString> listed(names.begin(), names.end());

        for (const auto& [name, _]: state.Files) {
            if (!listed.contains(name)) {
                ReportError(state, TStringBuilder() << name.Quote()
                    << " is missing from the listing");
            }
        }
        for (const auto& name: listed) {
            if (!state.Files.contains(name)) {
                ReportError(state, TStringBuilder() << name.Quote()
                    << " is listed but not expected");
            }
        }

        //
        // Each expected file should have the expected attrs.
        //

        for (const auto& [name, expected]: state.Files) {
            ++CheckedFiles;
            const auto path = dir / name;
            const auto st = LStat(path);
            if (st.IsNull()) {
                ReportError(state, FormatError(MakeSystemError("lstat", path)));
                continue;
            }

            const auto actual = ToFileAttrs(st);
            if (actual != expected) {
                ReportError(state, TStringBuilder() << name.Quote()
                    << ": expected " << FormatAttrs(expected)
                    << ", actual " << FormatAttrs(actual));
            }
        }

        //
        // The renamed-from and unlinked names should not exist.
        //

        CheckAbsent(state, dir, state.RenamedFrom, "renamed-from");
        CheckAbsent(state, dir, state.Unlinked, "unlinked");
    }

    void CheckAbsent(
        const TDirState& state,
        const TFsPath& dir,
        const TVector<TString>& names,
        const TString& kind)
    {
        for (const auto& name: names) {
            ++CheckedAbsentNames;
            const auto path = dir / name;
            const auto st = LStat(path);
            if (!st.IsNull()) {
                ReportError(state, TStringBuilder() << kind << " "
                    << name.Quote() << " should not exist, actual "
                    << FormatAttrs(ToFileAttrs(st)));
            } else if (LastSystemError() != ENOENT) {
                ReportError(
                    state,
                    TStringBuilder() << kind << " " << name.Quote() << ": "
                        << FormatError(MakeSystemError("lstat", path)));
            }
        }
    }

    void WriteReport() const
    {
        TOFStream os(Options.ReportPath);
        NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &os);
        buf.SetIndentSpaces(4);

        buf.BeginObject();
        buf.WriteKey("instance");
        buf.WriteULongLong(Options.InstanceId);
        buf.WriteKey("rounds");
        buf.WriteULongLong(RoundsDone);
        buf.WriteKey("checkedFiles");
        buf.WriteULongLong(CheckedFiles);
        buf.WriteKey("checkedAbsentNames");
        buf.WriteULongLong(CheckedAbsentNames);
        buf.WriteKey("errors");
        buf.WriteULongLong(ErrorCount);
        buf.WriteKey("errorSamples");
        buf.BeginList();
        for (const auto& error: ErrorSamples) {
            buf.WriteString(error);
        }
        buf.EndList();
        buf.WriteKey("fatalError");
        buf.WriteString(FatalError);
        buf.EndObject();

        os.Flush();

        STORAGE_INFO("Report: " << Options.ReportPath
            << ", rounds: " << RoundsDone
            << ", errors: " << ErrorCount);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int RunMultiClient(TLog& Log, const TOptions& options)
{
    TMultiClient client(Log, options);
    return client.Run();
}

}   // namespace NCloud::NFileStore
