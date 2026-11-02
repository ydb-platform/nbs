#include "app.h"

#include <cloud/filestore/tools/ops/state_file_tool/lib/file_lock.h>
#include <cloud/filestore/tools/ops/state_file_tool/lib/state_file_locator.h>
#include <cloud/filestore/tools/ops/state_file_tool/lib/state_file_editor.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NStateFileTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TString SerializeJson(const T& proto)
{
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.preserve_proto_field_names = true;

    TString json;
    const auto status =
        google::protobuf::util::MessageToJsonString(proto, &json, options);
    if (!status.ok()) {
        ythrow yexception()
            << "Failed to serialize protobuf as JSON: " << status.ToString();
    }
    return json;
}

////////////////////////////////////////////////////////////////////////////////

NProto::EStateFileType GetFileType(const TOptions& options)
{
    if (!options.StateFile.empty() &&
        options.FileType == NProto::EStateFileType::Unknown)
    {
        ythrow yexception()
            << "--state-file requires --file-type write-back-cache";
    }
    if (options.FileType != NProto::EStateFileType::Unknown &&
        options.FileType != NProto::EStateFileType::WriteBackCache)
    {
        ythrow yexception() << "Unsupported state file type: "
                            << static_cast<int>(options.FileType);
    }
    return options.FileType == NProto::EStateFileType::Unknown
               ? NProto::EStateFileType::WriteBackCache
               : options.FileType;
}

class TApp
{
private:
    const TOptions Options;
    const NProto::EStateFileType FileType;

public:
    explicit TApp(const TOptions& options)
        : Options(options)
        , FileType(GetFileType(options))
    {}

    int Run()
    {
        switch (Options.Command) {
            case ECommand::List:
                return ActionList();
            case ECommand::Check:
                return ExecuteAction(
                    true,
                    [this](auto& accessor) { return ActionCheck(accessor); });
            case ECommand::Dump:
                return ExecuteAction(
                    true,
                    [this](auto& accessor) { return ActionDump(accessor); });
            case ECommand::Patch: {
                // Parse input before taking the exclusive state-file lock. The
                // snapshot checksum is validated again while applying it.
                NProto::TStateFileDump patchState;
                ReadJson(patchState);
                return ExecuteAction(
                    false,
                    [this, &patchState](auto& accessor)
                    { return ActionPatch(accessor, patchState); });
            }
            case ECommand::UnknownCmd:
                Cerr << "Unknown command\n";
                return 1;
        }

        return 1;
    }

private:
    void PrintJson(const auto& proto)
    {
        const auto json = SerializeJson(proto);
        if (Options.OutputFile.empty()) {
            Cout << json << '\n';
            Cout.Flush();
        } else {
            // Require a new file so output cannot overwrite any state file.
            TFile outputFile(Options.OutputFile, CreateNew | WrOnly | Seq);
            TOFStream stream(outputFile);
            stream << json << '\n';
            stream.Finish();
        }
    }

    void ReadJson(NProto::TStateFileDump& proto)
    {
        if (Options.InputFile.empty()) {
            ReadStateFileDumpJson(Cin, proto);
        } else {
            TIFStream stream(Options.InputFile);
            ReadStateFileDumpJson(stream, proto);
        }
    }

    TResultOrError<TFile> LocateAndOpenStateFile(bool readOnly)
    {
        if (!Options.StateFile.empty()) {
            try {
                return TFile(
                    Options.StateFile,
                    OpenExisting | (readOnly ? RdOnly : RdWr));
            } catch (...) {
                return MakeError(
                    E_IO,
                    TStringBuilder()
                        << "Failed to open state file '" << Options.StateFile
                        << "': " << CurrentExceptionMessage());
            }
        }

        auto locator = CreateStateFileLocator(Options.StateDir);
        return locator->LocateAndOpenStateFile(
            Options.FsId,
            Options.SessionId,
            FileType,
            readOnly);
    }

    int ExecuteAction(bool readOnly, auto&& action)
    {
        auto stateFileOrError = LocateAndOpenStateFile(readOnly);
        if (HasError(stateFileOrError)) {
            Cerr << "Failed to access state file: "
                 << FormatError(stateFileOrError.GetError()) << '\n';
            return 1;
        }

        auto stateFile = stateFileOrError.GetResult();
        Cerr << "Using state file: " << stateFile.GetName() << '\n';

        // The lock is held until the last shared file handle closes.
        const auto lockOrError = TryLock(stateFile);
        if (HasError(lockOrError)) {
            Cerr << "Failed to lock state file: "
                 << FormatError(lockOrError.GetError()) << '\n';
            return 1;
        }

        if (!lockOrError.GetResult()) {
            Cerr << "State file is locked by another process\n";
            if (Options.UnsafeIgnoreLock) {
                Cerr << "Proceeding with --unsafe-ignore-lock\n";
            } else {
                return 1;
            }
        }

        TFileMapFileRingBufferAccessor accessor(
            stateFile,
            EFileRingBufferAccessorValidationMode::Debug,
            readOnly ? TMemoryMapCommon::EOpenModeFlag::oRdOnly
                     : TMemoryMapCommon::EOpenModeFlag::oRdWr);

        const auto mapError = accessor.Map();
        if (HasError(mapError)) {
            Cerr << "Failed to open and map state file: "
                 << FormatError(mapError) << '\n';
            return 1;
        }

        return action(accessor);
    }

    int ActionList()
    {
        auto locator = CreateStateFileLocator(Options.StateDir);
        auto stateFileListOrError = locator->ListStateFiles();
        if (HasError(stateFileListOrError)) {
            Cerr << "Failed to list state files: "
                 << FormatError(stateFileListOrError.GetError()) << '\n';
            return 1;
        }

        const auto& stateFileList = stateFileListOrError.GetResult();
        PrintJson(stateFileList);
        return 0;
    }

    int ActionCheck(TFileRingBufferAccessor& accessor)
    {
        const auto validationResult = accessor.ValidateAndInitialize();
        switch (validationResult) {
            case EFileRingBufferAccessorValidationStatus::NotInitialized:
                Cerr << "State file is not initialized\n";
                return 0;
            case EFileRingBufferAccessorValidationStatus::Failed:
                Cerr << "Validation failed: "
                     << FormatError(accessor.GetLastValidationError()) << '\n';
                return 1;
            case EFileRingBufferAccessorValidationStatus::Success:
                return 0;
        }

        return 1;
    }

    int ActionDump(TFileRingBufferAccessor& accessor)
    {
        PrintJson(TStateFileEditor::DumpStateFile(
            accessor,
            FileType));
        return 0;
    }

    int ActionPatch(
        TFileRingBufferAccessor& accessor,
        const NProto::TStateFileDump& patchState)
    {
        const auto patchError = TStateFileEditor::PatchStateFile(
            accessor,
            patchState,
            FileType,
            Options.UnsafeIgnoreCorruption);

        if (Options.UnsafeIgnoreCorruption &&
            accessor.GetDataProcessor() != nullptr &&
            HasError(accessor.GetLastValidationError()))
        {
            Cerr << "State file was corrupted: "
                 << FormatError(accessor.GetLastValidationError())
                 << "; --unsafe-ignore-corruption allows patching\n";
        }

        if (HasError(patchError)) {
            Cerr << "Failed to patch state file: " << FormatError(patchError)
                 << '\n';

            if (patchError.GetCode() == E_INVALID_STATE &&
                accessor.GetDataProcessor() != nullptr)
            {
                Cerr << "The patch does not match the current state file. "
                        "Please re-dump the state file and prepare a new "
                        "patch.\n";
            }
            return 1;
        }

        Cerr << "Patch applied; run 'check' to verify the state file\n";
        return 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ReadStateFileDumpJson(IInputStream& input, NProto::TStateFileDump& state)
{
    const TString inputJson = input.ReadAll();
    const auto status =
        google::protobuf::util::JsonStringToMessage(inputJson, &state);

    if (!status.ok()) {
        ythrow yexception()
            << "Failed to parse state file dump JSON: " << status.ToString();
    }
}

void WriteStateFileDumpJson(
    const NProto::TStateFileDump& state,
    IOutputStream& output)
{
    output << SerializeJson(state);
}

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TOptions& options)
{
    try {
        return TApp(options).Run();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << '\n';
        return 1;
    }
}

}   // namespace NCloud::NFileStore::NStateFileTool
