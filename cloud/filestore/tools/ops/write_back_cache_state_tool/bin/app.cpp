#include "app.h"

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/lib/file_lock.h>
#include <cloud/filestore/tools/ops/write_back_cache_state_tool/lib/state_file_locator.h>
#include <cloud/filestore/tools/ops/write_back_cache_state_tool/lib/state_file_processor.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void WriteJson(const T& proto, IOutputStream& output)
{
    google::protobuf::util::JsonPrintOptions options;
    options.add_whitespace = true;
    options.always_print_primitive_fields = true;
    options.preserve_proto_field_names = true;

    TString json;
    const auto status =
        google::protobuf::util::MessageToJsonString(proto, &json, options);
    if (!status.ok()) {
        ythrow yexception()
            << "Failed to serialize protobuf as JSON: " << status.ToString();
    }
    output.Write(json.data(), json.size());
}

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    const TOptions Options;
    NProto::TStateFileDump PatchState;

public:
    explicit TApp(const TOptions& options)
        : Options(options)
    {}

    int Run()
    {
        switch (Options.Command) {
            case ECommand::List:
                return ActionList();
            case ECommand::Check:
                return ExecuteAction(&TApp::ActionCheck, true);
            case ECommand::Dump:
                return ExecuteAction(&TApp::ActionDump, true);
            case ECommand::Patch:
                // Parse input before taking the exclusive state-file lock. The
                // snapshot checksum is validated again while applying it.
                ReadJson(PatchState);
                return ExecuteAction(&TApp::ActionPatch, false);
            case ECommand::UnknownCmd:
                Cerr << "Unknown command\n";
                return 1;
        }

        return 1;
    }

private:
    void PrintJson(const auto& proto)
    {
        if (Options.OutputFile.empty()) {
            WriteJson(proto, Cout);
            Cout << '\n';
            Cout.Flush();
        } else {
            // Require a new file so output cannot overwrite any state file.
            TFile outputFile(Options.OutputFile, CreateNew | WrOnly | Seq);
            TOFStream stream(outputFile);
            WriteJson(proto, stream);
            stream << '\n';
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
            NProto::EStateFileType::WriteBackCache,
            readOnly);
    }

    int ExecuteAction(
        int (TApp::*action)(TFileRingBufferAccessor& accessor),
        bool readOnly)
    {
        auto stateFileOrError = LocateAndOpenStateFile(readOnly);
        if (HasError(stateFileOrError)) {
            Cerr << "Failed to locate state file: "
                 << FormatError(stateFileOrError.GetError()) << '\n';
            return 1;
        }

        auto stateFile = stateFileOrError.GetResult();
        Cerr << "Using state file: " << stateFile.GetName() << '\n';

        // The lock belongs to stateFile and is released by its destructor.
        const auto lockOrError =
            TryLock(stateFile, /* exclusive = */ !readOnly);
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

        return (this->*action)(accessor);
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
                return 1;
            case EFileRingBufferAccessorValidationStatus::Failed:
                Cerr << "Validation failed: "
                     << FormatError(accessor.GetLastValidationError()) << '\n';
                return 1;
            case EFileRingBufferAccessorValidationStatus::Success:
                return 0;
        }

        return 2;
    }

    int ActionDump(TFileRingBufferAccessor& accessor)
    {
        PrintJson(TStateFileProcessor::DumpStateFile(accessor));
        return 0;
    }

    int ActionPatch(TFileRingBufferAccessor& accessor)
    {
        const auto validationResult = accessor.ValidateAndInitialize();
        switch (validationResult) {
            case EFileRingBufferAccessorValidationStatus::NotInitialized:
                Cerr << "State file is not initialized, patching is not "
                        "possible\n";
                return 1;
            case EFileRingBufferAccessorValidationStatus::Failed:
                Cerr << "State file is corrupted";
                if (!Options.UnsafeIgnoreCorruption) {
                    Cerr << ", patching is forbidden\n";
                    return 1;
                }
                Cerr << ", proceeding with --unsafe-ignore-corruption\n";
                break;
            case EFileRingBufferAccessorValidationStatus::Success:
                break;
        }

        const auto patchError =
            TStateFileProcessor::PatchStateFile(accessor, PatchState);

        if (HasError(patchError)) {
            Cerr << "Failed to patch state file: " << FormatError(patchError)
                 << '\n';

            if (patchError.GetCode() == E_INVALID_STATE) {
                Cerr << "The patch does not match the current state file. "
                        "Please re-dump the state file and prepare a new "
                        "patch.\n";
            }
            return 1;
        }

        Cerr << "State file patched successfully\n";
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
    WriteJson(state, output);
}

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TOptions& options)
{
    return TApp(options).Run();
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
