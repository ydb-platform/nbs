#include "app.h"

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/lib/state_file_locator.h>
#include <cloud/filestore/tools/ops/write_back_cache_state_tool/lib/state_file_processor.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

#include <library/cpp/protobuf/json/config.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/system/file_lock.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TApp
{
    const TOptions Options;

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
                return ExecuteAction(&TApp::ActionPatch, false);
            default:
                Cerr << "Unknown command\n";
                return 1;
        }
    }

private:
    void PrintJson(auto&& proto)
    {
        using EMissingKeyMode =
            NProtobufJson::TProto2JsonConfig::MissingKeyMode;

        NProtobufJson::TProto2JsonConfig config;
        config.SetEnumMode(NProtobufJson::TProto2JsonConfig::EnumName)
            .SetFormatOutput(true)
            .SetMissingSingleKeyMode(EMissingKeyMode::MissingKeyDefault);

        if (Options.OutputFile.empty()) {
            NProtobufJson::Proto2Json(proto, Cout, config);
            Cout << '\n';
        } else {
            TOFStream stream(Options.OutputFile);
            NProtobufJson::Proto2Json(proto, stream, config);
        }
    }

    void ReadJson(auto& proto)
    {
        NProtobufJson::TJson2ProtoConfig config;

        if (Options.InputFile.empty()) {
            NProtobufJson::Json2Proto(Cin, proto, config);
        } else {
            TIFStream stream(Options.InputFile);
            NProtobufJson::Json2Proto(stream, proto, config);
        }
    }

    int ExecuteAction(
        int (TApp::*func)(TFileRingBufferAccessor& accessor),
        bool readOnly)
    {
        auto locator = CreateStateFileLocator(Options.StateDir);
        auto stateFileOrError = locator->LocateStateFile(
            Options.FsId,
            Options.SessionId,
            NProto::EStateFileType::WriteBackCache);

        if (HasError(stateFileOrError)) {
            Cerr << "Failed to locate state file: "
                 << FormatError(stateFileOrError.GetError()) << "\n";
            return 1;
        }

        const auto& stateFile = stateFileOrError.GetResult();

        Cerr << "Using state file: " << stateFile << "\n";

        TFileLock fileLock(stateFile, EFileLockType::Exclusive);

        if (!fileLock.TryAcquire()) {
            Cerr << "State file is locked by another process\n";
            if (Options.UnsafeIgnoreLock) {
                Cerr << "Proceeding with --unsafe-ignore-lock\n";
            } else {
                return 1;
            }
        }

        TFileMapFileRingBufferAccessor accessor(stateFile, readOnly);

        auto mapResult = accessor.Map();
        if (HasError(mapResult)) {
            Cerr << "Failed to open and map state file: "
                 << FormatError(mapResult) << "\n";
            return 1;
        }

        return (this->*func)(accessor);
    }

    // Actions

    int ActionList()
    {
        auto locator = CreateStateFileLocator(Options.StateDir);
        auto stateFileListOrError = locator->ListStateFiles();

        if (HasError(stateFileListOrError)) {
            Cerr << "Failed to list state files: "
                 << FormatError(stateFileListOrError.GetError()) << "\n";
            return 1;
        }

        PrintJson(stateFileListOrError.GetResult());

        return 0;
    }

    int ActionCheck(TFileRingBufferAccessor& accessor)
    {
        auto validationResult = accessor.ValidateAndInitialize();

        if (HasError(validationResult)) {
            Cerr << "Validation failed: " << FormatError(validationResult)
                 << "\n";
            return 1;
        }

        if (!accessor.IsInitialized()) {
            Cerr << "State file is not initialized\n";
            return 1;
        }

        Cerr << "State file is valid\n";
        return 0;
    }

    int ActionDump(TFileRingBufferAccessor& accessor)
    {
        auto dump = TStateFileProcessor::DumpStateFile(accessor);
        PrintJson(dump);
        return 0;
    }

    int ActionPatch(TFileRingBufferAccessor& accessor)
    {
        auto validationResult = accessor.ValidateAndInitialize();

        if (HasError(validationResult)) {
            Cerr << "State file is corrupted";
            if (Options.UnsafeIgnoreCorruption) {
                Cerr << ", proceeding with --unsafe-ignore-corruption\n";
            } else {
                Cerr << ", patching is forbidden\n";
                return 1;
            }
        }

        NProto::TStateFileDump newState;
        ReadJson(newState);

        auto patchResult =
            TStateFileProcessor::PatchStateFile(accessor, newState);

        if (HasError(patchResult)) {
            Cerr << "Failed to patch state file: " << FormatError(patchResult)
                 << "\n";

            if (patchResult.GetCode() == E_INVALID_STATE) {
                Cerr << "The state file has been modified since the patch was "
                        "prepared. Please re-dump the state file and prepare a "
                        "new patch.\n";
            }
            return 1;
        }

        Cerr << "State file patched successfully\n";
        return 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TOptions& options)
{
    return TApp(options).Run();
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
