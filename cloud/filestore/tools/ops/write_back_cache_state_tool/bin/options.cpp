#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

const THashMap<TString, ECommand> nameToCommand = {
    {"list", ECommand::List},
    {"check", ECommand::Check},
    {"dump", ECommand::Dump},
    {"patch", ECommand::Patch},
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TOptions::Parse(int argc, char** argv)
{
    TOpts opts;
    opts.AddHelpOption();

    TString commandName = "";
    opts.SetFreeArgsNum(1);
    opts.AddFreeArgBinding(
        "command",
        commandName,
        "specify command:\n"
        "  - list: list states and give summary in JSON format;\n"
        "  - check: check integrity of the state file;\n"
        "  - dump: output the contents of the state file in JSON format, "
        "the payload is replaced with a hash;\n"
        "  - patch: apply changes to the data returned by 'dump' command.\n");

    opts.AddLongOption(
            "state-dir",
            "path to a directory with write-back cache state files")
        .RequiredArgument("STR")
        .DefaultValue("/Berkanavt/nfs-vhost/state")
        .StoreResult(&StateDir);

    opts.AddLongOption("fs-id", "file system id")
        .RequiredArgument("STR")
        .StoreResult(&FsId);

    opts.AddLongOption(
            "session-id",
            "session id (can be omitted if there is only a single session)")
        .RequiredArgument("STR")
        .StoreResult(&SessionId);

    opts.AddLongOption(
            "state-file",
            "state file path (can be used instead of "
            "<state_dir>/<fs_id>/<session_id>)")
        .RequiredArgument("STR")
        .StoreResult(&StateFile);

    opts.AddLongOption(
            'I',
            "input",
            "take input data from a file instead of stdin")
        .RequiredArgument("STR")
        .StoreResult(&InputFile);

    opts.AddLongOption(
            'O',
            "output",
            "write command output to a file instead of stdout")
        .RequiredArgument("STR")
        .StoreResult(&OutputFile);

    opts.AddLongOption(
            "unsafe-ignore-lock",
            "continue even if the advisory lock cannot be acquired "
            "(dangerous, will be removed)")
        .StoreTrue(&UnsafeIgnoreLock);

    opts.AddLongOption(
            "unsafe-ignore-corruption",
            "allow patching corrupted files")
        .StoreTrue(&UnsafeIgnoreCorruption);

    TOptsParseResultException parser(&opts, argc, argv);

    const auto* command = nameToCommand.FindPtr(commandName);
    if (!command) {
        ythrow yexception() << "Unknown command: " << commandName;
    }
    Command = *command;

    const bool hasStateFile = parser.Has("state-file");
    const bool hasStateDir = parser.Has("state-dir");
    const bool hasFsId = parser.Has("fs-id");
    const bool hasSessionId = parser.Has("session-id");

    if (Command == ECommand::List) {
        if (hasStateFile || hasFsId || hasSessionId) {
            ythrow yexception()
                << "The list command accepts --state-dir only; "
                   "--state-file, --fs-id and --session-id are not allowed";
        }
        return;
    }

    if (hasStateFile) {
        if (StateFile.empty()) {
            ythrow yexception() << "State file path must not be empty";
        }

        // StateDir has a default value, so checking the stored value would
        // reject every use of --state-file.  Has() does not count defaults.
        if (hasStateDir || hasFsId || hasSessionId) {
            ythrow yexception() << "Cannot specify --state-file with "
                                   "--state-dir, --fs-id or --session-id";
        }
        return;
    }

    if (FsId.empty()) {
        ythrow yexception()
            << "--fs-id is required unless --state-file is specified";
    }
}

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
