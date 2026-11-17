#pragma once

#include "options.h"

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NStateFileTool {

////////////////////////////////////////////////////////////////////////////////

void ReadStateFileDumpJson(IInputStream& input, NProto::TStateFileDump& state);

void WriteStateFileDumpJson(
    const NProto::TStateFileDump& state,
    IOutputStream& output);

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TOptions& options);

}   // namespace NCloud::NFileStore::NStateFileTool
