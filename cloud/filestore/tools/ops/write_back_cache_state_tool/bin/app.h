#pragma once

#include "options.h"

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/protos/write_back_cache_state_tool.pb.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

void ReadStateFileDumpJson(IInputStream& input, NProto::TStateFileDump& state);

void WriteStateFileDumpJson(
    const NProto::TStateFileDump& state,
    IOutputStream& output);

////////////////////////////////////////////////////////////////////////////////

int AppMain(const TOptions& options);

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
