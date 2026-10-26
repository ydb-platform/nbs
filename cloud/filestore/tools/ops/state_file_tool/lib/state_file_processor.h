#pragma once

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

namespace NCloud::NFileStore::NStateFileTool {

////////////////////////////////////////////////////////////////////////////////

class TStateFileProcessor
{
public:
    // The caller must keep the mapped file stable for the duration of the
    // dump, normally by holding a shared lock acquired with TryLock(file, false).
    // A Debug-mode accessor is required to expose recoverable structures from
    // corrupt files. Payloads are decoded only for explicitly supported file
    // types; otherwise, the dump contains ring-buffer metadata and checksums.
    static NProto::TStateFileDump DumpStateFile(
        TFileRingBufferAccessor& accessor,
        NProto::EStateFileType fileType);
};

}   // namespace NCloud::NFileStore::NStateFileTool
