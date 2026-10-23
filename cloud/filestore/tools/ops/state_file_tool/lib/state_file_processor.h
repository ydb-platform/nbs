#pragma once

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>
#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NFileStore::NStateFileTool {

////////////////////////////////////////////////////////////////////////////////

class TStateFileProcessor
{
public:
    // The caller must keep the mapped file stable for the duration of either
    // operation, normally by holding a lock acquired with TryLock(file, false)
    // for dumping or TryLock(file, true) for patching. Patching also requires a
    // writable mapping. A Debug-mode accessor is required to expose recoverable
    // structures from corrupt files.
    static NProto::TStateFileDump DumpStateFile(
        TFileRingBufferAccessor& accessor);

    static NCloud::NProto::TError PatchStateFile(
        TFileRingBufferAccessor& accessor,
        const NProto::TStateFileDump& newState);
};

}   // namespace NCloud::NFileStore::NStateFileTool
