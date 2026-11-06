#pragma once

#include <cloud/filestore/tools/ops/state_file_tool/protos/state_file_tool.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>
#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NFileStore::NStateFileTool {

////////////////////////////////////////////////////////////////////////////////

class TStateFileEditor
{
public:
    // The caller must keep the mapped file stable throughout either operation by
    // acquiring the exclusive advisory lock with TryLock(file). Check for errors
    // and a true result, map the same open file, and keep it alive until the
    // operation finishes so the lock remains held.
    //
    // Patching also requires a writable mapping.
    // A Debug-mode accessor is required to expose recoverable structures from
    // corrupt files. Payloads are decoded only for explicitly supported file
    // types; otherwise, the dump contains ring-buffer metadata and checksums.
    static NProto::TStateFileDump DumpStateFile(
        TFileRingBufferAccessor& accessor,
        NProto::EStateFileType fileType);

    // Retained live WriteBackCache entries must have valid request size, tag
    // and offset. Entries being freed or discarded are exempt from these checks.
    // Free entries cannot be made live.
    // DirectoryHandles state files are not supported by this ring-buffer editor.
    //
    // For an empty file with ReadPos at a slack marker and WritePos == 0,
    // set both cursors to zero in the patch, including metadata-only repairs.
    static NCloud::NProto::TError PatchStateFile(
        TFileRingBufferAccessor& accessor,
        const NProto::TStateFileDump& newState,
        NProto::EStateFileType fileType);
};

}   // namespace NCloud::NFileStore::NStateFileTool
