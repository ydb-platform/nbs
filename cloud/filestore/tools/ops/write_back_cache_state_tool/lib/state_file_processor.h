#pragma once

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/protos/write_back_cache_state_tool.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

class TStateFileProcessor
{
public:
    // The caller must keep the mapped file stable for the duration of the
    // dump, normally by holding a shared TExistingFileLock. A Debug-mode
    // accessor is required to expose recoverable structures from corrupt
    // files.
    static NProto::TStateFileDump DumpStateFile(
        TFileRingBufferAccessor& accessor);
};

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
