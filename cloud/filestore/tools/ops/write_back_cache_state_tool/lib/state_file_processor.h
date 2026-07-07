#pragma once

#include <cloud/filestore/tools/ops/write_back_cache_state_tool/protos/write_back_cache_state_tool.pb.h>

#include <cloud/storage/core/libs/file_backed_containers/file_ring_buffer_accessor.h>
#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NFileStore::NWriteBackCacheStateTool {

////////////////////////////////////////////////////////////////////////////////

class TStateFileProcessor
{
public:
    static NProto::TStateFileDump DumpStateFile(
        TFileRingBufferAccessor& accessor);

    static NCloud::NProto::TError PatchStateFile(
        TFileRingBufferAccessor& accessor,
        const NProto::TStateFileDump& newState);
};

}   // namespace NCloud::NFileStore::NWriteBackCacheStateTool
