#pragma once

#include "fs_directory_handle.h"

#if defined(FUSE_VIRTIO)
#   include <cloud/contrib/virtiofsd/fuse.h>
#else
struct fuse_entry_out;
#endif

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TDirectoryBuilder
{
private:
    TBufferPtr Buffer;

public:
    explicit TDirectoryBuilder(size_t size) noexcept;

    void Add(
        fuse_req_t req,
        const TString& name,
        const fuse_entry_param& entry,
        size_t offset);

    TBufferPtr Finish();
};

////////////////////////////////////////////////////////////////////////////////

using TDirEntryAttrVisitor = std::function<void(fuse_entry_out& e)>;
NProto::TError VisitEntries(
    char* data,
    ui64 len,
    const TDirEntryAttrVisitor& visitor);

}   // namespace NCloud::NFileStore::NFuse
