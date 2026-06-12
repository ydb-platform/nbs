#pragma once

#include "fs_directory_handle.h"

#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 MissingNodeId = -1;

////////////////////////////////////////////////////////////////////////////////

class TDirectoryBuilder
{
private:
    TBufferPtr Buffer;

public:
    explicit TDirectoryBuilder(size_t size) noexcept;

    /*
     * This method serializes fuse_entry_param into the underlying buffer.
     * THE NULL TERMINATOR OF THE name FIELD MAY BE CUT OFF!
     * Use dirent::namelen to get actual length of dirent::name. Do not use
     * strlen!
     */
    void Add(
        fuse_req_t req,
        const TString& name,
        const fuse_entry_param& entry,
        size_t offset);

    TBufferPtr Finish();
};

////////////////////////////////////////////////////////////////////////////////

using TNodeIdVisitor = std::function<bool(ui64)>;
using TDirectoryEntryVisitor = std::function<bool(ui64, TStringBuf)>;
NProto::TError ResetCacheTimeouts(
    char* data,
    ui64 len,
    const TNodeIdVisitor& shouldResetAttrTimeout,
    const TDirectoryEntryVisitor& shouldResetEntryTimeout);

}   // namespace NCloud::NFileStore::NFuse
