#pragma once

#include "fs_directory_handle.h"

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

    void Add(
        fuse_req_t req,
        const TString& name,
        const fuse_entry_param& entry,
        size_t offset);

    TBufferPtr Finish();
};

////////////////////////////////////////////////////////////////////////////////

using TNodeIdVisitor = std::function<bool(ui64)>;
NProto::TError ResetAttrTimeout(
    char* data,
    ui64 len,
    const TNodeIdVisitor& visitor);

}   // namespace NCloud::NFileStore::NFuse
