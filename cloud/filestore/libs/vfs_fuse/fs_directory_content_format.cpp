#include "fs_directory_content_format.h"

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

TDirectoryBuilder::TDirectoryBuilder(size_t size) noexcept
    : Buffer(std::make_shared<TBuffer>(size))
{}

void TDirectoryBuilder::Add(
    fuse_req_t req,
    const TString& name,
    const fuse_entry_param& entry,
    size_t offset)
{
#if defined(FUSE_VIRTIO)
    size_t entrySize = fuse_add_direntry_plus(
        req,
        nullptr,
        0,
        name.c_str(),
        &entry,
        0);

    Buffer->Advance(entrySize);

    fuse_add_direntry_plus(
        req,
        Buffer->Pos() - entrySize,
        entrySize,
        name.c_str(),
        &entry,
        offset + Buffer->Size());
#else
    size_t entrySize = fuse_add_direntry(
        req,
        nullptr,
        0,
        name.c_str(),
        &entry.attr,
        0);

    Buffer->Advance(entrySize);

    fuse_add_direntry(
        req,
        Buffer->Pos() - entrySize,
        entrySize,
        name.c_str(),
        &entry.attr,
        offset + Buffer->Size());
#endif
}

TBufferPtr TDirectoryBuilder::Finish()
{
    return std::move(Buffer);
}

}   // namespace NCloud::NFileStore::NFuse
