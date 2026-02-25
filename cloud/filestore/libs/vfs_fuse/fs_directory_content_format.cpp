#include "fs_directory_content_format.h"

#include <util/system/align.h>

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

////////////////////////////////////////////////////////////////////////////////

NProto::TError VisitEntries(
    char* data,
    ui64 len,
    const TDirEntryAttrVisitor& visitor)
{
#if defined(FUSE_VIRTIO)
    while (len > sizeof(fuse_direntplus)) {
        auto* de = reinterpret_cast<fuse_direntplus*>(data);

        if (de->dirent.ino != de->entry_out.attr.ino) {
            return MakeError(E_INVALID_STATE, TStringBuilder() << "ino mismatch"
                << " in dirent and attr: " << de->dirent.ino << " != "
                << de->entry_out.attr.ino);
        }

        visitor(de->entry_out);

        const ui64 fullSize = sizeof(fuse_direntplus)
            + AlignUp<ui64>(de->dirent.namelen, sizeof(ui64));

        if (len < fullSize) {
            return MakeError(E_INVALID_STATE, TStringBuilder() << "expected >= "
                << fullSize << " bytes of dir content, have " << len
                << " bytes");
        }

        len -= fullSize;
        data += fullSize;
    }
#else
    // for non-virtiofs builds we don't return attrs in listing results

    Y_UNUSED(data);
    Y_UNUSED(len);
    Y_UNUSED(visitor);
#endif

    return MakeError(S_OK);
}

}   // namespace NCloud::NFileStore::NFuse
