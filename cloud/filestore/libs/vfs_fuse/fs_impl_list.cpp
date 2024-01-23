#include "fs_impl.h"

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/random/random.h>
#include <util/system/mutex.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TBufferPtr = std::shared_ptr<TBuffer>;

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryContent {
    TBufferPtr Content = nullptr;
    size_t Offset = 0;
    size_t Size = 0;

    const char* GetData() const {
        return Content ? Content->Data() + Offset : nullptr;
    }

    size_t GetSize() const {
        return Content ? Min(Size, Content->Size() - Offset) : 0;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle
{
private:
    TMap<ui64, TBufferPtr> Content;
    TMutex Lock;

public:
    const fuse_ino_t Index;
    TString Cookie;

    TDirectoryHandle(fuse_ino_t ino)
        : Index(ino)
    {}

    TDirectoryContent UpdateContent(
        size_t size,
        size_t offset,
        const TBufferPtr& content,
        TString cookie)
    {
        with_lock (Lock) {
            size_t end = offset + content->size();
            Y_ABORT_UNLESS(Content.upper_bound(end) == Content.end());
            Content[end] = content;
            Cookie = std::move(cookie);
        }

        return TDirectoryContent{content, 0, size};
    }

    TMaybe<TDirectoryContent> ReadContent(size_t size, size_t offset, TLog& Log)
    {
        size_t end = 0;
        TBufferPtr content = nullptr;

        with_lock (Lock) {
            auto it = Content.upper_bound(offset);
            if (it != Content.end()) {
                end = it->first;
                content = it->second;
            } else if (Cookie) {
                return Nothing();
            }
        }

        TDirectoryContent result;
        if (content) {
            offset = offset - (end - content->size());
            if (offset >= content->size()) {
                STORAGE_ERROR("off %lu size %lu", offset, content->size());
                return Nothing();
            }
            result = {content, offset, size};
        }

        return result;
    }

    void ResetContent()
    {
        with_lock (Lock) {
            Content.clear();
            Cookie.clear();
        }
    }

    TString GetCookie()
    {
        with_lock (Lock) {
            return Cookie;
        }
    }
};

namespace {

bool CheckDirectoryHandle(fuse_req_t req, fuse_ino_t ino, std::shared_ptr<TDirectoryHandle> handle, TLog& Log, const char* funcName)
{
    if (handle->Index != ino) {
        STORAGE_ERROR("request #" << fuse_req_unique(req)
            << " consistency violation: " << funcName
            << " (handle->Index != ino) : "  <<
            "(" << handle->Index << " != " << ino << ")");
        return false;
    }
    return true;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDirectoryBuilder
{
private:
    TBufferPtr Buffer;

public:
    TDirectoryBuilder(size_t size) noexcept
        : Buffer(std::make_shared<TBuffer>(size))
    {}

#if defined(FUSE_VIRTIO)
    void Add(fuse_req_t req, const TString& name, const fuse_entry_param& entry, size_t offset)
    {
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
    }
#else
    void Add(fuse_req_t req, const TString& name, const fuse_entry_param& entry, size_t offset)
    {
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
    }
#endif

    TBufferPtr Finish()
    {
        return std::move(Buffer);
    }
};


////////////////////////////////////////////////////////////////////////////////

void TFileSystem::ClearDirectoryCache()
{
    with_lock (CacheLock) {
        STORAGE_DEBUG("clear directory cache of size %lu",
            DirectoryHandles.size());
        DirectoryHandles.clear();
    }
}

////////////////////////////////////////////////////////////////////////////////
// directory listing

void TFileSystem::OpenDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("OpenDir #" << ino << " @" << fi->flags);

    ui64 id = 0;
    auto handle = std::make_shared<TDirectoryHandle>(ino);
    with_lock (CacheLock) {
        do {
            id = RandomNumber<ui64>();
        } while (!DirectoryHandles.try_emplace(id, handle).second);
    }

    fuse_file_info info = {};
    info.flags = fi->flags;
    info.fh = id;

    const int res = ReplyOpen(*callContext, {}, req, &info);
    if (res != 0) {
        // syscall was interrupted
        with_lock (CacheLock) {
            DirectoryHandles.erase(id);
        }
    }
}

void TFileSystem::ReadDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    size_t size,
    off_t offset,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("ReadDir #" << ino
        << " offset:" << offset
        << " size:" << size);

    std::shared_ptr<TDirectoryHandle> handle;
    with_lock (CacheLock) {
        auto it = DirectoryHandles.find(fi->fh);
        if (it == DirectoryHandles.end()) {
            ReplyError(
                *callContext,
                ErrorInvalidHandle(fi->fh),
                req,
                EBADF);
            return;
        }

        handle = it->second;
    }

    Y_ABORT_UNLESS(handle);

    if (!CheckDirectoryHandle(req, ino, handle, Log, __func__)) {
        ReplyError(*callContext, ErrorInvalidHandle(fi->fh), req, EBADF);
        return;
    }

    auto reply = [=] (TFileSystem& fs, const TDirectoryContent& content) {
        fs.ReplyBuf(
            *callContext,
            {},
            req,
            content.GetData(),
            content.GetSize());
    };

    if (!offset) {
        // directory contents need to be refreshed on rewinddir()
        handle->ResetContent();
    } else if (auto content = handle->ReadContent(size, offset, Log)) {
        reply(*this, *content);
        return;
    }

    auto request = StartRequest<NProto::TListNodesRequest>(ino);
    request->SetCookie(handle->GetCookie());
    request->SetMaxBytes(Config->GetMaxBufferSize());

    Session->ListNodes(callContext, std::move(request))
        .Subscribe([=, ptr = weak_from_this()] (const auto& future) -> void {
            auto self = ptr.lock();
            const auto& response = future.GetValue();
            if (!CheckResponse(self, *callContext, req, response)) {
                return;
            } else if (response.NodesSize() != response.NamesSize()) {
                STORAGE_ERROR("listnodes #" << fuse_req_unique(req)
                    << " names/nodes count mismatch");

                self->ReplyError(
                    *callContext,
                    response.GetError(),
                    req,
                    EIO);
                return;
            }

            TDirectoryBuilder builder(size);
            if (offset == 0) {
                builder.Add(req, ".", { .attr = {.st_ino = MissingNodeId}}, offset);
                builder.Add(req, "..", { .attr = {.st_ino = MissingNodeId}}, offset);
            }

            for (size_t i = 0; i < response.NodesSize(); ++i) {
                const auto& attr = response.GetNodes(i);
                const auto& name = response.GetNames(i);

                fuse_entry_param entry = {
                    .ino = attr.GetId(),
                    .attr_timeout = (double)Config->GetAttrTimeout().Seconds(),
                    .entry_timeout = (double)Config->GetEntryTimeout().Seconds(),
                };

                ConvertAttr(Config->GetBlockSize(), attr, entry.attr);
                if (!entry.attr.st_ino) {
                    const auto error = MakeError(
                        E_IO,
                        TStringBuilder() << "#" << fuse_req_unique(req)
                        << " listed invalid entry: name " << name.Quote()
                        << ", stat " << DumpMessage(attr));

                    STORAGE_ERROR(error.GetMessage());
                    self->ReplyError(
                        *callContext,
                        error,
                        req,
                        EIO);
                    return;
                }

                builder.Add(req, name, entry, offset);
            }

            auto content = handle->UpdateContent(
                size,
                offset,
                builder.Finish(),
                response.GetCookie());

            STORAGE_TRACE("# " << fuse_req_unique(req)
                << " offset: " << offset
                << " limit: " << size
                << " actual size " << content.GetSize());

            reply(*self, content);
        });
}

void TFileSystem::ReleaseDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("ReleaseDir #" << ino);

    with_lock (CacheLock) {
        auto it = DirectoryHandles.find(fi->fh);
        if (it != DirectoryHandles.end()) {
            CheckDirectoryHandle(req, ino, it->second, Log, __func__);
            DirectoryHandles.erase(it);
        }
    }

    // should reply w/o lock
    ReplyError(*callContext, {}, req, 0);
}

}   // namespace NCloud::NFileStore::NFuse
