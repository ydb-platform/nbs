#include "fs_impl.h"

#include "fs_directory_handle.h"

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/random/random.h>
#include <util/system/mutex.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NFuse {

using namespace NCloud::NFileStore::NVFS;


namespace {

////////////////////////////////////////////////////////////////////////////////

bool CheckDirectoryHandle(
    fuse_req_t req,
    fuse_ino_t ino,
    const TDirectoryHandle& handle,
    TLog& Log,
    const char* funcName)
{
    if (handle.Index != ino) {
        STORAGE_ERROR("request #" << fuse_req_unique(req)
            << " consistency violation: " << funcName
            << " (handle.Index != ino) : "  <<
            "(" << handle.Index << " != " << ino << ")");

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
    explicit TDirectoryBuilder(size_t size) noexcept
        : Buffer(std::make_shared<TBuffer>(size))
    {}

#if defined(FUSE_VIRTIO)
    void Add(
        fuse_req_t req,
        const TString& name,
        const fuse_entry_param& entry,
        size_t offset)
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
    void Add(
        fuse_req_t req,
        const TString& name,
        const fuse_entry_param& entry,
        size_t offset)
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
    with_lock (DirectoryHandlesLock) {
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
    with_lock (DirectoryHandlesLock) {
        do {
            id = RandomNumber<ui64>();
        } while (!DirectoryHandles.try_emplace(id, handle).second);

        if (DirectoryHandlesStorage) {
            DirectoryHandlesStorage->StoreHandle(id, TDirectoryHandleChunk{.Index = ino});
        }

        DirectoryHandlesStats->IncreaseCacheSize(handle->GetSerializedSize());
        DirectoryHandlesStats->IncreaseChunkCount(handle->GetChunkCount());
    }

    fuse_file_info info = {};
    info.flags = fi->flags;
    info.fh = id;

    const int res = ReplyOpen(*callContext, {}, req, &info);
    if (res != 0) {
        // syscall was interrupted
        with_lock (DirectoryHandlesLock) {
            auto it = DirectoryHandles.find(id);
            if (it != DirectoryHandles.end()) {
                DirectoryHandlesStats->DecreaseCacheSize(
                    it->second->GetSerializedSize());
                DirectoryHandlesStats->DecreaseChunkCount(
                    it->second->GetChunkCount());
            }
            DirectoryHandles.erase(id);
            if (DirectoryHandlesStorage) {
                DirectoryHandlesStorage->RemoveHandle(id);
            }
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
        << " size:" << size
        << " fh:" << fi->fh);

    std::shared_ptr<TDirectoryHandle> handle;
    with_lock (DirectoryHandlesLock) {
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

    if (!CheckDirectoryHandle(req, ino, *handle, Log, __func__)) {
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
        DirectoryHandlesStats->DecreaseCacheSize(handle->GetSerializedSize());
        DirectoryHandlesStats->DecreaseChunkCount(handle->GetChunkCount());
        handle->ResetContent();
        DirectoryHandlesStats->IncreaseCacheSize(handle->GetSerializedSize());
        DirectoryHandlesStats->IncreaseChunkCount(handle->GetChunkCount());
        if (DirectoryHandlesStorage) {
            DirectoryHandlesStorage->ResetHandle(fi->fh);
        }
    } else if (auto content = handle->ReadContent(size, offset, Log)) {
        reply(*this, *content);
        return;
    }

    auto request = StartRequest<NProto::TListNodesRequest>(ino);
    request->SetCookie(handle->GetCookie());
    request->SetMaxBytes(Config->GetMaxBufferSize());

    // We don't know the nodes yet so we have to adjust the node size after
    // receiving ListNodes response.
    //
    // During ListNodes call, a node may be flushed and the information
    // about the node may be removed from WriteBackCache, and ListNodes
    // may return size without considering cached WriteData requests.
    //
    // Acquiring a node state reference prevents metadata from removal
    // for flushed nodes.
    const ui64 nodeStateRefId =
        WriteBackCache ? WriteBackCache.AcquireNodeStateRef() : 0;

    Session->ListNodes(callContext, std::move(request))
        .Subscribe(
            [=, fh = fi->fh, ptr = weak_from_this()](auto future) -> void
            {
                auto self = ptr.lock();

                Y_DEFER
                {
                    if (self && nodeStateRefId) {
                        self->WriteBackCache.ReleaseNodeStateRef(
                            nodeStateRefId);
                    }
                };

                NProto::TListNodesResponse response = future.ExtractValue();
                if (!CheckResponse(self, *callContext, req, response)) {
                    return;
                }

                if (response.NodesSize() != response.NamesSize()) {
                    STORAGE_ERROR(
                        "listnodes #" << fuse_req_unique(req)
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
                    builder.Add(
                        req,
                        ".",
                        {.attr = {.st_ino = MissingNodeId}},
                        offset);
                    builder.Add(
                        req,
                        "..",
                        {.attr = {.st_ino = MissingNodeId}},
                        offset);
                }

                if (WriteBackCache) {
                    for (auto& attr: *response.MutableNodes()) {
                        AdjustNodeSize(attr);
                    }
                }

                for (size_t i = 0; i < response.NodesSize(); ++i) {
                    const auto& attr = response.GetNodes(i);
                    const auto& name = response.GetNames(i);

                    const auto entryTimeout =
                        GetEntryCacheTimeout(attr).SecondsFloat();

                    fuse_entry_param entry = {
                        .ino = attr.GetId(),
                        .attr_timeout = Config->GetAttrTimeout().SecondsFloat(),
                        .entry_timeout = entryTimeout,
                    };

                    ConvertAttr(
                        Config->GetPreferredBlockSize(),
                        attr,
                        entry.attr);
                    if (!entry.attr.st_ino) {
                        const auto error = MakeError(
                            E_IO,
                            TStringBuilder() << "#" << fuse_req_unique(req)
                                             << " listed invalid entry: parent "
                                             << ino << ", name " << name.Quote()
                                             << ", stat " << DumpMessage(attr));

                        STORAGE_ERROR(error.GetMessage());
                        self->ReplyError(*callContext, error, req, EIO);
                        return;
                    }

                    builder.Add(req, name, entry, offset);
                }

                auto handleChunk = handle->UpdateContent(
                    size,
                    offset,
                    builder.Finish(),
                    response.GetCookie());

                STORAGE_TRACE(
                    "# " << fuse_req_unique(req) << " offset: " << offset
                         << " limit: " << size << " actual size "
                         << handleChunk.DirectoryContent.GetSize());

                if (DirectoryHandlesStorage) {
                    DirectoryHandlesStorage->UpdateHandle(fh, handleChunk);
                }

                DirectoryHandlesStats->IncreaseCacheSize(
                    handleChunk.GetSerializedSize());
                DirectoryHandlesStats->IncreaseChunkCount(1);

                reply(*self, handleChunk.DirectoryContent);
            });
}

void TFileSystem::ReleaseDir(
    TCallContextPtr callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    fuse_file_info* fi)
{
    STORAGE_DEBUG("ReleaseDir #" << ino);

    with_lock (DirectoryHandlesLock) {
        auto it = DirectoryHandles.find(fi->fh);
        if (it != DirectoryHandles.end()) {
            CheckDirectoryHandle(req, ino, *it->second, Log, __func__);

            DirectoryHandlesStats->DecreaseCacheSize(
                it->second->GetSerializedSize());
            DirectoryHandlesStats->DecreaseChunkCount(
                it->second->GetChunkCount());

            DirectoryHandles.erase(it);

            if (DirectoryHandlesStorage) {
                DirectoryHandlesStorage->RemoveHandle(fi->fh);
            }
        }
    }

    // should reply w/o lock
    ReplyError(*callContext, {}, req, 0);
}

bool TFileSystem::ValidateDirectoryHandle(
    TCallContext& callContext,
    fuse_req_t req,
    fuse_ino_t ino,
    uint64_t fh)
{
    std::shared_ptr<TDirectoryHandle> handle;
    with_lock (DirectoryHandlesLock) {
        auto it = DirectoryHandles.find(fh);
        if (it == DirectoryHandles.end()) {
            ReplyError(
                callContext,
                ErrorInvalidHandle(fh),
                req,
                EBADF);
            return false;
        }

        handle = it->second;
    }

    Y_ABORT_UNLESS(handle);

    if (!CheckDirectoryHandle(req, ino, *handle, Log, __func__)) {
        ReplyError(callContext, ErrorInvalidHandle(fh), req, EBADF);
        return false;
    }

    return true;
}

}   // namespace NCloud::NFileStore::NFuse
