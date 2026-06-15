#include "fs_directory_handle.h"

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Fixed-size serialization overhead for TDirectoryHandleChunk
constexpr size_t BaseSerializedSize = sizeof(ui64) +   // Index
                                      sizeof(ui64) +   // UpdateVersion
                                      sizeof(ui32);    // Cookie length

}   // namespace

////////////////////////////////////////////////////////////////////////////////

size_t TDirectoryHandleChunk::GetSerializedSize() const
{
    size_t size = BaseSerializedSize + Cookie.size();
    if (Key) {
        size += sizeof(ui64) + sizeof(ui32);   // Key value and content size
        if (DirectoryContent.Content) {
            size += DirectoryContent.Content->Size();
        }
    }
    return size;
}

void TDirectoryHandleChunk::Serialize(TBufferOutput& output) const
{
    output.Write(&Index, sizeof(Index));
    output.Write(&UpdateVersion, sizeof(UpdateVersion));

    ui32 cookieLen = Cookie.size();
    output.Write(&cookieLen, sizeof(cookieLen));
    if (cookieLen > 0) {
        output.Write(Cookie.data(), cookieLen);
    }

    if (!Key) {
        return;
    }

    ui64 keyValue = Key.value();

    output.Write(&keyValue, sizeof(keyValue));
    ui32 bufferSize = DirectoryContent.Content != nullptr
                          ? DirectoryContent.Content->Size()
                          : 0;
    output.Write(&bufferSize, sizeof(bufferSize));
    if (bufferSize > 0) {
        output.Write(DirectoryContent.Content->Data(), bufferSize);
    }
}

std::optional<TDirectoryHandleChunk> TDirectoryHandleChunk::Deserialize(
    TMemoryInput& input)
{
    if (input.Avail() < sizeof(fuse_ino_t)) {
        return std::nullopt;
    }

    TDirectoryHandleChunk chunk;

    if (input.Load(&chunk.Index, sizeof(chunk.Index)) != sizeof(chunk.Index)) {
        return std::nullopt;
    }

    if (input.Load(&chunk.UpdateVersion, sizeof(chunk.UpdateVersion)) !=
        sizeof(chunk.UpdateVersion))
    {
        return std::nullopt;
    }

    ui32 cookieLen = 0;
    if (input.Load(&cookieLen, sizeof(cookieLen)) != sizeof(cookieLen)) {
        return std::nullopt;
    }

    if (cookieLen > 0) {
        if (input.Avail() < cookieLen) {
            return std::nullopt;
        }
        chunk.Cookie.resize(cookieLen);
        if (input.Load(chunk.Cookie.begin(), cookieLen) != cookieLen) {
            return std::nullopt;
        }
    }

    if (input.Avail() > 0) {
        ui64 key = 0;
        if (input.Load(&key, sizeof(key)) != sizeof(key)) {
            return std::nullopt;
        }
        chunk.Key = key;

        ui32 bufferSize = 0;
        if (input.Load(&bufferSize, sizeof(bufferSize)) != sizeof(bufferSize)) {
            return std::nullopt;
        }

        if (bufferSize > 0) {
            if (input.Avail() < bufferSize) {
                return std::nullopt;
            }
            chunk.DirectoryContent.Content = std::make_shared<TBuffer>();
            chunk.DirectoryContent.Content->Resize(bufferSize);
            if (input.Load(
                    chunk.DirectoryContent.Content->Data(),
                    bufferSize) != bufferSize)
            {
                return std::nullopt;
            }
        }
    }

    return chunk;
}

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandle::TDirectoryHandle(fuse_ino_t ino)
    : SerializedSize(BaseSerializedSize)
    , Index(ino)
{}

TDirectoryHandleChunk TDirectoryHandle::UpdateContent(
    size_t size,
    size_t offset,
    const TBufferPtr& content,
    ui64 cacheVersion,
    TString cookie)
{
    size_t end = offset + content->size();
    TDirectoryHandleChunk chunk{
        .Key = end,
        .Index = Index,
        .DirectoryContent = {
            .Content = content,
            .Offset = 0,
            .Size = size,
            .CacheVersion = cacheVersion,
        },
    };

    with_lock (Lock) {
        Y_ABORT_UNLESS(Content.upper_bound(end) == Content.end());
        Content[end] = {.Buffer = content, .CacheVersion = cacheVersion};
        Cookie = std::move(cookie);
        chunk.Cookie = Cookie;
        chunk.UpdateVersion = ++UpdateVersion;
        SerializedSize += chunk.GetSerializedSize();
    }

    return chunk;
}

TMaybe<TDirectoryContent>
TDirectoryHandle::ReadContent(size_t size, size_t offset, TLog& Log)
{
    size_t end = 0;
    TBufferPtr content = nullptr;
    ui64 cacheVersion = 0;

    with_lock (Lock) {
        auto it = Content.upper_bound(offset);
        if (it != Content.end()) {
            end = it->first;
            content = it->second.Buffer;
            cacheVersion = it->second.CacheVersion;
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
        result = {
            .Content = content,
            .Offset = offset,
            .Size = size,
            .CacheVersion = cacheVersion};
    }

    return result;
}

void TDirectoryHandle::ResetContent()
{
    with_lock (Lock) {
        Content.clear();
        Cookie.clear();
        UpdateVersion = 0;
        SerializedSize = BaseSerializedSize;
    }
}

bool TDirectoryHandle::IsEmpty() const
{
    with_lock (Lock) {
        return Content.empty() && Cookie.empty() && UpdateVersion == 0 &&
               SerializedSize == BaseSerializedSize;
    }
}

TString TDirectoryHandle::GetCookie()
{
    with_lock (Lock) {
        return Cookie;
    }
}

TDirectoryHandleStats TDirectoryHandle::GetStats() const
{
    with_lock (Lock) {
        return {
            .SerializedSize = SerializedSize,
            .ChunkCount = UpdateVersion + 1,
        };
    }
}

ui64 TDirectoryHandle::GetEntryVersion() const
{
    with_lock (Lock) {
        return EntryVersion;
    }
}

void TDirectoryHandle::InvalidateEntries(ui64 version)
{
    with_lock (Lock) {
        if (EntryVersion < version) {
            EntryVersion = version;
        }
    }
}

void TDirectoryHandle::ConsumeChunk(TDirectoryHandleChunk& chunk, TLog& Log)
{
    Y_ABORT_UNLESS(Index == chunk.Index);

    const size_t chunkSize = chunk.GetSerializedSize();
    size_t serializedSizeDelta = 0;
    // The handle already counts BaseSerializedSize for the first chunk.
    // Add only the extra bytes for the first chunk. Count later chunks in full.
    if (chunk.UpdateVersion == 0) {
        Y_ABORT_UNLESS(
            chunkSize >= BaseSerializedSize,
            "Chunk size %zu is smaller than base serialized size %zu",
            chunkSize,
            BaseSerializedSize);
        serializedSizeDelta = chunkSize - BaseSerializedSize;
    } else {
        serializedSizeDelta = chunkSize;
    }

    SerializedSize += serializedSizeDelta;

    if (chunk.UpdateVersion > UpdateVersion) {
        UpdateVersion = chunk.UpdateVersion;
        Cookie = std::move(chunk.Cookie);
    }

    if (chunk.Key) {
        const auto insertResult = Content.emplace(
            chunk.Key.value(),
            std::move(chunk.DirectoryContent.Content));
        if (!insertResult.second) {
            STORAGE_WARN(
                "directory handle " << Index << " already contains chunk key "
                                    << chunk.Key.value() << ", update version "
                                    << chunk.UpdateVersion);
        }
    }
}

}   // namespace NCloud::NFileStore::NFuse
