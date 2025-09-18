#include "fs_directory_handle.h"

namespace NCloud::NFileStore::NFuse {

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

TDirectoryHandleChunk TDirectoryHandle::UpdateContent(
    size_t size,
    size_t offset,
    const TBufferPtr& content,
    TString cookie)
{
    size_t end = offset + content->size();
    TDirectoryHandleChunk chunk{
        .Key = end,
        .Index = Index,
        .DirectoryContent = {content, 0, size}};

    with_lock (Lock) {
        Y_ABORT_UNLESS(Content.upper_bound(end) == Content.end());
        Content[end] = content;
        Cookie = std::move(cookie);
        chunk.Cookie = Cookie;
        chunk.UpdateVersion = ++UpdateVersion;
    }

    return chunk;
}

TMaybe<TDirectoryContent>
TDirectoryHandle::ReadContent(size_t size, size_t offset, TLog& Log)
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

void TDirectoryHandle::ResetContent()
{
    with_lock (Lock) {
        Content.clear();
        Cookie.clear();
        UpdateVersion = 0;
    }
}

TString TDirectoryHandle::GetCookie()
{
    with_lock (Lock) {
        return Cookie;
    }
}

void TDirectoryHandle::ConsumeChunk(TDirectoryHandleChunk& chunk)
{
    Y_ABORT_UNLESS(Index == chunk.Index);

    if (chunk.UpdateVersion > UpdateVersion) {
        UpdateVersion = chunk.UpdateVersion;
        Cookie = std::move(chunk.Cookie);
    }

    if (chunk.Key) {
        Content.emplace(
            chunk.Key.value(),
            std::move(chunk.DirectoryContent.Content));
    }
}

}   // namespace NCloud::NFileStore::NFuse
