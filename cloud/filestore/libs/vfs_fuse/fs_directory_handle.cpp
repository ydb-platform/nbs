#include "fs_directory_handle.h"

namespace NCloud::NFileStore::NFuse {

TDirectoryContent TDirectoryHandle::UpdateContent(
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
    }
}

TString TDirectoryHandle::GetCookie()
{
    with_lock (Lock) {
        return Cookie;
    }
}

void TDirectoryHandle::Serialize(TBufferOutput& output) const
{
    with_lock (Lock) {
        output.Write(&Index, sizeof(Index));

        ui32 cookieLen = Cookie.size();
        output.Write(&cookieLen, sizeof(cookieLen));
        if (cookieLen > 0) {
            output.Write(Cookie.data(), cookieLen);
        }

        ui32 contentSize = Content.size();
        output.Write(&contentSize, sizeof(contentSize));

        for (const auto& [key, value]: Content) {
            output.Write(&key, sizeof(key));

            ui32 bufferSize = value ? value->Size() : 0;
            output.Write(&bufferSize, sizeof(bufferSize));
            if (bufferSize > 0) {
                output.Write(value->Data(), bufferSize);
            }
        }
    }
}

std::shared_ptr<TDirectoryHandle> TDirectoryHandle::Deserialize(
    TMemoryInput& input)
{
    if (input.Avail() < sizeof(fuse_ino_t)) {
        return nullptr;
    }

    fuse_ino_t index;
    if (input.Load(&index, sizeof(index)) != sizeof(index)) {
        return nullptr;
    }

    auto handle = std::make_shared<TDirectoryHandle>(index);

    ui32 cookieLen = 0;
    if (input.Load(&cookieLen, sizeof(cookieLen)) != sizeof(cookieLen)) {
        return nullptr;
    }

    TString cookie;
    if (cookieLen > 0) {
        if (input.Avail() < cookieLen) {
            return nullptr;
        }
        cookie.resize(cookieLen);
        if (input.Load(cookie.begin(), cookieLen) != cookieLen) {
            return nullptr;
        }
    }

    ui32 contentSize = 0;
    if (input.Load(&contentSize, sizeof(contentSize)) != sizeof(contentSize)) {
        return nullptr;
    }

    handle->Cookie = std::move(cookie);

    for (ui32 i = 0; i < contentSize; ++i) {
        ui64 key = 0;
        if (input.Load(&key, sizeof(key)) != sizeof(key)) {
            return nullptr;
        }

        ui32 bufferSize = 0;
        if (input.Load(&bufferSize, sizeof(bufferSize)) != sizeof(bufferSize)) {
            return nullptr;
        }

        if (bufferSize > 0) {
            if (input.Avail() < bufferSize) {
                return nullptr;
            }
            auto buffer = std::make_shared<TBuffer>();
            buffer->Resize(bufferSize);
            if (input.Load(buffer->Data(), bufferSize) != bufferSize) {
                return nullptr;
            }
            handle->Content[key] = buffer;
        } else {
            handle->Content[key] = nullptr;
        }
    }

    return handle;
}

}   // namespace NCloud::NFileStore::NFuse
