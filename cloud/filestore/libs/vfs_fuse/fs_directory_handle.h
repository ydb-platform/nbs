#pragma once

#include "public.h"

#include "fs.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/buffer.h>
#include <util/generic/map.h>
#include <util/stream/buffer.h>
#include <util/stream/mem.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

using namespace NCloud::NFileStore::NVFS;

using TBufferPtr = std::shared_ptr<TBuffer>;

struct TDirectoryContent
{
    TBufferPtr Content = nullptr;
    size_t Offset = 0;
    size_t Size = 0;
    // GlobalCacheVersion starts from 1
    // so this starting value is used to identify values
    // recovered after restart from persistent storage
    ui64 CacheVersion = 0;

    const char* GetData() const
    {
        return Content ? Content->Data() + Offset : nullptr;
    }

    size_t GetSize() const
    {
        return Content ? Min(Size, Content->Size() - Offset) : 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryHandleChunk
{
    std::optional<ui64> Key;
    ui64 UpdateVersion = 0;
    ui64 Index = 0;
    TString Cookie;
    TDirectoryContent DirectoryContent;

    void Serialize(TBufferOutput& output) const;
    static std::optional<TDirectoryHandleChunk> Deserialize(
        TMemoryInput& input);

    size_t GetSerializedSize() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TDirectoryHandleStats
{
    size_t SerializedSize = 0;
    size_t ChunkCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle
{
private:
    struct TContent
    {
        TBufferPtr Buffer;
        ui64 CacheVersion = 0;
    };

    TString Cookie;
    TMap<ui64, TContent> Content;
    ui64 UpdateVersion = 0;
    ui64 SerializedSize = 0;

    mutable TMutex Lock;

public:
    const fuse_ino_t Index;

public:
    explicit TDirectoryHandle(fuse_ino_t ino);

    TDirectoryHandleChunk UpdateContent(
        size_t size,
        size_t offset,
        const TBufferPtr& content,
        ui64 cacheVersion,
        TString cookie);

    TMaybe<TDirectoryContent>
    ReadContent(size_t size, size_t offset, TLog& Log);
    void ResetContent();
    TString GetCookie();

    // Returns the handle's current serialized size and chunk count.
    TDirectoryHandleStats GetStats() const;
    bool IsEmpty() const;

    // not thread safe, use only during restoration from storage
    void ConsumeChunk(TDirectoryHandleChunk& chunk, TLog& Log);
};

}   // namespace NCloud::NFileStore::NFuse
