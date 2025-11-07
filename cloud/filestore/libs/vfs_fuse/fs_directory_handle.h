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

using namespace NCloud::NFileStore::NVFS;

using TBufferPtr = std::shared_ptr<TBuffer>;

struct TDirectoryContent
{
    TBufferPtr Content = nullptr;
    size_t Offset = 0;
    size_t Size = 0;

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
};

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandle
{
private:
    TString Cookie;
    TMap<ui64, TBufferPtr> Content;
    ui64 UpdateVersion = 0;

    TMutex Lock;

public:
    const fuse_ino_t Index;

    explicit TDirectoryHandle(fuse_ino_t ino)
        : Index(ino)
    {}

    TDirectoryHandleChunk UpdateContent(
        size_t size,
        size_t offset,
        const TBufferPtr& content,
        TString cookie);

    TMaybe<TDirectoryContent>
    ReadContent(size_t size, size_t offset, TLog& Log);
    void ResetContent();
    TString GetCookie();

    // not thread safe, use only during restoration from storage
    void ConsumeChunk(TDirectoryHandleChunk& chunk);
};

}   // namespace NCloud::NFileStore::NFuse
