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

class TDirectoryHandle
{
private:
    TMutex Lock;
    TMap<ui64, TBufferPtr> Content;

public:
    const fuse_ino_t Index;
    TString Cookie;

    explicit TDirectoryHandle(fuse_ino_t ino)
        : Index(ino)
    {}

    TDirectoryContent UpdateContent(
        size_t size,
        size_t offset,
        const TBufferPtr& content,
        TString cookie);

    TMaybe<TDirectoryContent>
    ReadContent(size_t size, size_t offset, TLog& Log);
    void ResetContent();
    TString GetCookie();
    void Serialize(TBufferOutput& output) const;

    static std::shared_ptr<TDirectoryHandle> Deserialize(TMemoryInput& input);
};

}   // namespace NCloud::NFileStore::NFuse
