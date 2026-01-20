#pragma once

#include <util/generic/function_ref.h>
#include <util/generic/strbuf.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

struct IPersistentStorage
{
    using TVisitor = TFunctionRef<void(TStringBuf buffer)>;
    using TAllocationWriter = TFunctionRef<void(char* ptr, size_t size)>;

    virtual ~IPersistentStorage() = default;

    virtual bool Empty() const = 0;

    // Enumerates the contents of the persistent storage in the allocation order
    virtual void Visit(const TVisitor& visitor) = 0;

    // Gets the maximum possible buffer size that can be allocated.
    // It guaranteed to be successfully allocated if the storage is empty.
    virtual ui64 GetMaxSupportedAllocationByteCount() const = 0;

    // Allocates a buffer of the given size.
    //
    // On successful allocation, calls the writer, which should fill the buffer,
    // and returns a pointer to the buffer in persistent storage.
    // The returned pointer may differ from the pointer passed to the writer.
    //
    // Returns nullptr if there is not enough free space in the storage.
    // Throws an exception if the allocation size exceeds
    // MaxSupportedAllocationByteCount.
    virtual const void* Alloc(const TAllocationWriter& writer, size_t size) = 0;

    // Frees a previously allocated buffer.
    virtual void Free(const void* ptr) = 0;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
