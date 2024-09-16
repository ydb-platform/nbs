#pragma once

#include <library/cpp/digest/crc32c/crc32c.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/system/filemap.h>

#include <functional>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TBrokenFileRingBufferEntry
{
    TString Data;
    ui32 ExpectedChecksum = 0;
    ui32 ActualChecksum = 0;
};

struct TFileRingBuffer
{
private:
    static const ui32 VERSION = 1;

    TFileMap Map;
    const ui32 MaxEntrySize;

    struct THeader
    {
        ui32 Version = 0;
        ui32 Capacity = 0;
        ui32 First = 0;
        ui32 Next = 0;
    };

    struct TEntryHeader
    {
        ui32 Size = 0;
        ui32 Checksum = 0;
    };

    char* Data = nullptr;
    ui32 Count = 0;

private:
    THeader* Header()
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    const THeader* Header() const
    {
        return reinterpret_cast<THeader*>(Map.Ptr());
    }

    char* Next()
    {
        return Data + Header()->Next;
    }

    static void WriteEntry(IOutputStream& os, TStringBuf data)
    {
        TEntryHeader eh;
        eh.Size = data.Size();
        eh.Checksum = Crc32c(data.Data(), data.Size());
        os.Write(&eh, sizeof(eh));
        os.Write(data);
    }

    void SkipSlackSpace()
    {
        if (!Empty()) {
            const auto* b = Data + Header()->First;
            const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
            if (eh->Size == 0) {
                Header()->First = 0;
            }
        }
    }

    using TVisitor = std::function<void(ui32 checksum, TStringBuf entry)>;

    ui32 VisitEntry(const TVisitor& visitor, ui32 pos) const
    {
        const auto* b = Data + pos;
        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        if (eh->Size == 0) {
            return 0;
        }

        TStringBuf entry(b + sizeof(TEntryHeader), eh->Size);
        visitor(eh->Checksum, {b + sizeof(TEntryHeader), eh->Size});
        return pos + sizeof(TEntryHeader) + eh->Size;
    }

    void Visit(const TVisitor& visitor) const
    {
        ui32 pos = Header()->First;
        while (pos > Header()->Next) {
            pos = VisitEntry(visitor, pos);
        }

        while (pos < Header()->Next) {
            pos = VisitEntry(visitor, pos);
            if (!pos) {
                // can happen if the buffer is corrupted
                break;
            }
        }
    }

public:
    TFileRingBuffer(const TString& filePath, ui32 capacity, ui32 maxEntrySize)
        : Map(filePath, TMemoryMapCommon::oRdWr)
        , MaxEntrySize(sizeof(TEntryHeader) + maxEntrySize)
    {
        const ui32 realSize = sizeof(THeader) + capacity;
        if (Map.Length() < realSize) {
            Map.ResizeAndRemap(0, realSize);
        }

        if (Header()->Version) {
            Y_ABORT_UNLESS(Header()->Version == VERSION);
            Y_ABORT_UNLESS(Header()->Capacity == capacity);
        } else {
            Header()->Capacity = capacity;
            Header()->Version = VERSION;
        }

        Data = static_cast<char*>(Map.Ptr()) + sizeof(THeader);

        SkipSlackSpace();
        Visit([this] (ui32 checksum, TStringBuf entry) {
            Y_UNUSED(checksum);
            Y_UNUSED(entry);
            ++Count;
        });
    }

public:
    bool Push(TStringBuf data)
    {
        const auto sz = data.Size() + sizeof(TEntryHeader);
        if (data.Empty() || sz > MaxEntrySize) {
            return false;
        }

        auto* ptr = Data + Header()->Next;

        if (!Empty()) {
            // checking that we have a contiguous chunk of sz + 1 bytes
            // 1 extra byte is needed to distinguish between an empty buffer
            // and a buffer which is completely full
            if (Header()->First < Header()->Next) {
                // we have a single contiguous occupied region
                ui32 freeSpace = Header()->Capacity - Header()->Next;
                if (freeSpace <= sz) {
                    if (Header()->First <= sz) {
                        // out of space
                        return false;
                    }

                    memset(ptr, 0, freeSpace);
                    ptr = Data;
                }
            } else {
                // we have two occupied regions
                ui32 freeSpace = Header()->First - Header()->Next;
                if (freeSpace <= sz) {
                    // out of space
                    return false;
                }
            }
        }

        TMemoryOutput mo(ptr, sz);
        WriteEntry(mo, data);

        Header()->Next = ptr - Data + sz;
        ++Count;

        return true;
    }

    TStringBuf Front() const
    {
        if (Empty()) {
            return {};
        }

        const auto* b = Data + Header()->First;
        const auto* eh = reinterpret_cast<const TEntryHeader*>(b);
        return {b + sizeof(TEntryHeader), eh->Size};
    }

    void Pop()
    {
        auto data = Front();
        if (!data) {
            return;
        }

        Header()->First += sizeof(TEntryHeader) + data.Size();
        --Count;

        SkipSlackSpace();
    }

    ui64 Size() const
    {
        return Count;
    }

    bool Empty() const
    {
        return Size() == 0;
    }

    auto Validate() const
    {
        TVector<TBrokenFileRingBufferEntry> entries;

        Visit([&] (ui32 checksum, TStringBuf entry) {
            const ui32 actualChecksum = Crc32c(entry.Data(), entry.Size());
            if (actualChecksum != checksum) {
                entries.push_back({
                    TString(entry),
                    checksum,
                    actualChecksum});
            }
        });

        return entries;
    }
};

}   // namespace NCloud::NFileStore
