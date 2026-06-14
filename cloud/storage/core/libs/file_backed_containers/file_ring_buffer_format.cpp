#include "file_ring_buffer_format.h"

#include <util/digest/numeric.h>
#include <util/generic/utility.h>
#include <util/system/align.h>
#include <util/system/compiler.h>
#include <util/system/yassert.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TEntryHeader, bool RequireAlignment>
class TData
{
private:
    std::span<char> Data;

public:
    explicit TData(std::span<char> data)
        : Data(data)
    {
        if constexpr (RequireAlignment) {
            Y_ABORT_UNLESS(
                AlignDown(Data.data(), sizeof(ui64)) == Data.data(),
                "Data buffer is not properly aligned");
        }
    }

    ui64 Size() const
    {
        return Data.size();
    }

    // Treats memory at position pos as TEntryHeader.
    // Checks for alignment if RequireAlignment is set.
    // Ensures that [pos, pos + sizeof(TEntryHeader) is within data bounds.
    // Returns pointer to the header or nullptr if it is outside data bounds
    // or is not aligned.
    TEntryHeader* GetEntryHeaderPtr(ui64 pos) const
    {
        if constexpr (RequireAlignment) {
            if (AlignDown(pos, sizeof(ui64)) != pos) {
                return nullptr;
            }
        }

        const bool valid =
            pos < Data.size() && sizeof(TEntryHeader) <= Data.size() - pos;

        return valid ? reinterpret_cast<TEntryHeader*>(Data.data() + pos)
                     : nullptr;
    }

    // Treats memory at position pos as TEntryHeader and memory right after
    // the header as entry data.
    // Checks for alignment if RequireAlignment is set.
    // Ensures that [pos, pos + sizeof(TEntryHeader) + dataSize is within data
    // bounds.
    // Returns pointer to entry data or nullptr if it is outside data bounds
    // or is not aligned.
    char* GetEntryDataPtr(ui64 pos, ui64 dataSize) const
    {
        if constexpr (RequireAlignment) {
            if (AlignDown(pos, sizeof(ui64)) != pos) {
                return nullptr;
            }
        }

        if (pos >= Data.size()) {
            return nullptr;
        }

        ui64 tailSize = Data.size() - pos;
        if constexpr (RequireAlignment) {
            tailSize = AlignDown(tailSize, sizeof(ui64));
        }

        const bool valid = sizeof(TEntryHeader) <= tailSize &&
                           dataSize <= tailSize - sizeof(TEntryHeader);

        return valid ? Data.data() + pos + sizeof(TEntryHeader) : nullptr;
    }

    ui64 GetMaxDataSize(ui64 maxEntrySize) const
    {
        if constexpr (RequireAlignment) {
            maxEntrySize = AlignDown(maxEntrySize, sizeof(ui64));
        }

        return maxEntrySize > sizeof(TEntryHeader)
                   ? maxEntrySize - sizeof(TEntryHeader)
                   : 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferDataProcessorV4: public IFileRingBufferDataProcessor
{
private:
    // Entry header layout (lower bits come first):
    // [ DataSize (31 bits) | FreeFlag (1 bit) ]
    // [ Checksum (32 bits) ]
    struct Y_PACKED TEntryHeader
    {
        ui32 DataSize = 0;
        ui32 Checksum = 0;
    };

    static_assert(sizeof(TEntryHeader) == sizeof(ui64));

    TData<TEntryHeader, false> Data;

    static constexpr ui32 MaxDataSize = (1U << 31U) - 1U;
    static constexpr ui32 FreeFlagMask = 1U << 31U;

public:
    explicit TFileRingBufferDataProcessorV4(std::span<char> data)
        : Data(data)
    {}

    TFileRingBufferCapabilities GetCapabilities() const override
    {
        return {
            .MaxAllocationByteCount = GetMaxAllocationByteCount(Data.Size()),
            .MaxTag = 0,
        };
    }

    TFileRingBufferEntryHeader ReadEntryHeader(ui64 pos) const override
    {
        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return {};
        }

        return {
            .DataSize = eh->DataSize & MaxDataSize,
            .DataChecksum = eh->Checksum,
            .Tag = 0,
            .FreeFlag = (eh->DataSize & FreeFlagMask) != 0,
        };
    }

    bool WriteEntryHeader(
        ui64 pos,
        const TFileRingBufferEntryHeader& header) override
    {
        if (header.DataSize > MaxDataSize || header.Tag != 0) {
            return false;
        }

        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return false;
        }

        eh->DataSize = header.DataSize | (header.FreeFlag ? FreeFlagMask : 0);
        eh->Checksum = header.DataChecksum;
        return true;
    }

    ui64 GetEntrySize(ui64 dataSize) const override
    {
        return sizeof(TEntryHeader) + dataSize;
    }

    ui64 GetMaxAllocationByteCount(ui64 maxEntrySize) const override
    {
        return Min(
            Data.GetMaxDataSize(maxEntrySize),
            static_cast<ui64>(MaxDataSize));
    }

    const char* GetEntryDataPtr(ui64 pos, ui64 dataSize) const override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }

    char* GetEntryDataPtr(ui64 pos, ui64 dataSize) override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferDataProcessorV5Base
{
protected:
    // Entry header layout (lower bits come first):
    // [ DataSize (28 bits) | Tag (3 bits) | FreeFlag (1 bit) ]
    // [ Checksum (32 bits) ]
    static constexpr ui32 MaxDataSize = (1U << 28U) - 1U;
    static constexpr ui32 MaxTag = (1U << 3U) - 1U;
    static constexpr ui32 TagShift = 28U;
    static constexpr ui32 FreeFlagMask = 1U << 31U;
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferDataProcessorV5
    : public TFileRingBufferDataProcessorV5Base
    , public IFileRingBufferDataProcessor
{
private:
    struct Y_PACKED TEntryHeader
    {
        ui32 DataSize = 0;
        ui32 Checksum = 0;
    };

    static_assert(sizeof(TEntryHeader) == sizeof(ui64));

    TData<TEntryHeader, false> Data;

public:
    explicit TFileRingBufferDataProcessorV5(std::span<char> data)
        : Data(data)
    {}

    TFileRingBufferCapabilities GetCapabilities() const override
    {
        return {
            .MaxAllocationByteCount = GetMaxAllocationByteCount(Data.Size()),
            .MaxTag = MaxTag,
        };
    }

    TFileRingBufferEntryHeader ReadEntryHeader(ui64 pos) const override
    {
        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return {};
        }

        return {
            .DataSize = eh->DataSize & MaxDataSize,
            .DataChecksum = eh->Checksum,
            .Tag = (eh->DataSize >> TagShift) & MaxTag,
            .FreeFlag = (eh->DataSize & FreeFlagMask) != 0,
        };
    }

    bool WriteEntryHeader(
        ui64 pos,
        const TFileRingBufferEntryHeader& header) override
    {
        if (header.DataSize > MaxDataSize || header.Tag > MaxTag) {
            return false;
        }

        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return false;
        }

        eh->DataSize = header.DataSize | (header.Tag << TagShift) |
                       (header.FreeFlag ? FreeFlagMask : 0);
        eh->Checksum = header.DataChecksum;
        return true;
    }

    ui64 GetEntrySize(ui64 dataSize) const override
    {
        return sizeof(TEntryHeader) + dataSize;
    }

    ui64 GetMaxAllocationByteCount(ui64 maxEntrySize) const override
    {
        return Min(
            Data.GetMaxDataSize(maxEntrySize),
            static_cast<ui64>(MaxDataSize));
    }

    const char* GetEntryDataPtr(ui64 pos, ui64 dataSize) const override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }

    char* GetEntryDataPtr(ui64 pos, ui64 dataSize) override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileRingBufferDataProcessorV6
    : public TFileRingBufferDataProcessorV5Base
    , public IFileRingBufferDataProcessor
{
private:
    TData<ui64, true> Data;

public:
    explicit TFileRingBufferDataProcessorV6(std::span<char> data)
        : Data(data)
    {}

    TFileRingBufferCapabilities GetCapabilities() const override
    {
        return {
            .MaxAllocationByteCount = GetMaxAllocationByteCount(Data.Size()),
            .MaxTag = MaxTag,
        };
    }

    TFileRingBufferEntryHeader ReadEntryHeader(ui64 pos) const override
    {
        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return {};
        }

        ui64 value = __atomic_load_n(eh, __ATOMIC_RELAXED);
        ui32 lower = static_cast<ui32>(value);
        ui32 upper = static_cast<ui32>(value >> 32U);

        return {
            .DataSize = lower & MaxDataSize,
            .DataChecksum = upper ^ IntHash(lower),
            .Tag = (lower >> TagShift) & MaxTag,
            .FreeFlag = (lower & FreeFlagMask) != 0,
        };
    }

    bool WriteEntryHeader(
        ui64 pos,
        const TFileRingBufferEntryHeader& header) override
    {
        if (header.DataSize > MaxDataSize || header.Tag > MaxTag) {
            return false;
        }

        auto* eh = Data.GetEntryHeaderPtr(pos);
        if (eh == nullptr) {
            return false;
        }

        ui32 lower = header.DataSize | (header.Tag << TagShift) |
                     (header.FreeFlag ? FreeFlagMask : 0);
        ui32 upper = header.DataChecksum ^ IntHash(lower);
        ui64 value = (static_cast<ui64>(upper) << 32U) | lower;

        __atomic_store_n(eh, value, __ATOMIC_RELAXED);

        return true;
    }

    ui64 GetEntrySize(ui64 dataSize) const override
    {
        return sizeof(ui64) + AlignUp(dataSize, sizeof(ui64));
    }

    ui64 GetMaxAllocationByteCount(ui64 maxEntrySize) const override
    {
        return Min(
            Data.GetMaxDataSize(maxEntrySize),
            static_cast<ui64>(MaxDataSize));
    }

    const char* GetEntryDataPtr(ui64 pos, ui64 dataSize) const override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }

    char* GetEntryDataPtr(ui64 pos, ui64 dataSize) override
    {
        return Data.GetEntryDataPtr(pos, dataSize);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IFileRingBufferDataProcessor> CreateFileRingBufferDataProcessor(
    EFileRingBufferVersion version,
    std::span<char> data)
{
    switch (version) {
        case EFileRingBufferVersion::V4:
            return std::make_unique<TFileRingBufferDataProcessorV4>(data);
        case EFileRingBufferVersion::V5:
            return std::make_unique<TFileRingBufferDataProcessorV5>(data);
        case EFileRingBufferVersion::V6:
            return std::make_unique<TFileRingBufferDataProcessorV6>(data);
        default:
            Y_ABORT_UNLESS(
                false,
                "Unsupported file ring buffer version - %u",
                static_cast<ui32>(version));
    }
}

}   // namespace NCloud
