#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/string.h>
#include <util/system/align.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TAlignedBuffer
{
private:
    TString Buffer;
    const char* AlignedData;

public:
    static std::pair<const char*, size_t> ExtractAlignedData(const TString& buffer, ui32 align)
    {
        auto alignedData = buffer.begin();
        if (align) {
            alignedData = AlignUp(buffer.data(), align);
            if (alignedData > buffer.end()) {
                ythrow TServiceError(E_ARGUMENT)
                    << "Extracting unaligned buffer " << (void*)buffer.begin()
                    << " with size " << buffer.size();
            }
        }

        return {alignedData, buffer.end() - alignedData};
    }

    TAlignedBuffer()
        : AlignedData(Buffer.begin())
    {
    }

    TAlignedBuffer(const TAlignedBuffer&) = delete;
    TAlignedBuffer& operator = (const TAlignedBuffer&) = delete;

    TAlignedBuffer(TAlignedBuffer&&) = default;
    TAlignedBuffer& operator = (TAlignedBuffer&&) = default;

    TAlignedBuffer(ui32 size, ui32 align)
        : Buffer(TString::Uninitialized(size + align))
        , AlignedData(Buffer.begin())
    {
        if (align) {
            Y_ASSERT(IsPowerOf2(align));   // align should be power of 2
            AlignedData = AlignUp(Buffer.data(), align);
            Buffer.resize(AlignedData + size - Buffer.begin());
        }
    }

    TAlignedBuffer(TString&& buffer, ui32 align)
        : Buffer(std::move(buffer))
        , AlignedData(Buffer.begin())
    {
        if (align) {
            Y_ASSERT(IsPowerOf2(align));   // align should be power of 2
            AlignedData = AlignUp(Buffer.data(), align);
            if (AlignedData > Buffer.end()) {
                ythrow TServiceError(E_ARGUMENT)
                    << "Initializing from unaligned buffer "
                    << (void*)Buffer.begin()
                    << " with size " << Buffer.size();
            }
        }
    }

    size_t AlignedDataOffset() const
    {
        if (!Buffer) {
            return 0;
        }

        return AlignedData - Buffer.begin();
    }

    const char* Begin() const
    {
        if (!Buffer) {
            return Buffer.begin();
        }

        return AlignedData;
    }

    const char* End() const
    {
        if (!Buffer) {
            return Buffer.end();
        }

        return Buffer.end();
    }

    size_t Size() const
    {
        return End() - Begin();
    }

    void TrimSize(size_t size)
    {
        if (size > Size()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Tried to trim to size " << size << " > " << Size();
        }
        Buffer.resize(AlignedDataOffset() + size);
    }

    TString& GetBuffer()
    {
        return Buffer;
    }

};

}   // namespace NCloud
