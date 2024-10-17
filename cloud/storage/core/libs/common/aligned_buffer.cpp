#include "aligned_buffer.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/system/align.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TStringBuf TAlignedBuffer::ExtractAlignedData(
    const TString& buffer,
    ui32 align)
{
    auto alignedData = buffer.begin();
    if (align) {
        alignedData = AlignUp(buffer.data(), align);
        if (alignedData > buffer.end()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Extracting unaligned buffer " << (void*)buffer.begin()
                << " with alignment " << align
                << " with size " << buffer.size();
        }
    }

    return TStringBuf(alignedData, buffer.end() - alignedData);
}

TAlignedBuffer::TAlignedBuffer()
    : AlignedData(Buffer.begin())
{}

TAlignedBuffer::TAlignedBuffer(TAlignedBuffer&& other)
    : Buffer(std::move(other.Buffer))
    , AlignedData(std::move(other.AlignedData))
{
    other.Buffer.clear();
    other.AlignedData = other.Buffer.begin();
}

TAlignedBuffer& TAlignedBuffer::operator=(TAlignedBuffer&& other)
{
    Buffer = std::move(other.Buffer);
    AlignedData = std::move(other.AlignedData);
    other.Buffer.clear();
    other.AlignedData = other.Buffer.begin();
    return *this;
}

TAlignedBuffer::TAlignedBuffer(ui32 size, ui32 align)
    : Buffer(TString::Uninitialized(size + align))
    , AlignedData(Buffer.begin())
{
    if (align) {
        Y_DEBUG_ABORT_UNLESS(IsPowerOf2(align));   // align should be power of 2
        AlignedData = AlignUp(Buffer.data(), align);
        Buffer.resize(AlignedData + size - Buffer.begin());
    }
}

TAlignedBuffer::TAlignedBuffer(TString&& buffer, ui32 align)
    : Buffer(std::move(buffer))
    , AlignedData(Buffer.begin())
{
    if (align) {
        Y_DEBUG_ABORT_UNLESS(IsPowerOf2(align));   // align should be power of 2
        AlignedData = AlignUp(Buffer.data(), align);
        if (AlignedData > Buffer.end()) {
            ythrow TServiceError(E_ARGUMENT)
                << "Initializing from unaligned buffer "
                << (void*)Buffer.begin()
                << " with alignment " << align
                << " with size " << Buffer.size();
        }
    }
}

size_t TAlignedBuffer::AlignedDataOffset() const
{
    return AlignedData - Buffer.begin();
}

char* TAlignedBuffer::Begin()
{
    return const_cast<char*>(AlignedData);
}

const char* TAlignedBuffer::Begin() const
{
    return AlignedData;
}

char* TAlignedBuffer::End()
{
    return const_cast<char*>(Buffer.end());
}

const char* TAlignedBuffer::End() const
{
    return Buffer.end();
}

size_t TAlignedBuffer::Size() const
{
    return End() - Begin();
}

void TAlignedBuffer::TrimSize(size_t size)
{
    if (size > Size()) {
        ythrow TServiceError(E_ARGUMENT)
            << "Tried to trim to size " << size << " > " << Size();
    }
    Buffer.resize(AlignedDataOffset() + size);
}

TString& TAlignedBuffer::AccessBuffer()
{
    return Buffer;
}

}   // namespace NCloud
