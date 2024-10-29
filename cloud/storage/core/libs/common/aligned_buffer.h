#pragma once

#include <util/generic/string.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TAlignedBuffer
{
private:
    TString Buffer;
    const char* AlignedData;

public:
    TAlignedBuffer();
    TAlignedBuffer(const TAlignedBuffer&) = delete;
    TAlignedBuffer& operator=(const TAlignedBuffer&) = delete;

    TAlignedBuffer(TAlignedBuffer&& other) noexcept ;
    TAlignedBuffer& operator=(TAlignedBuffer&& other) noexcept ;

    TAlignedBuffer(ui32 size, ui32 align);
    TAlignedBuffer(TString&& buffer, ui32 align);

    size_t AlignedDataOffset() const;

    char* Begin();
    const char* Begin() const;

    char* UnalignedBegin();
    const char* UnalignedBegin() const;

    char* End();
    const char* End() const;

    size_t Size() const;
    void TrimSize(size_t size);

    // Take ownership of the buffer. Don't use aligned buffer after calling this method.
    TString TakeBuffer();
};
}   // namespace NCloud
