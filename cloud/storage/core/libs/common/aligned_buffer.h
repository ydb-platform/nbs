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
    static TStringBuf ExtractAlignedData(
        const TString& buffer,
        ui32 align);

    TAlignedBuffer();
    TAlignedBuffer(const TAlignedBuffer&) = delete;
    TAlignedBuffer& operator=(const TAlignedBuffer&) = delete;

    TAlignedBuffer(TAlignedBuffer&& other);
    TAlignedBuffer& operator=(TAlignedBuffer&& other);

    TAlignedBuffer(ui32 size, ui32 align);
    TAlignedBuffer(TString&& buffer, ui32 align);

    size_t AlignedDataOffset() const;

    char* Begin();
    const char* Begin() const;

    char* End();
    const char* End() const;

    size_t Size() const;
    void TrimSize(size_t size);

    TString& AccessBuffer();
};
}   // namespace NCloud
