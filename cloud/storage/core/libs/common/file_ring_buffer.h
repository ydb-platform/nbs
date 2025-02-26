#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer
{
public:
    struct TBrokenFileEntry
    {
        TString Data;
        ui32 ExpectedChecksum = 0;
        ui32 ActualChecksum = 0;
    };

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFileRingBuffer(const TString& filePath, ui32 capacity);
    ~TFileRingBuffer();

public:
    bool Push(TStringBuf data);
    TStringBuf Front() const;
    void Pop();
    ui32 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate() const;
};

}   // namespace NCloud
