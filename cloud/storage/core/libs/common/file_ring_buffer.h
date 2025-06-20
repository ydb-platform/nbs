#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

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

    using TVisitor = std::function<void(ui32 checksum, TStringBuf entry)>;
    using TBufferWriter = std::function<void(TStringBuf buffer)>;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFileRingBuffer(const TString& filePath, ui64 capacity);
    ~TFileRingBuffer();

public:
    bool PushBack(TStringBuf data);
    char* AllocateBack(size_t size);
    void CompleteAllocation(char* ptr);
    TStringBuf Front() const;
    TStringBuf Back() const;
    void PopFront();
    ui64 MaximalEntrySize() const;
    ui64 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate() const;
    void Visit(const TVisitor& visitor) const;
};

}   // namespace NCloud
