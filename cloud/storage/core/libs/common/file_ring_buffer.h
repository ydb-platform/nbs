#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <functional>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TFileRingBuffer
{
public:
    struct TAllocationHandle;

    struct TBrokenFileEntry
    {
        TString Data;
        ui32 ExpectedChecksum = 0;
        ui32 ActualChecksum = 0;
    };

    using TVisitor = std::function<void(ui32 checksum, TStringBuf entry)>;
    using TAllocationWriter = std::function<void(char* data, size_t size)>;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFileRingBuffer(const TString& filePath, ui64 capacity);
    ~TFileRingBuffer();

public:
    bool PushBack(TStringBuf data);
    ui64 MaxAllocationSize() const;
    bool AllocateBack(size_t size, TAllocationHandle* allocation);
    void Write(TAllocationHandle allocation, const TAllocationWriter& writer);
    void CommitAllocation(TAllocationHandle allocation);
    TStringBuf Front() const;
    TStringBuf Back() const;
    void PopFront();
    ui64 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate();
    void Visit(const TVisitor& visitor);
    bool IsCorrupted() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TFileRingBuffer::TAllocationHandle
{
private:
    friend class TImpl;
    static constexpr ui64 InvalidHandle = Max<ui64>();
    ui64 Handle = InvalidHandle;

public:
    explicit operator bool() const
    {
        return Handle != InvalidHandle;
    }
};

}   // namespace NCloud
