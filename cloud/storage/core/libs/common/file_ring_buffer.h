#pragma once

#include "error.h"

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

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TFileRingBuffer();
    TFileRingBuffer(TFileRingBuffer&&) noexcept;
    ~TFileRingBuffer();

    NProto::TError Init(const TString& filePath, ui64 capacity);

    explicit operator bool() const
    {
        return !!Impl;
    }

public:
    bool PushBack(TStringBuf data);
    TStringBuf Front() const;
    TStringBuf Back() const;
    void PopFront();
    ui64 Size() const;
    bool Empty() const;
    TVector<TBrokenFileEntry> Validate();
    void Visit(const TVisitor& visitor);
    bool IsCorrupted() const;
};

}   // namespace NCloud
