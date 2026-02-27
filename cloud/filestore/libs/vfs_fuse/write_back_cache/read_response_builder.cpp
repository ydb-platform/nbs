#include "read_response_builder.h"

#include <util/stream/mem.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct IResponseWriter
{
    virtual ~IResponseWriter() = default;

    virtual void TakeBytesFromOriginalResponse(ui64 offset, ui64 byteCount) = 0;
    virtual void TakeBytesFromCache(TStringBuf data) = 0;
    virtual void WriteZeroBytes(ui64 byteCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TExtendedMemoryOutput: public TMemoryOutput
{
public:
    using TMemoryOutput::TMemoryOutput;

    void Skip(ui64 byteCount)
    {
        Y_ABORT_UNLESS(byteCount <= Avail());
        Buf_ += byteCount;
    }

    void WriteZeroBytes(ui64 byteCount)
    {
        Y_ABORT_UNLESS(byteCount <= Avail());
        memset(Buf_, 0, byteCount);
        Buf_ += byteCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBufferWriterBase
    : protected TExtendedMemoryOutput
    , public IResponseWriter
{
protected:
    explicit TBufferWriterBase(char* buf, size_t len)
        : TExtendedMemoryOutput(buf, len)
    {}

public:
    void TakeBytesFromCache(TStringBuf data) override
    {
        Write(data);
    }

    void WriteZeroBytes(ui64 byteCount) override
    {
        TExtendedMemoryOutput::WriteZeroBytes(byteCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInPlaceBufferWriter: public TBufferWriterBase
{
public:
    explicit TInPlaceBufferWriter(char* buf, size_t len)
        : TBufferWriterBase(buf, len)
    {}

    void TakeBytesFromOriginalResponse(ui64 offset, ui64 byteCount) override
    {
        Y_UNUSED(offset);
        Skip(byteCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBufferWriter: public TBufferWriterBase
{
private:
    TStringBuf OriginalResponse;

public:
    TBufferWriter(TStringBuf originalResponse, TString& buffer)
        : TBufferWriterBase(buffer.begin(), buffer.size())
        , OriginalResponse(originalResponse)
    {}

    void TakeBytesFromOriginalResponse(ui64 offset, ui64 byteCount) override
    {
        Y_ABORT_UNLESS(offset < OriginalResponse.size());
        Y_ABORT_UNLESS(byteCount <= OriginalResponse.size() - offset);
        Write(OriginalResponse.SubStr(offset, byteCount));
    }

    bool Exhausted() const
    {
        return TMemoryOutput::Exhausted();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInPlaceIovecWriter: public IResponseWriter
{
private:
    using TIovecVector = ::google::protobuf::RepeatedPtrField<NProto::TIovec>;

    TExtendedMemoryOutput CurrentIovecWriter;
    TIovecVector::const_iterator CurrentIovec;
    TIovecVector::const_iterator EndIovec;

public:
    explicit TInPlaceIovecWriter(const TIovecVector& iovecs)
        : CurrentIovecWriter(nullptr, 0)
        , CurrentIovec(iovecs.begin())
        , EndIovec(iovecs.end())
    {}

    void TakeBytesFromOriginalResponse(ui64 offset, ui64 byteCount) override
    {
        // Skip byteCount bytes
        Y_UNUSED(offset);
        while (byteCount > 0) {
            const ui64 len = Min(byteCount, PrepareWriteAndGetAvail());
            Y_ABORT_UNLESS(len > 0);
            CurrentIovecWriter.Skip(len);
            byteCount -= len;
        }
    }

    void TakeBytesFromCache(TStringBuf data) override
    {
        while (!data.empty()) {
            const ui64 len = Min(data.size(), PrepareWriteAndGetAvail());
            Y_ABORT_UNLESS(len > 0);
            CurrentIovecWriter.Write(data.Head(len));
            data.Skip(len);
        }
    }

    void WriteZeroBytes(ui64 byteCount) override
    {
        while (byteCount > 0) {
            const ui64 len = Min(byteCount, PrepareWriteAndGetAvail());
            Y_ABORT_UNLESS(len > 0);
            CurrentIovecWriter.WriteZeroBytes(len);
            byteCount -= len;
        }
    }

private:
    ui64 PrepareWriteAndGetAvail()
    {
        PrepareWrite();
        return static_cast<ui64>(CurrentIovecWriter.Avail());
    }

    void PrepareWrite()
    {
        if (!CurrentIovecWriter.Exhausted()) {
            return;
        }

        Y_ABORT_UNLESS(
            CurrentIovec != EndIovec,
            "No more iovecs left to write");

        CurrentIovecWriter = TExtendedMemoryOutput(
            reinterpret_cast<char*>(CurrentIovec->GetBase()),
            CurrentIovec->GetLength());

        CurrentIovec++;

        Y_ABORT_UNLESS(
            !CurrentIovecWriter.Exhausted(),
            "Iovecs with zero length are not allowed");
    }
};

////////////////////////////////////////////////////////////////////////////////

void WriteResponse(
    ui64 originalResponseLength,
    const TCachedData& cachedData,
    IResponseWriter& writer)
{
    ui64 ofs = 0;
    for (const auto& part: cachedData.Parts) {
        if (ofs < part.RelativeOffset) {
            if (originalResponseLength <= ofs) {
                writer.WriteZeroBytes(part.RelativeOffset - ofs);
            } else if (part.RelativeOffset <= originalResponseLength) {
                writer.TakeBytesFromOriginalResponse(
                    ofs,
                    part.RelativeOffset - ofs);
            } else {
                // offset < actualLength < part.RelativeOffset
                writer.TakeBytesFromOriginalResponse(
                    ofs,
                    originalResponseLength - ofs);
                writer.WriteZeroBytes(
                    part.RelativeOffset - originalResponseLength);
            }
        }
        writer.TakeBytesFromCache(part.Data);
        ofs = part.RelativeOffset + part.Data.size();
    }

    if (ofs < originalResponseLength) {
        writer.TakeBytesFromOriginalResponse(ofs, originalResponseLength - ofs);
        ofs = originalResponseLength;
    }
    if (ofs < cachedData.ReadDataByteCount) {
        writer.WriteZeroBytes(cachedData.ReadDataByteCount - ofs);
    }
}

void Validate(const TCachedData& cachedData, ui64 requestedLength)
{
    Y_ABORT_UNLESS(
        cachedData.ReadDataByteCount <= requestedLength,
        "Cached data byte count %lu exceeds requested length %lu",
        cachedData.ReadDataByteCount,
        requestedLength);

    if (cachedData.Parts.empty()) {
        return;
    }

    // All parts should be ordered, non-empty, non-overlapping and lie in
    // [0, cachedData.ReadDataByteCount) range

    for (const auto& part: cachedData.Parts) {
        Y_ABORT_UNLESS(
            !part.Data.empty(),
            "Empty cached data parts are not allowed");

        Y_ABORT_UNLESS(
            part.RelativeOffset < cachedData.ReadDataByteCount &&
                part.Data.size() <=
                    cachedData.ReadDataByteCount - part.RelativeOffset,
            "Cached data part [%lu, %lu) lie outside [0, %lu)",
            part.RelativeOffset,
            part.RelativeOffset + part.Data.size(),
            cachedData.ReadDataByteCount);
    }

    for (size_t i = 1; i < cachedData.Parts.size(); i++) {
        const auto& prev = cachedData.Parts[i - 1];
        const auto& next = cachedData.Parts[i];

        Y_ABORT_UNLESS(
            prev.RelativeOffset < next.RelativeOffset,
            "Cached data parts are not ordered");

        Y_ABORT_UNLESS(
            prev.Data.size() <= next.RelativeOffset - prev.RelativeOffset,
            "Cached data parts overlap");
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TReadResponseBuilder::TReadResponseBuilder(
    const NProto::TReadDataRequest& request,
    const TWriteBackCacheState& state)
    : Request(request)
    , CachedData(state.GetCachedData(
          request.GetNodeId(),
          request.GetOffset(),
          request.GetLength()))
{
    Validate(CachedData, request.GetLength());

    for (const auto& part: CachedData.Parts) {
        if (part.RelativeOffset != ContiguousCachedDataByteCount) {
            break;
        }
        ContiguousCachedDataByteCount += part.Data.size();
    }
}

bool TReadResponseBuilder::HasCachedData() const
{
    return !CachedData.Parts.empty();
}

bool TReadResponseBuilder::CanFullyServeFromCache() const
{
    return ContiguousCachedDataByteCount == Request.GetLength();
}

NProto::TReadDataResponse TReadResponseBuilder::FullyServeFromCache() const
{
    Y_ABORT_UNLESS(CanFullyServeFromCache(), "Cannot fully serve from cache");

    NProto::TReadDataResponse response;
    AugmentResponseWithCachedData(response);
    return response;
}

void TReadResponseBuilder::AugmentResponseWithCachedData(
    NProto::TReadDataResponse& response) const
{
    // The backend may ignore iovecs in the request and respond with a buffer
    const bool useIovecs =
        response.GetBuffer().empty() && !Request.GetIovecs().empty();

    if (useIovecs) {
        auto writer = TInPlaceIovecWriter(Request.GetIovecs());
        WriteResponse(response.GetLength(), CachedData, writer);
        response.SetLength(
            Max(response.GetLength(), CachedData.ReadDataByteCount));
        return;
    }

    const ui64 originalResponseLength =
        response.GetBuffer().size() - response.GetBufferOffset();

    if (CachedData.ReadDataByteCount <= originalResponseLength) {
        // No need to reallocate buffer - just write cached data parts on top of
        // the existing buffer
        auto writer = TInPlaceBufferWriter(
            response.MutableBuffer()->begin() + response.GetBufferOffset(),
            originalResponseLength);

        WriteResponse(originalResponseLength, CachedData, writer);
    } else {
        // We need to reallocate buffer and merge response data with cached
        // data parts. Also we need to ensure that the client does not receive
        // uninitialized data.
        auto originalBuffer =
            TStringBuf(response.GetBuffer()).Skip(response.GetBufferOffset());

        auto newBuffer = TString::Uninitialized(CachedData.ReadDataByteCount);

        auto writer = TBufferWriter(originalBuffer, newBuffer);
        WriteResponse(originalResponseLength, CachedData, writer);
        Y_ABORT_UNLESS(writer.Exhausted());

        response.SetBuffer(std::move(newBuffer));
        response.SetBufferOffset(0);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
