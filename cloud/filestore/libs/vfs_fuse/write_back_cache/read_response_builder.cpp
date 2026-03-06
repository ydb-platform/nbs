#include "read_response_builder.h"

#include <cloud/storage/core/libs/common/byte_range.h>

#include <util/stream/mem.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Sequentially applies cached data on top of the response returned from backend
// (named OriginalResponse)
struct IResponseWriter
{
    virtual ~IResponseWriter() = default;

    // Copy data from the response returned from backend from the same position
    // Skip bytes if the response is being constructed in-place
    virtual void TakeBytesFromOriginalResponse(ui64 byteCount) = 0;

    // Copy cached data from the write-back cache
    virtual void TakeBytesFromCache(TStringBuf data) = 0;

    // Write zero bytes - used when response returned from backend is shorter
    // than when taking into account unflushed data
    virtual void WriteZeroBytes(ui64 byteCount) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TExtendedMemoryOutput: public TMemoryWriteBuffer
{
public:
    TExtendedMemoryOutput(void* buf, size_t len)
        : TMemoryWriteBuffer(buf, len)
    {}

    void Skip(ui64 byteCount)
    {
        Y_ABORT_UNLESS(byteCount <= Avail());
        SetPos(Len() + byteCount);
    }

    void WriteZeroBytes(ui64 byteCount)
    {
        Y_ABORT_UNLESS(byteCount <= Avail());
        memset(Buf(), 0, byteCount);
        SetPos(Len() + byteCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TInPlaceBufferWriter: public IResponseWriter
{
private:
    TExtendedMemoryOutput Out;

public:
    explicit TInPlaceBufferWriter(char* buf, size_t len)
        : Out(buf, len)
    {}

    void TakeBytesFromOriginalResponse(ui64 byteCount) override
    {
        Out.Skip(byteCount);
    }

    void TakeBytesFromCache(TStringBuf data) override
    {
        Out.Write(data);
    }

    void WriteZeroBytes(ui64 byteCount) override
    {
        Out.WriteZeroBytes(byteCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBufferWriter: public IResponseWriter
{
private:
    TExtendedMemoryOutput Out;
    TStringBuf OriginalResponseBuffer;

public:
    TBufferWriter(TStringBuf originalResponseBuffer, TString& buffer)
        : Out(buffer.begin(), buffer.size())
        , OriginalResponseBuffer(originalResponseBuffer)
    {}

    void TakeBytesFromOriginalResponse(ui64 byteCount) override
    {
        const ui64 offset = Out.Len();
        Y_ABORT_UNLESS(offset < OriginalResponseBuffer.size());
        Y_ABORT_UNLESS(byteCount <= OriginalResponseBuffer.size() - offset);
        Out.Write(OriginalResponseBuffer.SubStr(offset, byteCount));
    }

    void TakeBytesFromCache(TStringBuf data) override
    {
        Out.Write(data);
    }

    void WriteZeroBytes(ui64 byteCount) override
    {
        Out.WriteZeroBytes(byteCount);
    }

    bool Exhausted() const
    {
        return Out.Exhausted();
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

    void TakeBytesFromOriginalResponse(ui64 byteCount) override
    {
        // Skip byteCount bytes
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

// Wrapper over IResponseWriter that combines TakeBytesFromOriginalResponse and
// TakeBytesFromCache into TakeNonCachedDataUpToOffset - it takes bytes from
// original response until it is exhaused and then writes zeroes
class TResponseWriterWrapper
{
private:
    const ui64 OriginalResponseLength;
    IResponseWriter& Writer;
    ui64 Offset = 0;

public:
    TResponseWriterWrapper(ui64 originalResponseLength, IResponseWriter& writer)
        : OriginalResponseLength(originalResponseLength)
        , Writer(writer)
    {}

    void TakeCachedData(TStringBuf data)
    {
        Writer.TakeBytesFromCache(data);
        Offset += data.size();
    }

    void TakeNonCachedDataUpToOffset(ui64 newOffset)
    {
        Y_ABORT_UNLESS(Offset <= newOffset);

        // Take bytes from original response if it is not exhaused
        const ui64 ofs = Min(OriginalResponseLength, newOffset);
        if (Offset < ofs) {
            Writer.TakeBytesFromOriginalResponse(ofs - Offset);
            Offset = ofs;
        }

        // Zero fill the remaining bytes
        if (Offset < newOffset) {
            Writer.WriteZeroBytes(newOffset - Offset);
            Offset = newOffset;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TByteRange ToRange(const TCachedDataPart& part)
{
    return {part.RelativeOffset, part.Data.size(), 1};
}

// originalResponseLength is the length of the response returned from backend
// (without cached data parts)
ui64 WriteResponse(
    ui64 originalResponseLength,
    const TCachedData& cachedData,
    IResponseWriter& writer)
{
    // Single-pass algorithm that minimizes data copying: no data is written
    // twice, only the necessary data is copied, everything is written
    // sequentially.

    TResponseWriterWrapper responseWriter(originalResponseLength, writer);

    for (const auto& part: cachedData.Parts) {
        responseWriter.TakeNonCachedDataUpToOffset(part.RelativeOffset);
        responseWriter.TakeCachedData(part.Data);
    }

    const ui64 expectedResponseLength =
        Max(originalResponseLength, cachedData.ReadDataByteCount);

    responseWriter.TakeNonCachedDataUpToOffset(expectedResponseLength);

    return expectedResponseLength;
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

    const TByteRange validRange(0, cachedData.ReadDataByteCount, 1);

    for (const auto& part: cachedData.Parts) {
        Y_ABORT_UNLESS(
            !part.Data.empty(),
            "Empty cached data parts are not allowed");

        const auto range = ToRange(part);

        Y_ABORT_UNLESS(
            validRange.Contains(range),
            "Cached data part %s lies outside %s",
            range.Describe().c_str(),
            validRange.Describe().c_str());
    }

    for (size_t i = 1; i < cachedData.Parts.size(); i++) {
        const auto prev = ToRange(cachedData.Parts[i - 1]);
        const auto next = ToRange(cachedData.Parts[i]);

        Y_ABORT_UNLESS(
            prev.End() <= next.Offset,
            "Cached data parts %s and %s are not ordered",
            prev.Describe().c_str(),
            next.Describe().c_str());

        Y_ABORT_UNLESS(
            !prev.Overlaps(next),
            "Cached data parts %s and %s overlap",
            prev.Describe().c_str(),
            next.Describe().c_str());
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

std::optional<NProto::TReadDataResponse>
TReadResponseBuilder::TryFullyServeFromCache() const
{
    if (ContiguousCachedDataByteCount != Request.GetLength()) {
        return {};
    }

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
        ui64 len = WriteResponse(response.GetLength(), CachedData, writer);
        response.SetLength(len);
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

        response.SetLength(newBuffer.size());
        response.SetBuffer(std::move(newBuffer));
        response.SetBufferOffset(0);
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
