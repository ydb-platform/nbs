#include "protobuf.h"

#include "protocol.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/common/helpers.h>

#include <google/protobuf/message.h>

#include <util/stream/printf.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                                         \
    if (Y_UNLIKELY(!(expr))) {                                                 \
        return MakeError(E_ARGUMENT, TStringBuilder() << message);             \
    }                                                                          \
// Y_ENSURE_RETURN

static inline size_t MessageByteSize(size_t protoLen, size_t dataLen)
{
    return protoLen + dataLen + RDMA_PROTO_HEADER_SIZE;
}

////////////////////////////////////////////////////////////////////////////////

void TProtoMessageSerializer::RegisterProto(ui32 msgId, IProtoFactoryPtr factory)
{
    auto [it, inserted] = Messages.emplace(msgId, std::move(factory));
    Y_ENSURE(inserted, "could not register protobuf message");
}

TProtoMessagePtr TProtoMessageSerializer::CreateProto(ui32 msgId) const
{
    if (const auto* factory = Messages.FindPtr(msgId)) {
        return (*factory)->Create();
    }
    return nullptr;
}

// static
size_t TProtoMessageSerializer::MessageByteSize(
    const TProtoMessage& proto,
    size_t dataLen)
{
    size_t protoLen = proto.ByteSizeLong();

    Y_ENSURE(protoLen < RDMA_MAX_PROTO_LEN, "protobuf message is too big");
    Y_ENSURE(dataLen < RDMA_MAX_DATA_LEN, "attached data is too big");

    return NRdma::MessageByteSize(protoLen, dataLen);
}

// static
size_t TProtoMessageSerializer::Serialize(
    TStringBuf buffer,
    ui32 msgId,
    ui32 flags,
    const TProtoMessage& proto)
{
    return SerializeWithDataLength(buffer, msgId, flags, proto, 0);
}

// static
size_t TProtoMessageSerializer::SerializeWithData(
    TStringBuf buffer,
    ui32 msgId,
    ui32 flags,
    const TProtoMessage& proto,
    TBlockDataRefSpan data)
{
    const size_t dataLen = Accumulate(
        data,
        size_t{},
        [](size_t acc, TBlockDataRef dataRef) { return acc + dataRef.Size(); });

    char* ptr = const_cast<char*>(buffer.data());
    ptr += SerializeWithDataLength(buffer, msgId, flags, proto, dataLen);

    if (HasProtoFlag(flags, RDMA_PROTO_FLAG_DATA_AT_THE_END)) {
        ptr = const_cast<char*>(buffer.data()) + buffer.length() - dataLen;
    }

    for (const auto part : data) {
        if (part.Data()) {
            memcpy(ptr, part.Data(), part.Size());
        } else {
            memset(ptr, 0, part.Size());
        }
        ptr += part.Size();
    }

    return ptr - buffer.data();
}

// static
size_t TProtoMessageSerializer::SerializeWithDataLength(
    TStringBuf buffer,
    ui32 msgId,
    ui32 flags,
    const TProtoMessage& proto,
    size_t dataLen)
{
    size_t protoLen = proto.ByteSizeLong();
    Y_ENSURE(protoLen < RDMA_MAX_PROTO_LEN, "protobuf message is too big");

    Y_ENSURE(dataLen < RDMA_MAX_DATA_LEN, "attached data is too big");

    size_t totalLen = NRdma::MessageByteSize(protoLen, dataLen);
    Y_ENSURE(
        buffer.length() >= totalLen,
        TStringBuilder() << "insufficient buffer length: "
            << buffer.length() << " < " << totalLen);

    TProtoHeader header = {
        .MsgId = msgId,
        .Flags = flags,
        .ProtoLen = (ui32)protoLen,
        .DataLen = (ui32)dataLen,
    };

    char* ptr = const_cast<char*>(buffer.data());
    memcpy(ptr, &header, RDMA_PROTO_HEADER_SIZE);
    ptr += RDMA_PROTO_HEADER_SIZE;

    bool succeeded = proto.SerializeToArray(ptr, protoLen);
    Y_ENSURE(succeeded, "could not serialize protobuf message");
    ptr += protoLen;

    return ptr - buffer.data();
}

TResultOrError<TProtoMessageSerializer::TParseResult>
TProtoMessageSerializer::Parse(TStringBuf buffer) const
{
    Y_ENSURE_RETURN(
        buffer.length() >= RDMA_PROTO_HEADER_SIZE,
        "invalid buffer length: " << buffer.length());

    TProtoHeader header;

    const char* ptr = buffer.data();
    memcpy(&header, ptr, RDMA_PROTO_HEADER_SIZE);
    ptr += RDMA_PROTO_HEADER_SIZE;

    size_t totalLen = NRdma::MessageByteSize(header.ProtoLen, header.DataLen);
    Y_ENSURE_RETURN(
        buffer.length() >= totalLen,
        "invalid buffer length: " << buffer.length()
        << " (expected: " << totalLen << ")");

    auto proto = CreateProto(header.MsgId);
    Y_ENSURE_RETURN(proto, "unknown protobuf message: " << header.MsgId);

    bool succeeded = proto->ParseFromArray(ptr, header.ProtoLen);
    Y_ENSURE_RETURN(succeeded, "could not parse protobuf message");
    ptr += header.ProtoLen;

    if (HasProtoFlag(header.Flags, RDMA_PROTO_FLAG_DATA_AT_THE_END)) {
        ptr =
            const_cast<char*>(buffer.data()) + buffer.length() - header.DataLen;
    }

    auto data = TStringBuf(ptr, header.DataLen);
    ptr += header.DataLen;

    return TParseResult { header.MsgId, header.Flags, std::move(proto), data };
}

////////////////////////////////////////////////////////////////////////////////

size_t SerializeError(ui32 code, TStringBuf message, TStringBuf buffer)
{
    auto error = MakeError(code, TString(message));

    if (error.ByteSizeLong() > buffer.size()) {
        error.SetMessage("rdma error");
        ReportFailedToSerializeRdmaError(
            TStringBuilder()
            << "Failed to serialize RDMA error with code " << code
            << ", original message: \"" << message << "\"");
    }

    if (error.SerializeToArray(
        const_cast<char*>(buffer.data()),
        error.ByteSize()))
    {
        return error.ByteSizeLong();
    }

    return 0;   // will be interpreted as E_FAIL by ParseError
}

NProto::TError ParseError(TStringBuf buffer)
{
    NProto::TError error;

    if (!error.ParseFromArray(buffer.data(), buffer.size())) {
        error.SetCode(E_FAIL);
        error.SetMessage("rdma error");
        ReportFailedToParseRdmaError(
            TStringBuilder()
            << "Failed to parse RDMA error from buffer " << buffer.size()
            << "; error: " << FormatError(error));
    }

    return error;
}

}   // namespace NCloud::NBlockStore::NRdma
