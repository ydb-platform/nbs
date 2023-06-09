#include "protobuf.h"

#include "protocol.h"

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

size_t TProtoMessageSerializer::MessageByteSize(
    const TProtoMessage& proto,
    size_t dataLen)
{
    size_t protoLen = proto.ByteSizeLong();

    Y_ENSURE(protoLen < RDMA_MAX_PROTO_LEN, "protobuf message is too big");
    Y_ENSURE(dataLen < RDMA_MAX_DATA_LEN, "attached data is too big");

    return NRdma::MessageByteSize(protoLen, dataLen);
}

size_t TProtoMessageSerializer::Serialize(
    TStringBuf buffer,
    ui32 msgId,
    const TProtoMessage& proto,
    TContIOVector data) const
{
    size_t protoLen = proto.ByteSizeLong();
    Y_ENSURE(protoLen < RDMA_MAX_PROTO_LEN, "protobuf message is too big");

    size_t dataLen = data.Bytes();
    Y_ENSURE(dataLen < RDMA_MAX_DATA_LEN, "attached data is too big");

    size_t totalLen = NRdma::MessageByteSize(protoLen, dataLen);
    Y_ENSURE(
        buffer.length() >= totalLen,
        TStringBuilder() << "insufficient buffer length: "
            << buffer.length() << " < " << totalLen);

    TProtoHeader header = {
        .MsgId = msgId,
        .ProtoLen = (ui32)protoLen,
        .DataLen = (ui32)dataLen,
    };

    char* ptr = const_cast<char*>(buffer.data());
    memcpy(ptr, &header, RDMA_PROTO_HEADER_SIZE);
    ptr += RDMA_PROTO_HEADER_SIZE;

    bool succeeded = proto.SerializeToArray(ptr, protoLen);
    Y_ENSURE(succeeded, "could not serialize protobuf message");
    ptr += protoLen;

    for (size_t i = 0; i < data.Count(); ++i) {
        const auto& part = data.Parts()[i];
        memcpy(ptr, part.buf, part.len);
        ptr += part.len;
    }

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

    auto data = TStringBuf(ptr, header.DataLen);
    ptr += header.DataLen;

    return TParseResult { header.MsgId, std::move(proto), data };
}

}   // namespace NCloud::NBlockStore::NRdma
