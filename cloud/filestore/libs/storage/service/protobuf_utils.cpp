#include "protobuf_utils.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore {

NCloud::NProto::TError ParseReadDataResponse(
    NActors::TEventSerializedData& buffer,
    NProto::TReadDataResponse& response,
    const ::google::protobuf::RepeatedPtrField<
        ::NCloud::NFileStore::NProto::TIovec>& iovecs)
{
    using namespace google::protobuf::internal;

    TRope::TConstIterator iter = buffer.GetBeginIter();
    ui64 size = buffer.GetSize();
    NActors::TRopeStream stream(iter, size);
    google::protobuf::io::CodedInputStream input(&stream);

    input.PushLimit(size);

    while (true) {
        auto tag = input.ReadTag();

        if (tag == 0) {
            // end of message
            break;
        }

        int field = WireFormatLite::GetTagFieldNumber(tag);
        auto wire = WireFormatLite::GetTagWireType(tag);

        switch (field) {
            case NProto::TReadDataResponse::kErrorFieldNumber: {
                if (wire != WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                    return MakeError(
                        E_FAIL,
                        "Invalid wire type "
                        "for error field in ReadData response: %d",
                        wire);
                }

                ui32 len = 0;
                if (!input.ReadVarint32(&len)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to read length for error field in ReadData "
                        "response");
                }

                auto oldLimit = input.PushLimit(len);
                if (!response.MutableError()->ParseFromCodedStream(&input)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to parse error field in ReadData response");
                }
                if (!input.ConsumedEntireMessage()) {
                    return MakeError(
                        E_FAIL,
                        "Failed to consume entire error message in ReadData "
                        "response");
                }
                input.PopLimit(oldLimit);

                break;
            }

            case NProto::TReadDataResponse::kBufferFieldNumber: {
                if (wire != WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                    return MakeError(
                        E_FAIL,
                        "Invalid wire type "
                        "for buffer field in ReadData response: %d",
                        wire);
                }

                ui32 len = 0;
                if (!input.ReadVarint32(&len)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to read length for buffer field in ReadData "
                        "response");
                }

                ui64 currentOffset = 0;
                ui64 bufferSize = len;

                for (const auto& iovec: iovecs) {
                    auto dataToWrite = Min(iovec.GetLength(), bufferSize);

                    if (dataToWrite > 0) {
                        char* targetData =
                            reinterpret_cast<char*>(iovec.GetBase());
                        if (!input.ReadRaw(targetData, dataToWrite)) {
                            return MakeError(
                                E_FAIL,
                                "Failed to read buffer data in ReadData "
                                "response");
                        }
                        bufferSize -= dataToWrite;
                        currentOffset += dataToWrite;
                    }
                }
                response.SetLength(currentOffset);

                if (currentOffset < len) {
                    return MakeError(
                        E_FAIL,
                        "Failed to consume entire buffer in ReadData response");
                }
                break;
            }

            case NProto::TReadDataResponse::kHeadersFieldNumber: {
                if (wire != WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
                    return MakeError(
                        E_FAIL,
                        "Invalid wire type "
                        "for headers field in ReadData response: %d",
                        wire);
                }

                ui32 len = 0;
                if (!input.ReadVarint32(&len)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to read length for headers field in ReadData "
                        "response");
                }

                auto oldLimit = input.PushLimit(len);
                if (!response.MutableHeaders()->ParseFromCodedStream(&input)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to parse headers field in ReadData response");
                }
                if (!input.ConsumedEntireMessage()) {
                    return MakeError(
                        E_FAIL,
                        "Failed to consume entire headers message in ReadData "
                        "response");
                }
                input.PopLimit(oldLimit);
                break;
            }

            case NProto::TReadDataResponse::kBufferOffsetFieldNumber: {
                if (wire != WireFormatLite::WIRETYPE_VARINT) {
                    return MakeError(
                        E_FAIL,
                        "Invalid wire type "
                        "for buffer offset field in ReadData response: %d",
                        wire);
                }

                ui32 bufferOffset = 0;
                if (!input.ReadVarint32(&bufferOffset)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to read buffer offset in ReadData "
                        "response");
                }
                response.SetBufferOffset(bufferOffset);
                break;
            }

            default:
                if (!WireFormatLite::SkipField(&input, tag)) {
                    return MakeError(
                        E_FAIL,
                        "Failed to skip field in ReadData response");
                }
        }
    }

    if (!input.ConsumedEntireMessage()) {
        return MakeError(
            E_FAIL,
            "Failed to consume entire ReadData response message");
    }

    return {};
}

}   // namespace NCloud::NFileStore
