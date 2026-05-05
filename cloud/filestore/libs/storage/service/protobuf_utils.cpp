#include "protobuf_utils.h"

namespace NCloud::NFileStore {

namespace {

bool SkipField(
    google::protobuf::io::CodedInputStream& input,
    uint32_t tag)
{
    return google::protobuf::internal::WireFormatLite::SkipField(&input, tag);
}

bool ParseError(
    google::protobuf::io::CodedInputStream& input,
    int size,
    NCloud::NProto::TError& error)
{
    auto oldLimit = input.PushLimit(size);

    while (input.BytesUntilLimit() > 0) {
        uint32_t tag = input.ReadTag();
        if (tag == 0) {
            return false;
        }

        int field =
            google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag);
        auto wire =
            google::protobuf::internal::WireFormatLite::GetTagWireType(tag);

        switch (field) {
            case NCloud::NProto::TError::kCodeFieldNumber: {
                if (wire !=
                    google::protobuf::internal::WireFormatLite::WIRETYPE_VARINT)
                {
                    return false;
                }
                ui32 code = 0;
                if (!input.ReadVarint32(&code)) {
                    return false;
                }
                error.SetCode(code);
                break;
            }

            case NCloud::NProto::TError::kMessageFieldNumber: {
                if (wire != google::protobuf::internal::WireFormatLite::
                                WIRETYPE_LENGTH_DELIMITED)
                {
                    return false;
                }

                uint32_t len = 0;
                if (!input.ReadVarint32(&len)) {
                    return false;
                }

                auto* message = error.MutableMessage();
                message->ReserveAndResize(len);
                if (!input.ReadRaw(&(*message)[0], len)) {
                    return false;
                }
                break;
            }

            case NCloud::NProto::TError::kFlagsFieldNumber: {
                if (wire !=
                    google::protobuf::internal::WireFormatLite::WIRETYPE_VARINT)
                {
                    return false;
                }
                ui32 flags = 0;
                if (!input.ReadVarint32(&flags)) {
                    return false;
                }
                error.SetFlags(flags);
                break;
            }

            default:
                if (!SkipField(input, tag)) {
                    return false;
                }
                break;
        }
    }

    input.PopLimit(oldLimit);
    return true;
}

}   // namespace

bool ParseReadDataResponse(
    google::protobuf::io::CodedInputStream& input,
    NProto::TReadDataResponse& response,
    const ::google::protobuf::RepeatedPtrField<
        ::NCloud::NFileStore::NProto::TIovec>& iovecs)
{
    while (true) {
        uint32_t tag = input.ReadTag();

        if (tag == 0) {
            return true;
        }

        int field =
            google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag);
        auto wire =
            google::protobuf::internal::WireFormatLite::GetTagWireType(tag);

        switch (field) {
            case NProto::TReadDataResponse::kErrorFieldNumber: {
                if (wire != google::protobuf::internal::WireFormatLite::
                                WIRETYPE_LENGTH_DELIMITED)
                {
                    return false;
                }

                uint32_t len = 0;
                if (!input.ReadVarint32(&len)) {
                    return false;
                }

                if (!ParseError(
                        input,
                        static_cast<int>(len),
                        *response.MutableError()))
                {
                    return false;
                }

                break;
            }

            case NProto::TReadDataResponse::kBufferFieldNumber: {
                if (wire != google::protobuf::internal::WireFormatLite::
                                WIRETYPE_LENGTH_DELIMITED)
                {
                    return false;
                }

                uint32_t len = 0;
                if (!input.ReadVarint32(&len)) {
                    return false;
                }

                ui64 currentOffset = 0;
                ui64 bufferSize = len;

                for (const auto& iovec: iovecs) {
                    auto dataToWrite = Min(iovec.GetLength(), bufferSize);

                    if (dataToWrite > 0) {
                        char* targetData =
                            reinterpret_cast<char*>(iovec.GetBase());
                        input.ReadRaw(targetData, dataToWrite);
                        bufferSize -= dataToWrite;
                        currentOffset += dataToWrite;
                    }
                }
                response.SetLength(currentOffset);
                break;
            }

            case NProto::TReadDataResponse::kBufferOffsetFieldNumber: {
                if (wire !=
                    google::protobuf::internal::WireFormatLite::WIRETYPE_VARINT)
                {
                    return false;
                }

                ui32 bufferOffset = 0;
                if (!input.ReadVarint32(&bufferOffset)) {
                    return false;
                }
                response.SetBufferOffset(bufferOffset);
                break;
            }

            default:
                if (!SkipField(input, tag)) {
                    return false;
                }
                break;
        }
    }

    return true;
}

}  // namespace NCloud::NFileStore
