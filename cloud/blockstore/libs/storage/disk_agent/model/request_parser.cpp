#include "request_parser.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<char> AllocateStorage(ui64 size)
{
    return {
        static_cast<char*>(std::aligned_alloc(DefaultBlockSize, size)),
        std::free};
}

////////////////////////////////////////////////////////////////////////////////

struct TFakeRecord
    : NProto::TWriteDeviceBlocksRequest
{
    using TWire = google::protobuf::internal::WireFormatLite;
    using TStream = google::protobuf::io::CodedInputStream;

    std::shared_ptr<char> Storage;
    ui64 StorageSize = 0;

    bool ReadBlocks(TStream& input)
    {
        ui32 length = 0;
        if (!input.ReadVarint32(&length)) {
            return false;
        }

        const ui64 capacity = AlignUp(length, DefaultBlockSize);
        Storage = AllocateStorage(capacity);

        const auto limit = input.PushLimit(static_cast<int>(length));

        char* dst = Storage.get();
        while (const ui32 tag = input.ReadTag()) {
            if (TWire::GetTagFieldNumber(tag) !=
                NProto::TIOVector::kBuffersFieldNumber)
            {
                return false;
            }

            ui32 bufferSize = 0;
            if (!input.ReadVarint32(&bufferSize)) {
                return false;
            }
            if (!input.ReadRaw(dst, static_cast<int>(bufferSize))) {
                return false;
            }

            dst += bufferSize;
        }

        StorageSize = dst - Storage.get();

        input.PopLimit(limit);

        return true;
    }

    bool ParseFromZeroCopyStream(TRopeStream* stream)
    {
        TStream input(stream);

        while (ui32 tag = input.ReadTag()) {
            switch (TWire::GetTagFieldNumber(tag)) {
                case NProto::TWriteDeviceBlocksRequest::kHeadersFieldNumber: {
                    if (!TWire::ReadMessage(&input, MutableHeaders())) {
                        return false;
                    }
                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kDeviceUUIDFieldNumber: {
                    if (!TWire::ReadString(&input, MutableDeviceUUID())) {
                        return false;
                    }
                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kStartIndexFieldNumber: {
                    ui64 val = 0;
                    if (!TWire::ReadPrimitive<ui64, TWire::TYPE_UINT64>(
                            &input,
                            &val))
                    {
                        return false;
                    }
                    SetStartIndex(val);

                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kBlockSizeFieldNumber: {
                    ui32 val = 0;
                    if (!TWire::ReadPrimitive<ui32, TWire::TYPE_UINT32>(
                            &input,
                            &val))
                    {
                        return false;
                    }
                    SetBlockSize(val);

                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kBlocksFieldNumber: {
                    if (!ReadBlocks(input)) {
                        return false;
                    }
                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kVolumeRequestIdFieldNumber: {
                    ui64 val = 0;
                    if (!TWire::ReadPrimitive<ui64, TWire::TYPE_UINT64>(
                            &input,
                            &val))
                    {
                        return false;
                    }
                    SetVolumeRequestId(val);

                    break;
                }
                case NProto::TWriteDeviceBlocksRequest::kMultideviceRequestFieldNumber: {
                    bool val = false;
                    if (!TWire::ReadPrimitive<bool, TWire::TYPE_BOOL>(
                            &input,
                            &val))
                    {
                        return false;
                    }
                    SetMultideviceRequest(val);

                    break;
                }
                default:
                    if (TWire::SkipField(&input, tag)) {
                        return false;
                    }
                    break;
            }
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

auto ParseWriteDeviceBlocksRequest(TAutoPtr<IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>
{
    if (ev->Type != TEvDiskAgent::EvWriteDeviceBlocksRequest) {
        return nullptr;
    }

    using TFakeEvent = TEventPB<
        TProtoRequestEvent<
            TFakeRecord,
            TEvDiskAgent::EvWriteDeviceBlocksRequest>,
        TFakeRecord,
        TEvDiskAgent::EvWriteDeviceBlocksRequest>;

    auto* fakeEvent = ev->Get<TFakeEvent>();

    auto r = std::make_unique<
        TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>();

    r->Record.Swap(&fakeEvent->Record);
    r->Storage = std::move(fakeEvent->Record.Storage);
    r->StorageSize = fakeEvent->Record.StorageSize;

    return r;
}

auto CopyWriteDeviceBlocksRequest(TAutoPtr<IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>
{
    auto request = std::make_unique<
        TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>();

    // parse protobuf
    auto* msg = ev->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
    request->Record.Swap(&msg->Record);

    const auto& buffers = request->Record.GetBlocks().GetBuffers();

    ui64 bytesCount = 0;
    for (const auto& buffer: buffers) {
        bytesCount += buffer.size();
    }

    request->Storage = AllocateStorage(bytesCount);
    request->StorageSize = bytesCount;

    char* dst = request->Storage.get();
    for (const auto& buffer: buffers) {
        std::memcpy(dst, buffer.data(), buffer.size());
        dst += buffer.size();
    }
    request->Record.ClearBlocks();

    return request;
}

auto DefaultWriteDeviceBlocksRequest(TAutoPtr<IEventHandle>& ev)
    -> std::unique_ptr<TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>
{
    auto request = std::make_unique<
        TEvDiskAgentPrivate::TEvParsedWriteDeviceBlocksRequest>();

    auto* msg = ev->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();
    request->Record.Swap(&msg->Record);

    return request;
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
