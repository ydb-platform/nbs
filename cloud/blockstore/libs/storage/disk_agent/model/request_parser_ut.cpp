#include "request_parser.h"
#include "public.h"

#include <cloud/blockstore/libs/storage/api/disk_agent.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/protobuf/src/google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;
using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRequestParserTest)
{
    Y_UNIT_TEST(ShouldBeActual)
    {
        const auto* descr = NProto::TWriteDeviceBlocksRequest::GetDescriptor();
        UNIT_ASSERT(descr);

        UNIT_ASSERT_VALUES_EQUAL(7, descr->field_count());
        UNIT_ASSERT_VALUES_EQUAL(1, descr->field(0)->number());
        UNIT_ASSERT_VALUES_EQUAL(2, descr->field(1)->number());
        UNIT_ASSERT_VALUES_EQUAL(3, descr->field(2)->number());
        UNIT_ASSERT_VALUES_EQUAL(4, descr->field(3)->number());
        UNIT_ASSERT_VALUES_EQUAL(5, descr->field(4)->number());

        UNIT_ASSERT_VALUES_EQUAL(7, descr->field(5)->number());
        UNIT_ASSERT_VALUES_EQUAL(8, descr->field(6)->number());
    }

    Y_UNIT_TEST(ShouldParseRequest)
    {
        NProto::TWriteDeviceBlocksRequest src;
        auto& headers = *src.MutableHeaders();
        headers.SetTraceId("trace-id");
        headers.SetIdempotenceId(TString(100, 'x'));
        headers.SetClientId("client-id");
        headers.SetTimestamp(Now().GetValue());
        headers.SetRequestId(0x80080042);
        headers.SetRequestTimeout(1500);
        headers.SetRequestGeneration(801);
        headers.SetOptimizeNetworkTransfer(NProto::SKIP_VOID_BLOCKS);
        headers.MutableInternal()->SetAuthToken("auth");
        headers.MutableInternal()->SetTraceTs(Now().GetValue());
        headers.MutableInternal()->SetControlSource(
            NProto::SOURCE_SERVICE_MONITORING);

        src.SetBlockSize(4096);
        src.SetDeviceUUID("uuid-1");
        src.SetStartIndex(42);
        src.SetVolumeRequestId(100500);
        auto& blocks = *src.MutableBlocks();

        const ui64 blockCount = 10;

        for (size_t i = 0; i != blockCount; ++i) {
            blocks.AddBuffers(TString(src.GetBlockSize(), 'a' + i));
        }

        TString buffer;
        UNIT_ASSERT(src.SerializeToString(&buffer));
        UNIT_ASSERT(buffer.size() > 0);

        TAutoPtr<IEventHandle> event{new IEventHandle{
            TEvDiskAgent::EvWriteDeviceBlocksRequest,
            {},           // flags
            TActorId{},   // recipient
            TActorId{},   // sender
            MakeIntrusive<TEventSerializedData>(
                buffer,
                TEventSerializationInfo{}),
            0,   // cookie
        }};

        auto parsedEvent = NDiskAgent::ParseWriteDeviceBlocksRequest(event);

        UNIT_ASSERT_VALUES_EQUAL(
            src.GetBlockSize(),
            parsedEvent->Record.GetBlockSize());

        UNIT_ASSERT_VALUES_EQUAL(
            src.GetDeviceUUID(),
            parsedEvent->Record.GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(
            src.GetStartIndex(),
            parsedEvent->Record.GetStartIndex());

        UNIT_ASSERT_VALUES_EQUAL(
            src.GetVolumeRequestId(),
            parsedEvent->Record.GetVolumeRequestId());

        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(
            src.GetHeaders(),
            parsedEvent->Record.GetHeaders()));

        UNIT_ASSERT(parsedEvent->Storage);
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount * src.GetBlockSize(),
            parsedEvent->StorageSize);

        const char* ptr = parsedEvent->Storage.get();
        for (const auto& buffer: src.GetBlocks().GetBuffers()) {
            UNIT_ASSERT(!std::memcmp(buffer.data(), ptr, buffer.size()));
            ptr += buffer.size();
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
