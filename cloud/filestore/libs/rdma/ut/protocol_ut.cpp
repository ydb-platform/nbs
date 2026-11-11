#include <cloud/filestore/libs/rdma/protocol.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NCloud::NStorage::NRdma;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTabletRdmaProtocolTest)
{
    Y_UNIT_TEST(ShouldRoundTripRequestEnvelopeWithBodyInDataSegment)
    {
        NProtoPrivate::TTabletRdmaRequest envelope;
        envelope.SetMethodId(123);
        envelope.MutablePipeServerId()->SetRawX1(42);
        envelope.MutablePipeServerId()->SetRawX2(43);

        const TString body = "serialized-request-body";

        const TBlockDataRef dataRef(body.data(), body.size());
        const TBlockDataRefSpan data(&dataRef, 1);

        const size_t size =
            TProtoMessageSerializer::MessageByteSize(envelope, body.size());

        TString buffer(size, 0);
        const size_t written = TProtoMessageSerializer::SerializeWithData(
            buffer,
            TTabletRdmaProtocol::TabletRequest,
            0,
            envelope,
            data);
        UNIT_ASSERT_VALUES_EQUAL(size, written);

        auto parsed =
            TTabletRdmaProtocol::Serializer()->Parse(buffer).ExtractResult();

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletRequest),
            parsed.MsgId);

        auto* req =
            static_cast<NProtoPrivate::TTabletRdmaRequest*>(parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(123, req->GetMethodId());
        UNIT_ASSERT_VALUES_EQUAL(42, req->GetPipeServerId().GetRawX1());
        UNIT_ASSERT_VALUES_EQUAL(43, req->GetPipeServerId().GetRawX2());

        UNIT_ASSERT_VALUES_EQUAL(body, TString(parsed.Data));
    }

    Y_UNIT_TEST(ShouldRoundTripResponseEnvelopeWithoutBody)
    {
        NProtoPrivate::TTabletRdmaResponse envelope;
        envelope.SetMethodId(7);

        const size_t size =
            TProtoMessageSerializer::MessageByteSize(envelope, 0);

        TString buffer(size, 0);
        TProtoMessageSerializer::Serialize(
            buffer,
            TTabletRdmaProtocol::TabletResponse,
            0,
            envelope);

        auto parsed =
            TTabletRdmaProtocol::Serializer()->Parse(buffer).ExtractResult();

        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(TTabletRdmaProtocol::TabletResponse),
            parsed.MsgId);

        auto* resp = static_cast<NProtoPrivate::TTabletRdmaResponse*>(
            parsed.Proto.get());
        UNIT_ASSERT_VALUES_EQUAL(7, resp->GetMethodId());
        UNIT_ASSERT(parsed.Data.empty());
    }

    Y_UNIT_TEST(ShouldCarryLargeBodyThatWouldNotFitInProtoLengthField)
    {
        NProtoPrivate::TTabletRdmaRequest envelope;
        envelope.SetMethodId(2);

        const TString body(1_MB, 'x');

        const TBlockDataRef dataRef(body.data(), body.size());
        const TBlockDataRefSpan data(&dataRef, 1);

        const size_t size =
            TProtoMessageSerializer::MessageByteSize(envelope, body.size());

        TString buffer(size, 0);
        TProtoMessageSerializer::SerializeWithData(
            buffer,
            TTabletRdmaProtocol::TabletRequest,
            0,
            envelope,
            data);

        auto parsed =
            TTabletRdmaProtocol::Serializer()->Parse(buffer).ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(body.size(), parsed.Data.size());
        UNIT_ASSERT_VALUES_EQUAL(body, TString(parsed.Data));
    }

    Y_UNIT_TEST(ShouldFailToParseUnknownMessage)
    {
        TString buffer(RDMA_PROTO_HEADER_SIZE, 0);
        auto result = TTabletRdmaProtocol::Serializer()->Parse(buffer);
        UNIT_ASSERT(HasError(result));
    }
}

}   // namespace NCloud::NFileStore::NStorage
