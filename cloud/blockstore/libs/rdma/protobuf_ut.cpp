#include "protobuf.h"

#include <library/cpp/testing/unittest/registar.h>

#include <cloud/blockstore/libs/rdma/protobuf.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <util/generic/singleton.h>

namespace NCloud::NBlockStore::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockStoreProtocol
{
    enum ERequestType
    {
        ReadBlocksRequest = 1,
        ReadBlocksResponse = 2,
    };

    static TProtoMessageSerializer* Serializer()
    {
        struct TSerializer : TProtoMessageSerializer
        {
            TSerializer()
            {
                RegisterProto<NProto::TReadBlocksRequest>(ReadBlocksRequest);
                RegisterProto<NProto::TReadBlocksResponse>(ReadBlocksResponse);
            }
        };

        return Singleton<TSerializer>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProtoMessageSerializerTest)
{
    Y_UNIT_TEST(ShouldSerializeMessages)
    {
        auto* serializer = TBlockStoreProtocol::Serializer();

        NProto::TReadBlocksRequest proto;
        proto.SetDiskId("test");

        auto data = TString(1024, 0);
        IOutputStream::TPart part(data.data(), data.length());

        size_t byteSize = serializer->MessageByteSize(proto, data.length());
        auto buffer = TString::Uninitialized(byteSize);

        size_t serializedBytes = serializer->Serialize(
            buffer,
            TBlockStoreProtocol::ReadBlocksRequest,
            proto,
            TContIOVector(&part, 1));
        UNIT_ASSERT_EQUAL(serializedBytes, byteSize);

        auto resultOrError = serializer->Parse(buffer);
        UNIT_ASSERT(!HasError(resultOrError));

        const auto& result = resultOrError.GetResult();
        UNIT_ASSERT_EQUAL(result.MsgId, TBlockStoreProtocol::ReadBlocksRequest);
        UNIT_ASSERT_EQUAL(result.Data, data);

        const auto& proto2 = static_cast<const NProto::TReadBlocksRequest&>(*result.Proto);
        UNIT_ASSERT_EQUAL(proto2.GetDiskId(), "test");
    }
};

}   // namespace NCloud::NBlockStore::NRdma
