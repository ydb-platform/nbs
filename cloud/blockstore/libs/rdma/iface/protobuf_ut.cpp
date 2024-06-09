#include "protobuf.h"

#include "protocol.h"

#include <library/cpp/testing/unittest/registar.h>

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

        auto data = TString(1024, 'A');
        IOutputStream::TPart part(data.data(), data.length());

        size_t msgByteSize = serializer->MessageByteSize(proto, data.length());
        TVector testedFlags{0U, RDMA_PROTO_FLAG_DATA_AT_THE_END};
        TVector testedBufferSizes{
            msgByteSize,
            msgByteSize + 1024,
            msgByteSize + 4096};

        for (auto bufferSize : testedBufferSizes) {
            for (auto flags : testedFlags) {
                auto buffer = TString::Uninitialized(bufferSize);

                size_t serializedBytes = serializer->Serialize(
                    buffer,
                    TBlockStoreProtocol::ReadBlocksRequest,
                    flags,
                    proto,
                    TContIOVector(&part, 1));

                if (flags & RDMA_PROTO_FLAG_DATA_AT_THE_END) {
                    UNIT_ASSERT_EQUAL(serializedBytes, bufferSize);
                } else {
                    UNIT_ASSERT_EQUAL(serializedBytes, msgByteSize);
                }

                auto resultOrError = serializer->Parse(buffer);
                UNIT_ASSERT(!HasError(resultOrError));

                const auto& result = resultOrError.GetResult();
                UNIT_ASSERT_EQUAL(result.MsgId, TBlockStoreProtocol::ReadBlocksRequest);
                UNIT_ASSERT_EQUAL(result.Data, data);

                const auto& proto2 = static_cast<const NProto::TReadBlocksRequest&>(*result.Proto);
                UNIT_ASSERT_EQUAL(proto2.GetDiskId(), "test");
            }
        }
    }
};

}   // namespace NCloud::NBlockStore::NRdma
