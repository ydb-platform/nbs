#include "protobuf.h"

#include "protocol.h"

#include <cloud/storage/core/libs/common/helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NStorage::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestProtocol
{
    enum ERequestType
    {
        Error = 1,
    };

    static TProtoMessageSerializer* Serializer()
    {
        struct TSerializer: TProtoMessageSerializer
        {
            TSerializer()
            {
                RegisterProto<NProto::TError>(Error);
            }
        };

        return Singleton<TSerializer>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TProtoMessageSerializerTest, ShouldSerializeMessages)
{
    auto* serializer = TTestProtocol::Serializer();

    NProto::TError proto;
    proto.SetCode(E_ARGUMENT);
    proto.SetMessage("test");

    const auto data = TString(1024, 'A');
    const TBlockDataRef part[1] = {TBlockDataRef(data.data(), data.length())};

    const size_t msgByteSize =
        NRdma::TProtoMessageSerializer::MessageByteSize(proto, data.length());
    const TVector<ui32> testedFlags = {0U, RDMA_PROTO_FLAG_DATA_AT_THE_END};
    const TVector<size_t> testedBufferSizes = {
        msgByteSize,
        msgByteSize + 1024,
        msgByteSize + 4096,
    };

    for (const auto bufferSize: testedBufferSizes) {
        for (const auto flag: testedFlags) {
            ui32 flags = 0;
            if (flag) {
                SetProtoFlag(flags, flag);
            }

            auto buffer = TString::Uninitialized(bufferSize);

            const size_t serializedBytes =
                NRdma::TProtoMessageSerializer::SerializeWithData(
                    buffer,
                    TTestProtocol::Error,
                    flags,
                    proto,
                    part);

            if (HasProtoFlag(flags, RDMA_PROTO_FLAG_DATA_AT_THE_END)) {
                ASSERT_EQ(bufferSize, serializedBytes)
                    << "bufferSize=" << bufferSize
                    << " msgByteSize=" << msgByteSize << " flags=" << flags;
            } else {
                ASSERT_EQ(msgByteSize, serializedBytes)
                    << "bufferSize=" << bufferSize
                    << " msgByteSize=" << msgByteSize << " flags=" << flags;
            }

            const auto resultOrError = serializer->Parse(buffer);
            ASSERT_FALSE(HasError(resultOrError))
                << "bufferSize=" << bufferSize << " msgByteSize=" << msgByteSize
                << " flags=" << flags;

            const auto& result = resultOrError.GetResult();
            EXPECT_EQ(TTestProtocol::Error, result.MsgId);
            EXPECT_EQ(data, result.Data);

            const auto& proto2 =
                static_cast<const NProto::TError&>(*result.Proto);
            EXPECT_EQ(E_ARGUMENT, proto2.GetCode());
            EXPECT_EQ("test", proto2.GetMessage());
        }
    }
}

}   // namespace NCloud::NStorage::NRdma
