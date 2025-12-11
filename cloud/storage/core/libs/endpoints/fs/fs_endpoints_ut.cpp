#include "fs_endpoints.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/generic/guid.h>
#include <util/generic/scope.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TProtoMessage = NCloud::NProto::TError;

TProtoMessage CreateTestProtoMessage(const TString& id)
{
    TProtoMessage msg;
    msg.SetCode(E_FAIL);
    msg.SetMessage(id);
    return msg;
}

const TString& GetProtoMessageId(const TProtoMessage& msg)
{
    return msg.GetMessage();
}

struct TFixture: public NUnitTest::TBaseFixture
{
    const TString DirPath = "./" + CreateGuidAsString();
    std::unique_ptr<TTempDir> EndpointsDir;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        EndpointsDir = std::make_unique<TTempDir>(DirPath);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileEndpointsTest)
{
    Y_UNIT_TEST_F(ShouldGetStoredEndpointsFromFiles, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        THashMap<TString, TProtoMessage> loadedEndpoints;
        for (size_t i = 0; i < 3; ++i) {
            TString diskId = "TestDisk" + ToString(i);
            auto request = CreateTestProtoMessage(diskId);
            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto ret =
                endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
            UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());
            loadedEndpoints.emplace(diskId, request);
        }

        auto idsOrError = endpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(idsOrError), idsOrError.GetError());

        auto endpointIds = idsOrError.GetResult();
        UNIT_ASSERT_EQUAL(loadedEndpoints.size(), endpointIds.size());

        for (const auto& keyringId: endpointIds) {
            auto endpointOrError = endpointStorage->GetEndpoint(keyringId);
            UNIT_ASSERT_C(
                !HasError(endpointOrError),
                endpointOrError.GetError());
            auto endpoint =
                DeserializeEndpoint<TProtoMessage>(endpointOrError.GetResult());
            UNIT_ASSERT(endpoint);

            auto it = loadedEndpoints.find(GetProtoMessageId(*endpoint));
            UNIT_ASSERT(it != loadedEndpoints.end());

            google::protobuf::util::MessageDifferencer comparator;
            UNIT_ASSERT(comparator.Equals(*endpoint, it->second));
        }
    }

    Y_UNIT_TEST_F(ShouldGetStoredEndpointByIdFromFiles, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        const TString diskId = "TestDiskId";

        auto request = CreateTestProtoMessage(diskId);
        auto strOrError = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto ret = endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

        auto requestOrError = endpointStorage->GetEndpoint(diskId);
        UNIT_ASSERT_C(!HasError(requestOrError), requestOrError.GetError());
        auto storedRequest =
            DeserializeEndpoint<TProtoMessage>(requestOrError.GetResult());
        UNIT_ASSERT(storedRequest);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(*storedRequest, request));
    }

    Y_UNIT_TEST_F(ShouldNotGetStoredEndpointByWrongIdFromFiles, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        const TString diskId = "TestDiskId";

        auto request = CreateTestProtoMessage(diskId);
        auto strOrError = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto ret = endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

        const TString wrongKeyringId = "WrongTestDiskId";

        auto requestOrError = endpointStorage->GetEndpoint(wrongKeyringId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            requestOrError.GetError().GetCode(),
            requestOrError.GetError());
    }

    Y_UNIT_TEST_F(ShouldRemoveEndpoint, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        const TString diskId = "TestDiskId";

        auto request = CreateTestProtoMessage(diskId);
        auto strOrError = SerializeEndpoint(request);
        UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

        auto ret = endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

        auto endpointOrError = endpointStorage->GetEndpoint(diskId);
        UNIT_ASSERT_C(!HasError(endpointOrError), endpointOrError.GetError());

        ret = endpointStorage->RemoveEndpoint(diskId);
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

        endpointOrError = endpointStorage->GetEndpoint(diskId);
        UNIT_ASSERT(HasError(endpointOrError));
    }

    Y_UNIT_TEST_F(RemoveNonExistentEndpoint, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        const TString diskId = "TestDiskId";

        auto ret = endpointStorage->RemoveEndpoint(diskId);
        UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());
    }

    Y_UNIT_TEST_F(AddEndpointTwice, TFixture)
    {
        auto endpointStorage = CreateFileEndpointStorage(DirPath);
        const TString diskId = "TestDiskId";
        {
            auto request = CreateTestProtoMessage("request1");
            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto ret =
                endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
            UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());
        }

        {
            auto request = CreateTestProtoMessage("request2");
            auto strOrError = SerializeEndpoint(request);
            UNIT_ASSERT_C(!HasError(strOrError), strOrError.GetError());

            auto ret =
                endpointStorage->AddEndpoint(diskId, strOrError.GetResult());
            UNIT_ASSERT_EQUAL_C(S_OK, ret.GetCode(), ret.GetMessage());

            auto requestOrError = endpointStorage->GetEndpoint(diskId);
            UNIT_ASSERT_EQUAL_C(
                S_OK,
                requestOrError.GetError().GetCode(),
                requestOrError.GetError().GetMessage());

            UNIT_ASSERT_EQUAL(
                strOrError.GetResult(),
                requestOrError.ExtractResult());
        }
    }
}

}   // namespace NCloud
