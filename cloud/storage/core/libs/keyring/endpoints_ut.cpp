#include "endpoints.h"

#include "endpoints_test.h"

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/guid.h>
#include <util/generic/scope.h>

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

////////////////////////////////////////////////////////////////////////////////

struct TStorages
{
    IEndpointStoragePtr EndpointStorage;
    IMutableEndpointStoragePtr MutableEndpointStorage;
};

////////////////////////////////////////////////////////////////////////////////

TStorages InitKeyringStorages()
{
    const TString guid = CreateGuidAsString();
    const TString nbsDesc = "nbs_" + guid;
    const TString endpointsDesc = "nbs_endpoints_" + guid;

    auto endpointStorage = CreateKeyringEndpointStorage(
        nbsDesc,
        endpointsDesc);

    auto mutableEndpointStorage = CreateKeyringMutableEndpointStorage(
        nbsDesc,
        endpointsDesc);

    return {endpointStorage, mutableEndpointStorage};
}

////////////////////////////////////////////////////////////////////////////////

TStorages InitFileStorages()
{
    const TString dirPath = "./" + CreateGuidAsString();

    auto endpointStorage = CreateFileEndpointStorage(dirPath);

    auto mutableEndpointStorage = CreateFileMutableEndpointStorage(dirPath);

    return {endpointStorage, mutableEndpointStorage};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TEndpointsTest)
{
    void ShouldGetStoredEndpoints(const TStorages& storages)
    {
        auto endpointStorage = storages.EndpointStorage;
        auto mutableStorage = storages.MutableEndpointStorage;

        auto error = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(error), error);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        THashMap<TString, TProtoMessage> loadedEndpoints;
        for (size_t i = 0; i < 3; ++i) {
            TString diskId = "TestDisk" + ToString(i);
            auto request = CreateTestProtoMessage(diskId);
            auto keyOrError = mutableStorage->AddEndpoint(
                diskId,
                SerializeEndpoint(request));
            UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetResult());
            loadedEndpoints.emplace(diskId, request);
        }

        auto idsOrError = endpointStorage->GetEndpointIds();
        UNIT_ASSERT_C(!HasError(idsOrError), idsOrError.GetError());

        auto endpointIds = idsOrError.GetResult();
        UNIT_ASSERT_EQUAL(loadedEndpoints.size(), endpointIds.size());

        for (auto keyringId: endpointIds) {
            auto endpointOrError = endpointStorage->GetEndpoint(keyringId);
            UNIT_ASSERT_C(!HasError(endpointOrError), endpointOrError.GetError());
            auto endpoint = DeserializeEndpoint<TProtoMessage>(
                endpointOrError.GetResult());
            UNIT_ASSERT(endpoint);

            auto it = loadedEndpoints.find(GetProtoMessageId(*endpoint));
            UNIT_ASSERT(it != loadedEndpoints.end());

            google::protobuf::util::MessageDifferencer comparator;
            UNIT_ASSERT(comparator.Equals(*endpoint, it->second));
        }
    }

    Y_UNIT_TEST(ShouldGetStoredEndpointsFromKeyring)
    {
        ShouldGetStoredEndpoints(InitKeyringStorages());
    }

    Y_UNIT_TEST(ShouldGetStoredEndpointsFromFiles)
    {
        ShouldGetStoredEndpoints(InitFileStorages());
    }

    void ShouldGetStoredEndpointById(const TStorages& storages)
    {
        auto endpointStorage = storages.EndpointStorage;
        auto mutableStorage = storages.MutableEndpointStorage;

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        const TString diskId = "TestDiskId";

        auto request = CreateTestProtoMessage(diskId);
        auto keyOrError = mutableStorage->AddEndpoint(
            diskId,
            SerializeEndpoint(request));
        UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());

        auto requestOrError = endpointStorage->GetEndpoint(keyOrError.GetResult());
        UNIT_ASSERT_C(!HasError(requestOrError), requestOrError.GetError());
        auto storedRequest = DeserializeEndpoint<TProtoMessage>(
            requestOrError.GetResult());
        UNIT_ASSERT(storedRequest);

        google::protobuf::util::MessageDifferencer comparator;
        UNIT_ASSERT(comparator.Equals(*storedRequest, request));
    }

    Y_UNIT_TEST(ShouldGetStoredEndpointByIdFromKeyring)
    {
        ShouldGetStoredEndpointById(InitKeyringStorages());
    }

    Y_UNIT_TEST(ShouldGetStoredEndpointByIdFromFiles)
    {
        ShouldGetStoredEndpointById(InitFileStorages());
    }

    void ShouldNotGetStoredEndpointByWrongId(const TStorages& storages)
    {
        auto endpointStorage = storages.EndpointStorage;
        auto mutableStorage = storages.MutableEndpointStorage;

        auto initError = mutableStorage->Init();
        UNIT_ASSERT_C(!HasError(initError), initError);

        Y_DEFER {
            auto error = mutableStorage->Remove();
            UNIT_ASSERT_C(!HasError(error), error);
        };

        const TString diskId = "TestDiskId";

        auto request = CreateTestProtoMessage(diskId);
        auto keyOrError = mutableStorage->AddEndpoint(
            diskId,
            SerializeEndpoint(request));
        UNIT_ASSERT_C(!HasError(keyOrError), keyOrError.GetError());

        auto wrongKeyringId = keyOrError.GetResult() + 42;

        auto requestOrError = endpointStorage->GetEndpoint(wrongKeyringId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            requestOrError.GetError().GetCode(),
            requestOrError.GetError());
    }

    Y_UNIT_TEST(ShouldNotGetStoredEndpointByWrongIdFromKeyring)
    {
        ShouldNotGetStoredEndpointByWrongId(InitKeyringStorages());
    }

    Y_UNIT_TEST(ShouldNotGetStoredEndpointByWrongIdFromFiles)
    {
        ShouldNotGetStoredEndpointByWrongId(InitFileStorages());
    }
}

}   // namespace NCloud
