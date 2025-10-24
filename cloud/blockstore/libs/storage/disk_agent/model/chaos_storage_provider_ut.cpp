#include "chaos_storage_provider.h"

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

using namespace NThreading;

class TMockStorage final: public IStorage
{
public:
    ui64 RequestCount = 0;
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> WriteRequest;

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        ++RequestCount;
        return MakeFuture(NProto::TZeroBlocksResponse());
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        ++RequestCount;
        return MakeFuture(NProto::TReadBlocksLocalResponse());
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        Y_UNUSED(callContext);

        ++RequestCount;
        WriteRequest = request;
        return MakeFuture(NProto::TWriteBlocksLocalResponse());
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        ++RequestCount;
        return MakeFuture(NProto::TError());
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void ReportIOError() override
    {}
};

struct TMockStorageProvider: public IStorageProvider
{
    std::shared_ptr<TMockStorage> Storage = std::make_shared<TMockStorage>();

    NThreading::TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume&,
        const TString&,
        NProto::EVolumeAccessMode) override
    {
        IStoragePtr storage = Storage;
        return NThreading::MakeFuture(storage);
    }
};

struct TFixture: public NUnitTest::TBaseFixture
{
    std::shared_ptr<TMockStorageProvider> MockProvider =
        std::make_shared<TMockStorageProvider>();

    [[nodiscard]] IStoragePtr CreateStorage(
        const NProto::TChaosConfig& chaosConfig) const
    {
        auto chaosProvider =
            NServer::CreateChaosStorageProvider(MockProvider, chaosConfig);

        auto chaosStorage =
            chaosProvider
                ->CreateStorage(
                    NProto::TVolume{},
                    TString{},
                    NProto::EVolumeAccessMode::VOLUME_ACCESS_READ_WRITE)
                .GetValueSync();
        return chaosStorage;
    }

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {}
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

Y_UNIT_TEST_SUITE(TChaosStorageProviderTest)
{
    Y_UNIT_TEST_F(ShouldPassRequestsToUnderlingLayer, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(0.0);
        auto chaosStorage = CreateStorage(chaosConfig);

        {
            auto response =
                chaosStorage
                    ->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TZeroBlocksRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        {
            auto response =
                chaosStorage
                    ->ReadBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TReadBlocksLocalRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            const TString data(8, 'a');
            request->MutableBlocks()->AddBuffers(data.c_str(), data.size());

            auto response =
                chaosStorage
                    ->WriteBlocksLocal(MakeIntrusive<TCallContext>(), request)
                    .GetValueSync();
            const auto& writtenBlock =
                MockProvider->Storage->WriteRequest->GetBlocks().GetBuffers(0);
            UNIT_ASSERT_VALUES_EQUAL(data, writtenBlock);

            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        UNIT_ASSERT_VALUES_EQUAL(3, MockProvider->Storage->RequestCount);
    }

    Y_UNIT_TEST_F(ShouldGenerateErrorsImmediately, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(1.0);
        chaosConfig.SetImmediateReplyProbability(1.0);
        chaosConfig.SetDataDamageProbability(0.0);
        auto chaosStorage = CreateStorage(chaosConfig);

        {
            auto response =
                chaosStorage
                    ->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TZeroBlocksRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        {
            auto response =
                chaosStorage
                    ->ReadBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TReadBlocksLocalRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        {
            auto response =
                chaosStorage
                    ->WriteBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TWriteBlocksLocalRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        UNIT_ASSERT_VALUES_EQUAL(0, MockProvider->Storage->RequestCount);
    }

    Y_UNIT_TEST_F(ShouldGenerateDelayedErrors, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(1.0);
        chaosConfig.SetImmediateReplyProbability(0.0);
        chaosConfig.SetDataDamageProbability(0.0);
        auto chaosStorage = CreateStorage(chaosConfig);

        {
            auto response =
                chaosStorage
                    ->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TZeroBlocksRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
        {
            auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
            const TString data(8, 'a');
            request->MutableBlocks()->AddBuffers(data.c_str(), data.size());

            auto response =
                chaosStorage
                    ->WriteBlocksLocal(MakeIntrusive<TCallContext>(), request)
                    .GetValueSync();
            const auto& writtenBlock =
                MockProvider->Storage->WriteRequest->GetBlocks().GetBuffers(0);
            UNIT_ASSERT_VALUES_EQUAL(data, writtenBlock);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }

        UNIT_ASSERT_VALUES_EQUAL(2, MockProvider->Storage->RequestCount);
    }

    Y_UNIT_TEST_F(ShouldGenerateErrorsFormConfig, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(1.0);
        chaosConfig.SetImmediateReplyProbability(0.0);
        chaosConfig.SetDataDamageProbability(0.0);
        chaosConfig.MutableErrorCodes()->Add(E_TIMEOUT);
        chaosConfig.MutableErrorCodes()->Add(E_FAIL);
        auto chaosStorage = CreateStorage(chaosConfig);

        TMap<EWellKnownResultCodes, ui64> errorCodes;

        for (size_t i = 0; i < 100; ++i) {
            auto response =
                chaosStorage
                    ->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TZeroBlocksRequest>())
                    .GetValueSync();
            errorCodes[static_cast<EWellKnownResultCodes>(
                response.GetError().GetCode())]++;
        }

        UNIT_ASSERT_VALUES_EQUAL(
            100,
            errorCodes[E_TIMEOUT] + errorCodes[E_FAIL]);
        UNIT_ASSERT_LE(1, errorCodes[E_TIMEOUT]);
        UNIT_ASSERT_LE(1, errorCodes[E_FAIL]);
    }

    Y_UNIT_TEST_F(ShouldReplaceNonErrorsFormConfig, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(1.0);
        chaosConfig.SetImmediateReplyProbability(0.0);
        chaosConfig.SetDataDamageProbability(0.0);
        chaosConfig.MutableErrorCodes()->Add(S_OK);
        chaosConfig.MutableErrorCodes()->Add(S_ALREADY);
        chaosConfig.MutableErrorCodes()->Add(S_FALSE);
        auto chaosStorage = CreateStorage(chaosConfig);

        TMap<EWellKnownResultCodes, ui64> errorCodes;

        for (size_t i = 0; i < 100; ++i) {
            auto response =
                chaosStorage
                    ->ZeroBlocks(
                        MakeIntrusive<TCallContext>(),
                        std::make_shared<NProto::TZeroBlocksRequest>())
                    .GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response.GetError().GetCode(),
                FormatError(response.GetError()));
        }
    }

    Y_UNIT_TEST_F(ShouldDamageDataBeforeWrite, TFixture)
    {
        NProto::TChaosConfig chaosConfig;
        chaosConfig.SetCritEventReportingPolicy(NProto::CCERP_FIRST_ERROR);
        chaosConfig.SetChaosProbability(1.0);
        chaosConfig.SetImmediateReplyProbability(1.0);
        chaosConfig.SetDataDamageProbability(1.0);
        auto chaosStorage = CreateStorage(chaosConfig);

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        const TString data(8, 'a');
        request->MutableBlocks()->AddBuffers(data.c_str(), data.size());

        auto response =
            chaosStorage
                ->WriteBlocksLocal(MakeIntrusive<TCallContext>(), request)
                .GetValueSync();
        const auto& writtenBlock =
            MockProvider->Storage->WriteRequest->GetBlocks().GetBuffers(0);
        UNIT_ASSERT_VALUES_UNEQUAL(data, writtenBlock);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response.GetError().GetCode(),
            FormatError(response.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, MockProvider->Storage->RequestCount);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
