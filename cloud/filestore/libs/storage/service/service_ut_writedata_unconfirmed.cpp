#include "service_ut_helpers.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/city.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestSetup
{
    NProto::TStorageConfig StorageConfig;
    std::unique_ptr<TTestEnv> Env;
    ui32 NodeIdx = 0;
    std::unique_ptr<TServiceClient> Service;
    THeaders Headers;
    ui64 NodeId = 0;
    ui64 Handle = 0;
    TString FileSystemId = "test";

    explicit TTestSetup(
        bool unconfirmedEnabled = true,
        bool threeStageWriteEnabled = true,
        bool unalignedThreeStageWriteEnabled = true,
        IProfileLogPtr profileLog = CreateProfileLogStub())
    {
        StorageConfig.SetAddingUnconfirmedDataEnabled(unconfirmedEnabled);
        StorageConfig.SetUnconfirmedDataCountHardLimit(100);
        StorageConfig.SetWriteBlobThreshold(128_KB);
        StorageConfig.SetThreeStageWriteEnabled(threeStageWriteEnabled);
        StorageConfig.SetUnalignedThreeStageWriteEnabled(
            unalignedThreeStageWriteEnabled);

        Env = std::make_unique<TTestEnv>(
            TTestEnvConfig{},
            StorageConfig,
            NKikimr::NFake::TCaches{},
            std::move(profileLog));
        NodeIdx = Env->AddDynamicNode();
        Service = std::make_unique<TServiceClient>(Env->GetRuntime(), NodeIdx);
        Service->CreateFileStore(
            FileSystemId,
            10000,
            DefaultBlockSize,
            NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);

        Headers = Service->InitSession(FileSystemId, "client");

        NodeId =
            Service
                ->CreateNode(Headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        Handle = Service
                     ->CreateHandle(
                         Headers,
                         FileSystemId,
                         NodeId,
                         "",
                         TCreateHandleArgs::RDWR)
                     ->Record.GetHandle();
    }

    TTestActorRuntime& GetRuntime()
    {
        return Env->GetRuntime();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TCriticalEventLogBackend final: public TLogBackend
{
private:
    TVector<TString> Messages;

public:
    void WriteData(const TLogRecord& rec) override
    {
        Messages.emplace_back(rec.Data);
    }

    void ReopenLog() override
    {}

    bool Contains(TStringBuf pattern) const
    {
        for (const auto& message: Messages) {
            if (message.Contains(pattern)) {
                return true;
            }
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvService::TEvWriteDataResponse>
WriteData(TTestSetup& setup, ui64 offset, const TString& data)
{
    return setup.Service->WriteData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        data);
}

std::unique_ptr<TEvService::TEvWriteDataResponse>
WriteData(TTestSetup& setup, ui64 offset, const TVector<TString>& iovecs)
{
    return setup.Service->WriteData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        iovecs);
}

std::unique_ptr<TEvService::TEvReadDataResponse>
ReadData(TTestSetup& setup, ui64 offset, ui64 bytes)
{
    return setup.Service->ReadData(
        setup.Headers,
        setup.FileSystemId,
        setup.NodeId,
        setup.Handle,
        offset,
        bytes);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TWriteDataUnconfirmedTest)
{
    // =========================================================================
    // Success path tests
    // =========================================================================

    Y_UNIT_TEST(ShouldUseUnconfirmedFlowWhenEnabled)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        UNIT_ASSERT_C(
            unconfirmedFlowEnabled,
            "Tablet should enable unconfirmed data");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 1,
            "ConfirmAddData should be called");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldWriteLargeDataWithMultipleBlobs)
    {
        TTestSetup setup;

        TString data =
            GenerateValidateData(DefaultBlockSize * BlockGroupSize * 2);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), stat.GetSize());
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedHead)
    {
        TTestSetup setup;

        const ui64 offset = 100;
        TString data = GenerateValidateData(256_KB);

        WriteData(setup, offset, data);
        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");

        const auto firstBlockResponse = ReadData(setup, 0, 4_KB);
        const auto firstBlock = firstBlockResponse->Record.GetBuffer();
        TString expectedZeros(offset, '\0');
        UNIT_ASSERT_VALUES_EQUAL(
            expectedZeros,
            TString(firstBlock.substr(0, offset)));
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedTail)
    {
        TTestSetup setup;

        TString data = GenerateValidateData(256_KB + 100);

        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size(), stat.GetSize());
    }

    Y_UNIT_TEST(ShouldWriteDataWithUnalignedHeadAndTail)
    {
        TTestSetup setup;

        const ui64 offset = 100;
        const ui64 dataSize = 512_KB + 200;
        TString data = GenerateValidateData(dataSize, 42);

        WriteData(setup, offset, data);
        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
        const auto stat = setup.Service
                              ->GetNodeAttr(
                                  setup.Headers,
                                  setup.FileSystemId,
                                  RootNodeId,
                                  "file")
                              ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(offset + data.size(), stat.GetSize());

        const auto firstBlockResponse = ReadData(setup, 0, 4_KB);
        const auto firstBlock = firstBlockResponse->Record.GetBuffer();

        TString expectedZeros(offset, '\0');
        UNIT_ASSERT_VALUES_EQUAL(
            expectedZeros,
            TString(firstBlock.substr(0, offset)));
    }

    Y_UNIT_TEST(ShouldWriteWithIovecs)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        // Track whether unconfirmed data is allowed
        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        TVector<TString> iovecs;
        for (ui32 i = 0; i < 8; ++i) {
            iovecs.push_back(GenerateValidateData(32_KB, i));
        }

        WriteData(setup, 0, iovecs);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest) >= 1,
            "Three-stage write should trigger GenerateBlobIds");

        UNIT_ASSERT_C(
            unconfirmedFlowEnabled,
            "Tablet should enable unconfirmed data for iovecs");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 1,
            "ConfirmAddData should be called in unconfirmed flow");

        ui64 offset = 0;
        for (const auto& chunk: iovecs) {
            const auto readBufferResponse =
                ReadData(setup, offset, chunk.size());
            const auto readBuffer = readBufferResponse->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL_C(
                CityHash64(chunk),
                CityHash64(readBuffer),
                "Data mismatch at offset " << offset);
            offset += chunk.size();
        }
    }

    Y_UNIT_TEST(ShouldHandleMultipleWritesSequentially)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        for (ui32 i = 0; i < 3; ++i) {
            TString data = GenerateValidateData(256_KB, i);
            WriteData(setup, i * 256_KB, data);
            const auto readDataResponse =
                ReadData(setup, i * 256_KB, data.size());
            const auto readResponse = readDataResponse->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL_C(
                CityHash64(data),
                CityHash64(readResponse),
                "Data mismatch at write " << i);
        }

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 3,
            "ConfirmAddData should be called 3 times for 3 writes");
    }

    // =========================================================================
    // Flow control tests
    // =========================================================================

    Y_UNIT_TEST(ShouldNotUseUnconfirmedFlowWhenDisabled)
    {
        TTestSetup setup(false);
        auto& runtime = setup.GetRuntime();

        bool unconfirmedFlowEnabled = false;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    unconfirmedFlowEnabled =
                        msg->Record.GetUnconfirmedFlowEnabled();
                }
                return false;
            });

        // Use unaligned offset to verify that session-level disable still
        // forces regular AddData flow.
        const ui64 offset = 100;
        TString data = GenerateValidateData(256_KB);
        WriteData(setup, offset, data);

        UNIT_ASSERT_C(
            !unconfirmedFlowEnabled,
            "Unconfirmed data should be disabled");

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest));
        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called in regular flow");

        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldNotUseUnconfirmedFlowWhenTabletRejects)
    {
        // Session has unconfirmed enabled, but the tablet clears
        // UnconfirmedFlowEnabled in the response (e.g. hit hard limit).
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsResponse)
                {
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    msg->Record.SetUnconfirmedFlowEnabled(false);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called when tablet rejects");

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called in regular flow");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    Y_UNIT_TEST(ShouldNotCallConfirmAddDataInRegularFlow)
    {
        TTestSetup setup(false /*unconfirmedEnabled*/);
        auto& runtime = setup.GetRuntime();

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvAddDataRequest) >= 1,
            "AddData should be called");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest));
    }

    // =========================================================================
    // Error handling and fallback tests
    // =========================================================================

    Y_UNIT_TEST(ShouldFallbackOnGenerateBlobIdsError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvGenerateBlobIdsResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called when GenerateBlobIds fails");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldFallbackOnWriteBlobError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NActors::TActorId worker;
        ui32 evPuts = 0;
        ui32 cancelAddDataRequests = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker && evPuts == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        if (event->Recipient == worker) {
                            ++evPuts;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++cancelAddDataRequests;
                        }
                        break;
                    }
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL_C(
            0,
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest),
            "ConfirmAddData should not be called after WriteBlob error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            cancelAddDataRequests,
            "CancelAddData should be called once after WriteBlob error");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
        UNIT_ASSERT_VALUES_EQUAL(1, evPuts);
    }

    Y_UNIT_TEST(ShouldProfileCancelAddDataAsSeparateRequestType)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();
        TTestSetup setup(
            true,   // unconfirmedEnabled
            true,   // threeStageWriteEnabled
            true,   // unalignedThreeStageWriteEnabled
            profileLog);
        auto& runtime = setup.GetRuntime();

        bool enableWriteBlobErrorInjection = false;
        bool writeBlobErrorInjected = false;
        const ui32 confirmAddDataType =
            static_cast<ui32>(EFileStoreRequest::ConfirmAddData);
        const ui32 cancelAddDataType =
            static_cast<ui32>(EFileStoreRequest::CancelAddData);
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (enableWriteBlobErrorInjection &&
                            !writeBlobErrorInjected) {
                            msg->Status = NKikimrProto::ERROR;
                            writeBlobErrorInjected = true;
                        }
                        break;
                    }
                }
                return false;
            });

        const auto confirmBefore =
            profileLog->Requests[confirmAddDataType].size();
        const auto cancelBefore =
            profileLog->Requests[cancelAddDataType].size();

        // Successful unconfirmed flow: ConfirmAddData should be profiled.
        TString firstData = GenerateValidateData(256_KB, 1);
        WriteData(setup, 0, firstData);
        UNIT_ASSERT_C(
            profileLog->Requests[confirmAddDataType].size() >= confirmBefore + 1,
            "ConfirmAddData must be logged under its own request type");
        UNIT_ASSERT_VALUES_EQUAL(
            cancelBefore,
            profileLog->Requests[cancelAddDataType].size());

        // WriteBlob error path: CancelAddData should be profiled, but not
        // ConfirmAddData for this write.
        const auto confirmAfterFirstWrite =
            profileLog->Requests[confirmAddDataType].size();
        const auto cancelAfterFirstWrite =
            profileLog->Requests[cancelAddDataType].size();
        enableWriteBlobErrorInjection = true;
        writeBlobErrorInjected = false;

        TString secondData = GenerateValidateData(256_KB, 2);
        WriteData(setup, 0, secondData);
        UNIT_ASSERT_C(
            writeBlobErrorInjected,
            "WriteBlob error should be injected for CancelAddData path");
        UNIT_ASSERT_VALUES_EQUAL(
            confirmAfterFirstWrite,
            profileLog->Requests[confirmAddDataType].size());
        UNIT_ASSERT_C(
            profileLog->Requests[cancelAddDataType].size() >=
                cancelAfterFirstWrite + 1,
            "CancelAddData must be logged under its own request type");
    }

    Y_UNIT_TEST(ShouldRetryCancelAddDataOnProxyErrorAndThenFallback)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy test error");

        NActors::TActorId worker;
        ui32 evPuts = 0;
        ui32 cancelAddDataRequests = 0;
        ui32 fallbackWriteDataRequests = 0;
        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case NKikimr::TEvBlobStorage::EvPutResult: {
                        auto* msg = event->template Get<
                            NKikimr::TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker && evPuts == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        if (event->Recipient == worker) {
                            ++evPuts;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++cancelAddDataRequests;
                        }
                        break;
                    }
                    case TEvIndexTablet::EvCancelAddDataResponse: {
                        if (!errorInjected) {
                            errorInjected = true;
                            auto* msg = event->template Get<
                                TEvIndexTablet::TEvCancelAddDataResponse>();
                            msg->Record.MutableError()->CopyFrom(error);
                            event->Sender = MakeIndexTabletProxyServiceId();
                        }
                        break;
                    }
                    case TEvService::EvWriteDataRequest: {
                        if (event->Recipient ==
                            MakeIndexTabletProxyServiceId()) {
                            ++fallbackWriteDataRequests;
                        }
                        break;
                    }
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        UNIT_ASSERT_C(
            cancelAddDataRequests >= 2,
            "CancelAddData should be retried on proxy-origin error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            fallbackWriteDataRequests,
            "Fallback to WriteData should happen once after CancelAddData "
            "response is received");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after CancelAddData retry");
    }

    Y_UNIT_TEST(ShouldFallbackOnAddDataErrorInUnconfirmedFlow)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldFallbackOnConfirmAddDataError)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("test error");

        bool errorInjected = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);
        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after fallback");
    }

    Y_UNIT_TEST(ShouldRetryConfirmAddDataOnProxyErrorAndThenUseTabletAnswer)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy test error");

        bool errorInjected = false;
        ui32 fallbackWriteDataRequests = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvWriteDataRequest &&
                    event->Recipient == MakeIndexTabletProxyServiceId())
                {
                    ++fallbackWriteDataRequests;
                }

                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    !errorInjected)
                {
                    errorInjected = true;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    event->Sender = MakeIndexTabletProxyServiceId();
                }

                return false;
            });

        TString data = GenerateValidateData(256_KB);
        WriteData(setup, 0, data);

        UNIT_ASSERT_C(
            runtime.GetCounter(TEvIndexTablet::EvConfirmAddDataRequest) >= 2,
            "ConfirmAddData should be retried on proxy-origin error");
        UNIT_ASSERT_VALUES_EQUAL_C(
            1,
            fallbackWriteDataRequests,
            "After retry, tablet ConfirmAddData error should follow regular "
            "flow and fallback to WriteData");

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch after ConfirmAddData retry");
    }

    Y_UNIT_TEST(ShouldReportCriticalEventOnConfirmAddDataProxyRetryThreshold)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        auto criticalEventBackend =
            std::make_shared<TCriticalEventLogBackend>();
        auto logging = CreateLoggingService(criticalEventBackend);
        logging->Start();
        NCloud::SetCriticalEventsLog(logging->CreateLog("NFS_TEST"));

        NProto::TError error;
        error.SetCode(E_REJECTED);
        error.SetMessage("proxy retry threshold test error");

        constexpr ui32 proxyErrorInjectionLimit = 20;
        ui32 proxyErrorsInjected = 0;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvConfirmAddDataResponse &&
                    proxyErrorsInjected < proxyErrorInjectionLimit)
                {
                    ++proxyErrorsInjected;
                    auto* msg = event->template Get<
                        TEvIndexTablet::TEvConfirmAddDataResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    event->Sender = MakeIndexTabletProxyServiceId();
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB, 17);
        WriteData(setup, 0, data);

        UNIT_ASSERT_VALUES_EQUAL(proxyErrorInjectionLimit, proxyErrorsInjected);
        UNIT_ASSERT_C(
            criticalEventBackend->Contains(
                "CRITICAL_EVENT:AppCriticalEvents/"
                "UnconfirmedFlowProxyRetryThresholdReached"),
            "Expected retry threshold critical event log");

        logging->Stop();

        const auto actualResponse = ReadData(setup, 0, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    // =========================================================================
    // Request verification tests (continued)
    // =========================================================================

    Y_UNIT_TEST(ShouldSendHeadAndTailUnalignedRangesToGenerateBlobIds)
    {
        TTestSetup setup;
        auto& runtime = setup.GetRuntime();

        NProtoPrivate::TGenerateBlobIdsRequest capturedGenRequest;

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvIndexTablet::EvGenerateBlobIdsRequest)
                {
                    capturedGenRequest =
                        event
                            ->template Get<
                                TEvIndexTablet::TEvGenerateBlobIdsRequest>()
                            ->Record;
                }
                return false;
            });

        const ui64 offset = 100;
        const ui64 alignedOffset = 4_KB;
        const ui64 alignedLength = 128_KB;
        const ui64 tailLength = 300;
        const ui64 headLength = alignedOffset - offset;
        const ui64 writeLength = headLength + alignedLength + tailLength;
        TString data = GenerateValidateData(writeLength, 42);

        WriteData(setup, offset, data);

        UNIT_ASSERT_VALUES_EQUAL(alignedOffset, capturedGenRequest.GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(alignedLength, capturedGenRequest.GetLength());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            capturedGenRequest.UnalignedDataRangesSize());

        const ui64 tailOffset = alignedOffset + alignedLength;
        UNIT_ASSERT_C(
            offset + data.size() > tailOffset,
            "Expected head+tail write to include tail bytes");
        const ui64 actualTailLength = offset + data.size() - tailOffset;
        const ui64 alignedDataOffset = headLength;

        UNIT_ASSERT_VALUES_EQUAL(
            offset,
            capturedGenRequest.GetUnalignedDataRanges(0).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).SubStr(0, headLength),
            TStringBuf(
                capturedGenRequest.GetUnalignedDataRanges(0).GetContent()));

        UNIT_ASSERT_VALUES_EQUAL(
            tailOffset,
            capturedGenRequest.GetUnalignedDataRanges(1).GetOffset());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).SubStr(
                alignedDataOffset + alignedLength,
                actualTailLength),
            TStringBuf(
                capturedGenRequest.GetUnalignedDataRanges(1).GetContent()));

        const auto actualResponse = ReadData(setup, offset, data.size());
        const auto actual = actualResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(data),
            CityHash64(actual),
            "Data mismatch");
    }

    // =========================================================================
    // Tablet reboot tests
    // =========================================================================

    Y_UNIT_TEST(ShouldReadAndWriteUnalignedDataAfterTabletReboot)
    {
        TTestSetup setup;
        const auto tabletId =
            setup.Service->GetFileStoreInfo(setup.FileSystemId)
                ->Record.GetFileStore()
                .GetMainTabletId();

        const ui64 firstOffset = 101;
        const TString firstData = GenerateValidateData(256_KB + 137, 13);
        WriteData(setup, firstOffset, firstData);
        const auto actualFirstResponse =
            ReadData(setup, firstOffset, firstData.size());
        const auto actualFirst = actualFirstResponse->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(firstData),
            CityHash64(actualFirst),
            "Data mismatch");

        TIndexTabletClient tablet(setup.GetRuntime(), setup.NodeIdx, tabletId);
        tablet.RebootTablet();

        // Remake session/handle after reboot; old session becomes invalid.
        setup.Headers =
            setup.Service->InitSession(setup.FileSystemId, "client");
        setup.Handle = setup.Service
                           ->CreateHandle(
                               setup.Headers,
                               setup.FileSystemId,
                               setup.NodeId,
                               "",
                               TCreateHandleArgs::RDWR)
                           ->Record.GetHandle();

        const auto actualAfterReboot =
            ReadData(setup, firstOffset, firstData.size());
        const auto actualAfterRebootBuffer =
            actualAfterReboot->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(firstData),
            CityHash64(actualAfterRebootBuffer),
            "Data mismatch after tablet reboot");

        const ui64 secondOffset = 4_KB + 77;
        const TVector<TString> iovecs{
            GenerateValidateData(1_KB, 21),
            GenerateValidateData(3_KB, 22),
            GenerateValidateData(static_cast<ui32>(128_KB + 211 - 4_KB), 23)};
        TString secondData;
        for (const auto& iovec: iovecs) {
            secondData += iovec;
        }

        WriteData(setup, secondOffset, iovecs);
        const auto actualSecond =
            ReadData(setup, secondOffset, secondData.size());
        const auto actualSecondBuffer = actualSecond->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL_C(
            CityHash64(secondData),
            CityHash64(actualSecondBuffer),
            "Data mismatch for unaligned iovec write after tablet reboot");
    }
}

}   // namespace NCloud::NFileStore::NStorage
