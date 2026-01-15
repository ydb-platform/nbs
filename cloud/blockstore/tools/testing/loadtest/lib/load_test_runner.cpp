#include "load_test_runner.h"

#include "client_factory.h"
#include "helpers.h"
#include "suite_runner.h"
#include "validation_callback.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/validation/validation.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/endpoints/iface/endpoints.h>
#include <cloud/storage/core/libs/endpoints/fs/fs_endpoints.h>

#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NLoadTest {

using namespace NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool NeedSocketPath(NProto::EClientIpcType ipcType)
{
    switch (ipcType) {
        case NProto::IPC_GRPC:
        case NProto::IPC_VHOST:
        case NProto::IPC_NBD:
            return true;
        case NProto::IPC_NVME:
        case NProto::IPC_SCSI:
        case NProto::IPC_RDMA:
            return false;

        default:
            // not supported
            Y_ABORT();
            return false;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GetFreeSocketPath(const TString dirPath, const TString& name)
{
    TString fileName = TStringBuilder()
        << CreateGuidAsString() << "_" << name;
    TString socketPath = JoinFsPaths(dirPath, fileName);

    // Chop path because it's limited in some environments.
    const size_t UdsPathLenLimit = 100;
    if (socketPath.length() > UdsPathLenLimit) {
        auto toChop = std::min(
            socketPath.length() - UdsPathLenLimit,
            fileName.length() - 1);
        socketPath.erase(socketPath.length() - toChop);
    }
    Y_ENSURE(socketPath.length() <= UdsPathLenLimit);

    TFsPath(socketPath).DeleteIfExists();
    return socketPath;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLoadTestRunner::TLoadTestRunner(
        TLog& log,
        TAppContext& appContext,
        TAliasedVolumes& aliasedVolumes,
        NClient::TClientAppConfigPtr clientConfig,
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IVolumeStatsPtr volumeStats,
        IClientFactory& clientFactory,
        TTestContext& testContext)
    : Log(log)
    , AppContext(appContext)
    , AliasedVolumes(aliasedVolumes)
    , ClientConfig(std::move(clientConfig))
    , Timer(std::move(timer))
    , Scheduler(std::move(scheduler))
    , Logging(std::move(logging))
    , RequestStats(std::move(requestStats))
    , VolumeStats(std::move(volumeStats))
    , ClientFactory(clientFactory)
    , TestContext(testContext)
{
    const auto& endpointStorageDir = ClientFactory.GetEndpointStorageDir();
    if (endpointStorageDir) {
        EndpointStorage = CreateFileEndpointStorage(endpointStorageDir);
    }
}

TVector<ui32> TLoadTestRunner::SuccessOnError(const NProto::TLoadTest& test)
{
    TVector<ui32> successOnError(
        test.GetSuccessOnError().begin(),
        test.GetSuccessOnError().end()
    );

    return successOnError;
}

void TLoadTestRunner::FillLatency(
    const TLatencyHistogram& hist,
    NProto::TLatency& latency)
{
    latency.SetP50(hist.GetValueAtPercentile(50));
    latency.SetP90(hist.GetValueAtPercentile(90));
    latency.SetP95(hist.GetValueAtPercentile(95));
    latency.SetP99(hist.GetValueAtPercentile(99));
    latency.SetP999(hist.GetValueAtPercentile(99.9));
    latency.SetMin(hist.GetMin());
    latency.SetMax(hist.GetMax());
    latency.SetMean(hist.GetMean());
    latency.SetStdDeviation(hist.GetStdDeviation());
}

int TLoadTestRunner::Run(
    const NProto::TLoadTest& test,
    const TVector<TTestContext*> dependencies)
{
    try {
        SetupTest(test, dependencies, SuccessOnError(test));

        if (!AppContext.ShouldStop.load(std::memory_order_acquire)) {
            auto testResult = RunTest(test, SuccessOnError(test));

            if (testResult == NProto::TEST_STATUS_FAILURE) {
                AppContext.FailedTests.fetch_add(1);
                return 1;
            }
        }

        TeardownTest(test, SuccessOnError(test));
    } catch (...) {
        STORAGE_ERROR("Exception during test execution: "
            << CurrentExceptionMessage());
        AppContext.FailedTests.fetch_add(1);
        return EC_LOAD_TEST_FAILED;
    }

    return 0;
}

void TLoadTestRunner::SetupTest(
    const NProto::TLoadTest& test,
    const TVector<TTestContext*>& dependencies,
    const TVector<ui32>& successOnError)
{
    auto volumeName = AliasedVolumes.ResolveAlias(test.GetVolumeName());
    auto volumeFile = test.GetVolumeFile();

    if (volumeFile) {
        TestContext.Client = ClientFactory.CreateAndStartFilesystemClient();
        volumeName = std::move(volumeFile);
    } else {
        TestContext.Client = ClientFactory.CreateClient(successOnError);
        TestContext.Client->Start();
    }

    bool isEmptyVolume = false;

    if (volumeName.empty()) {
        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        request->CopyFrom(test.GetCreateVolumeRequest());

        volumeName = request->GetDiskId();

        if (volumeName.StartsWith("@")) {
            const auto alias = volumeName;
            volumeName =
                TString("loadtest-") + alias + "-" + CreateGuidAsString();
            AliasedVolumes.RegisterAlias(volumeName, alias);
            request->SetDiskId(volumeName);
        }

        if (!volumeName) {
            volumeName = TString("loadtest-") + CreateGuidAsString();
            request->SetDiskId(volumeName);
        }

        request->SetBaseDiskId(AliasedVolumes.ResolveAlias(request->GetBaseDiskId()));

        isEmptyVolume = !request->GetBaseDiskId();

        const auto requestId = GetRequestId(*request);

        STORAGE_INFO("Create volume: " << volumeName);
        WaitForCompletion(
            "CreateVolume",
            TestContext.Client->CreateVolume(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)),
            successOnError);
    }

    TSessionConfig sessionConfig;
    sessionConfig.DiskId = volumeName;

    if (test.HasStartEndpointRequest()) {
        auto request = std::make_shared<NProto::TStartEndpointRequest>();
        request->CopyFrom(test.GetStartEndpointRequest());
        request->SetDiskId(volumeName);

        auto clientIpcType = request->GetIpcType();

        if (clientIpcType == NProto::IPC_NBD) {
            // start grpc-endpoint + additional nbd-endpoint
            request->SetIpcType(NProto::IPC_GRPC);
        }

        sessionConfig.AccessMode = request->GetVolumeAccessMode();
        sessionConfig.MountMode = request->GetVolumeMountMode();
        sessionConfig.MountFlags = request->GetMountFlags();
        sessionConfig.IpcType = request->GetIpcType();

        EndpointSocketPath = request->GetUnixSocketPath();
        if (EndpointSocketPath.empty()) {
            if (NeedSocketPath(clientIpcType)) {
                EndpointSocketPath = GetFreeSocketPath(
                    AppContext.TempDir.Path(),
                    "endpoint.socket");
            } else {
                EndpointSocketPath = CreateGuidAsString();
            }
            request->SetUnixSocketPath(EndpointSocketPath);
        }

        auto clientId = request->GetClientId();
        if (!clientId) {
            clientId = CreateGuidAsString();
            request->SetClientId(clientId);
        }

        const auto requestId = GetRequestId(*request);

        STORAGE_INFO("Start endpoint"
            << ", ipcType: " << static_cast<int>(request->GetIpcType())
            << ", volume: " << request->GetDiskId()
            << ", socket: " << request->GetUnixSocketPath()
            << ", endpointsDir: " << ClientFactory.GetEndpointStorageDir());

        if (EndpointStorage) {
            EndpointsDir = std::make_unique<TTempDir>(
                ClientFactory.GetEndpointStorageDir());

            auto strOrError = SerializeEndpoint(*request);
            Y_ABORT_UNLESS(!HasError(strOrError));

            auto keyOrError = EndpointStorage->AddEndpoint(
                request->GetUnixSocketPath(),
                strOrError.GetResult());
            Y_ABORT_UNLESS(!HasError(keyOrError));
        }

        WaitForCompletion(
            "StartEndpoint",
            TestContext.Client->StartEndpoint(
                MakeIntrusive<TCallContext>(requestId),
                request),
            successOnError);

        STORAGE_INFO("Create endpoint client, ipcType: "
            << static_cast<int>(clientIpcType));

        TestContext.DataClient = ClientFactory.CreateEndpointDataClient(
            clientIpcType,
            request->GetUnixSocketPath(),
            request->GetClientId(),
            successOnError);
    } else {
        const auto& mountVolumeRequest = test.GetMountVolumeRequest();
        sessionConfig.MountToken = mountVolumeRequest.GetToken();
        sessionConfig.AccessMode = mountVolumeRequest.GetVolumeAccessMode();
        sessionConfig.MountMode = mountVolumeRequest.GetVolumeMountMode();
        sessionConfig.MountFlags = mountVolumeRequest.GetMountFlags();
        sessionConfig.IpcType = NProto::IPC_GRPC;
        sessionConfig.EncryptionSpec = mountVolumeRequest.GetEncryptionSpec();

        TestContext.DataClient = TestContext.Client;
    }

    IBlockStoreValidationClient* validationClient = nullptr;
    auto validationRange = DefaultValidationRange;
    if (test.GetVerify()) {
        if (test.GetValidationRangeBlockCount()) {
            validationRange = TBlockRange64::WithLength(
                test.GetValidationRangeStart(),
                test.GetValidationRangeBlockCount()
            );
        }

        auto client = ClientFactory.CreateValidationClient(
            std::move(TestContext.DataClient),
            std::make_shared<TValidationCallback>(AppContext),
            MakeLoggingTag(test.GetName()),
            validationRange);
        validationClient = client.get();
        TestContext.DataClient = std::move(client);
    }

    if (test.HasClientPerformanceProfile()) {
        TestContext.DataClient = ClientFactory.CreateThrottlingClient(
            std::move(TestContext.DataClient),
            test.GetClientPerformanceProfile()
        );
    }

    TestContext.DataClient->Start();

    TestContext.Session = CreateSession(
        Timer,
        Scheduler,
        Logging,
        RequestStats,
        VolumeStats,
        TestContext.DataClient,
        ClientConfig,
        sessionConfig);

    STORAGE_INFO("Mount volume: " << volumeName);
    auto response = WaitForCompletion(
        "MountVolume",
        TestContext.Session->MountVolume(),
        successOnError);

    TestContext.Volume = response.GetVolume();

    if (validationClient) {
        STORAGE_INFO(
            "Initialize validation client block crcs" <<
            " for test: " << test.GetName() <<
            " and volume: " << volumeName);

        if (isEmptyVolume) {
            validationClient->InitializeBlockChecksums(volumeName);
        } else if (dependencies) {
            validationClient->InitializeBlockChecksums(volumeName);

            TVector<std::pair<ui64, ui64>> checksums;

            for (const auto* dependency: dependencies) {
                if (!dependency->Finished.load(std::memory_order_acquire)) {
                    throw yexception() << "dependency for test " << test.GetName()
                        << " still running";
                }

                for (const auto& x: dependency->BlockChecksums) {
                    checksums.push_back(std::make_pair(x.first, x.second));
                }
            }

            validationClient->SetBlockChecksums(volumeName, checksums);
        } else {
            auto blockSize = TestContext.Volume.GetBlockSize();
            auto blocksCount = TestContext.Volume.GetBlocksCount();
            const ui64 chunkSize = 1024;
            const ui64 bufferSize = blockSize * chunkSize;

            TVector<char> buffer(bufferSize);

            for (ui64 blockIndex: xrange(validationRange, chunkSize)) {
                if (blockIndex >= blocksCount) {
                    break;
                }

                if (AppContext.ShouldStop.load(std::memory_order_acquire)) {
                    STORAGE_INFO(
                        "Cancelled block crcs initialization for volume: " << volumeName);
                    return;
                }

                const auto requestSize = Min(chunkSize, blocksCount - blockIndex);
                auto sglist = TSgList{{buffer.data(), blockSize * requestSize}};

                auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
                request->SetStartIndex(blockIndex);
                request->SetBlocksCount(requestSize);
                request->SetCheckpointId(test.GetCheckpointId());
                request->BlockSize = blockSize;
                request->Sglist = TGuardedSgList(std::move(sglist));
                auto guardedSgList = request->Sglist;

                auto future = TestContext.Session->ReadBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::move(request));
                auto response = future.GetValueSync();
                guardedSgList.Close();

                if (HasError(response)) {
                    const auto& error = response.GetError();
                    STORAGE_ERROR(
                        "Validation client initialization failed" <<
                        " for disk " << volumeName <<
                        " at block " << blockIndex <<
                        " range " << requestSize <<
                        " with error " << error.GetCode() <<
                        " , message " << error.GetMessage());
                    throw yexception()
                        << "Failed to initialize validation client: "
                        << FormatError(error);
                }
            }
        }

        STORAGE_INFO(
            "Completed block crcs initialization for volume: " << volumeName);
    }
}

NProto::ETestStatus TLoadTestRunner::RunTest(
    const NProto::TLoadTest& test,
    const TVector<ui32>& successOnError)
{
    STORAGE_INFO("Started test: " << test.GetName());

    TSuiteRunner suiteRunner(
        AppContext,
        Logging,
        successOnError,
        test.GetCheckpointId(),
        test.GetName(),
        TestContext
    );

    if (test.HasArtificialLoadSpec()) {
        for (const auto& range: test.GetArtificialLoadSpec().GetRanges()) {
            suiteRunner.StartSubtest(range);
        }
    } else if (test.HasRealLoadSpec()) {
        const auto& rls = test.GetRealLoadSpec();
        suiteRunner.StartReplay(
            rls.GetProfileLogPath(),
            rls.GetIoDepth(),
            rls.GetFullSpeed(),
            rls.GetDiskId(),
            rls.GetStartTime(),
            rls.GetEndTime(),
            rls.GetMaxRequestsInMemory()
        );
    }

    suiteRunner.Wait(test.GetTestDuration());

    STORAGE_INFO("Completed test: " << test.GetName());
    auto stopped = TInstant::Now();

    NJson::TJsonValue result;

    auto request = std::make_shared<NProto::TStatVolumeRequest>();
    request->SetDiskId(TestContext.Volume.GetDiskId());

    STORAGE_INFO("Stat volume: " << request->GetDiskId());
    auto response = WaitForCompletion(
        "StatVolume",
        TestContext.Client->StatVolume(
            MakeIntrusive<TCallContext>(GetRequestId(*request)),
            request
        ),
        {}
    );

    NProtobufJson::TProto2JsonConfig config;
    config.AddStringTransform(new NProtobufJson::TBase64EncodeBytesTransform);

    NProtobufJson::Proto2Json(response, result["StatVolumeResponse"], config);

    const auto& suiteResults = suiteRunner.GetResults();

    NProto::TTestResults proto;
    proto.SetName(test.GetName());
    proto.SetResult(suiteResults.Status);
    proto.SetStartTime(suiteRunner.GetStartTime().MicroSeconds());
    proto.SetEndTime(stopped.MicroSeconds());
    proto.SetRequestsCompleted(suiteResults.RequestsCompleted);

    if (suiteResults.BlocksRead) {
        proto.SetBlocksRead(suiteResults.BlocksRead);
        FillLatency(suiteResults.ReadHist, *proto.MutableReadLatency());
    }

    if (suiteResults.BlocksWritten) {
        proto.SetBlocksWritten(suiteResults.BlocksWritten);
        FillLatency(suiteResults.WriteHist, *proto.MutableWriteLatency());
    }

    if (suiteResults.BlocksZeroed) {
        proto.SetBlocksZeroed(suiteResults.BlocksZeroed);
        FillLatency(suiteResults.ZeroHist, *proto.MutableZeroLatency());
    }

    NProtobufJson::Proto2Json(proto, result["TestResults"], {});
    TestContext.Result = NJson::WriteJson(result, false, false, false);

    return suiteResults.Status;
}

void TLoadTestRunner::TeardownTest(
    const NProto::TLoadTest& test,
    const TVector<ui32>& successOnError)
{
    const auto& diskId = TestContext.Volume.GetDiskId();

    STORAGE_INFO("Unmount volume: " << diskId);
    WaitForCompletion(
        "UnmountVolume",
        TestContext.Session->UnmountVolume(),
        successOnError);

    if (TestContext.DataClient) {
        TestContext.DataClient->Stop();
    }

    if (test.HasStartEndpointRequest()) {
        auto request = std::make_shared<NProto::TStopEndpointRequest>();
        request->SetUnixSocketPath(EndpointSocketPath);

        STORAGE_INFO("Stop endpoint"
            << ", socket: " << request->GetUnixSocketPath());

        WaitForCompletion(
            "StopEndpoint",
            TestContext.Client->StopEndpoint(
                MakeIntrusive<TCallContext>(),
                request),
            successOnError);

        if (EndpointStorage) {
            auto error = EndpointStorage->RemoveEndpoint(EndpointSocketPath);
            Y_ABORT_UNLESS(!HasError(error));
            EndpointsDir.reset();
        }
    }

    auto isAliased = AliasedVolumes.IsAliased(diskId);

    // Aliased volumes will be destroyed later.
    if (!isAliased && !test.GetVolumeName() && !test.GetVolumeFile()) {
        auto request = std::make_shared<NProto::TDestroyVolumeRequest>();
        request->SetDiskId(diskId);

        const auto requestId = GetRequestId(*request);

        STORAGE_INFO("Destroy volume: " << diskId);
        WaitForCompletion(
            "DestroyVolume",
            TestContext.Client->DestroyVolume(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request)),
            successOnError);
    }

    if (TestContext.Client) {
        TestContext.Client->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NLoadTest
