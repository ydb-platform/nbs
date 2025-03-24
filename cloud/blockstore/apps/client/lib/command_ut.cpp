#include "bootstrap.h"
#include "factory.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/util/pb_io.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/guid.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/progname.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path_to_test_volume";
static const ui32 DefaultBlockSize = 1024;
static const ui64 DefaultBlocksCount = 4096;

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TClientFactories> MakeClientFactories()
{
    auto clientFactories = std::make_shared<TClientFactories>();

    clientFactories->IamClientFactory =
        [](NCloud::NIamClient::TIamClientConfigPtr config,
           NCloud::ILoggingServicePtr logging,
           NCloud::ISchedulerPtr scheduler,
           NCloud::ITimerPtr timer)
    {
        Y_UNUSED(config);
        Y_UNUSED(logging);
        Y_UNUSED(scheduler);
        Y_UNUSED(timer);
        return NCloud::NIamClient::CreateIamTokenClientStub();
    };
    return clientFactories;
}

bool ExecuteRequest(
    const char* command,
    const TVector<TString>& argv,
    IBlockStorePtr client)
{
    TVector<const char*> args;
    args.reserve(argv.size());

    for (const auto& arg: argv) {
        args.push_back(arg.data());
    }

    auto handler = GetHandler(command, std::move(client));
    if (!handler) {
        Cerr << "Failed to find handler for command " << command << Endl;
        return false;
    }
    handler->SetClientFactories(MakeClientFactories());

    return handler->Run(args.size(), &args[0]);
}

std::pair<TString, TBlockRange64> ParseCheckRangeRequestJson(
    const TString& input)
{
    NJson::TJsonValue json;

    UNIT_ASSERT(NJson::ReadJsonTree(input, &json));

    auto diskId = json["DiskId"].GetStringRobust();
    ui32 startIndex = json["StartIndex"].GetUIntegerRobust();
    ui32 blocksCount = json["BlocksCount"].GetUIntegerRobust();
    return {diskId, TBlockRange64::WithLength(startIndex, blocksCount)};
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCommandTest)
{
    Y_UNIT_TEST(ShouldAutoMountUnmountVolumeOnReadWriteZeroBlocksRequests)
    {
        auto client = std::make_shared<TTestService>();

        bool detectedMountVolumeRequest = false;
        bool detectedUnmountVolumeRequest = false;
        bool detectedReadBlocksRequest = false;
        bool detectedWriteBlocksRequest = false;
        bool detectedZeroBlocksRequest = false;
        TString sessionId = CreateGuidAsString();
        TString mountToken = CreateGuidAsString();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            detectedMountVolumeRequest = true;
            UNIT_ASSERT(
                request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(request->GetToken() == mountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(4096);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            detectedUnmountVolumeRequest = true;
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };
        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            detectedReadBlocksRequest = true;

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            for (ui64 i = 0; i < sglist.size(); ++i) {
                auto* dstPtr = const_cast<char*>(sglist[i].Data());
                memset(dstPtr, 0, sglist[i].Size());
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };
        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            detectedWriteBlocksRequest = true;
            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };
        client->ZeroBlocksHandler =
            [&](std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            detectedZeroBlocksRequest = true;
            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        {
            TVector<TString> argv;
            argv.reserve(5);
            argv.emplace_back(GetProgramName());
            argv.emplace_back(TStringBuilder() << "--token=" << mountToken);
            argv.emplace_back(
                TStringBuilder() << "--disk-id=" << DefaultDiskId);
            argv.emplace_back("--start-index=0");
            argv.emplace_back("--blocks-count=1");

            UNIT_ASSERT(ExecuteRequest("readblocks", argv, client));
            UNIT_ASSERT(detectedMountVolumeRequest);
            UNIT_ASSERT(detectedReadBlocksRequest);
            UNIT_ASSERT(detectedUnmountVolumeRequest);
        }

        detectedMountVolumeRequest = false;
        detectedUnmountVolumeRequest = false;

        {
            TString blocks(1024, 1);

            TFile writeBlocksRequestFile(
                "blocks",
                EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);
            writeBlocksRequestFile.Write(blocks.data(), blocks.size());
            writeBlocksRequestFile.Flush();
            writeBlocksRequestFile.Close();

            TVector<TString> argv;
            argv.reserve(5);
            argv.emplace_back(GetProgramName());
            argv.emplace_back("--token=" + mountToken);
            argv.emplace_back("--disk-id=" + DefaultDiskId);
            argv.emplace_back("--start-index=0");
            argv.emplace_back("--input=blocks");

            UNIT_ASSERT(ExecuteRequest("writeblocks", argv, client));
            UNIT_ASSERT(detectedMountVolumeRequest);
            UNIT_ASSERT(detectedWriteBlocksRequest);
            UNIT_ASSERT(detectedUnmountVolumeRequest);

            NFs::Remove(writeBlocksRequestFile.GetName());
        }

        detectedMountVolumeRequest = false;
        detectedUnmountVolumeRequest = false;

        {
            TVector<TString> argv;
            argv.reserve(5);
            argv.emplace_back(GetProgramName());
            argv.emplace_back("--token=" + mountToken);
            argv.emplace_back("--disk-id=" + DefaultDiskId);
            argv.emplace_back("--start-index=0");
            argv.emplace_back("--blocks-count=1");

            UNIT_ASSERT(ExecuteRequest("zeroblocks", argv, client));
            UNIT_ASSERT(detectedMountVolumeRequest);
            UNIT_ASSERT(detectedZeroBlocksRequest);
            UNIT_ASSERT(detectedUnmountVolumeRequest);
        }
    }

    Y_UNIT_TEST(ShouldSendProtoRequestsAndReceiveProtoResponses)
    {
        TString createVolumeRequest = TStringBuilder()
                                      << " DiskId:" << DefaultDiskId.Quote()
                                      << " BlockSize:" << DefaultBlockSize
                                      << " BlocksCount:" << DefaultBlocksCount;

        TFile createVolumeRequestFile(
            "create-volume-request",
            EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);

        createVolumeRequestFile.Write(
            createVolumeRequest.data(),
            createVolumeRequest.size());

        createVolumeRequestFile.Flush();
        createVolumeRequestFile.Close();

        auto client = std::make_shared<TTestService>();
        bool detectedCreateVolumeRequest = false;

        client->CreateVolumeHandler =
            [&](std::shared_ptr<NProto::TCreateVolumeRequest> request)
        {
            detectedCreateVolumeRequest = true;
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetBlockSize() == DefaultBlockSize);
            UNIT_ASSERT(request->GetBlocksCount() == DefaultBlocksCount);
            NProto::TCreateVolumeResponse response;
            response.MutableError()->SetCode(S_ALREADY);
            return MakeFuture(response);
        };

        TString outputFileName = "create-volume-response";

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--proto");
        argv.emplace_back(
            TStringBuilder()
            << "--input=" << createVolumeRequestFile.GetName());
        argv.emplace_back(TStringBuilder() << "--output=" << outputFileName);

        UNIT_ASSERT(ExecuteRequest("createvolume", argv, client));
        UNIT_ASSERT(detectedCreateVolumeRequest);

        TFile outputFile(outputFileName, EOpenModeFlag::RdOnly);
        TFileInput outputFileStream(outputFile);
        NProto::TCreateVolumeResponse response;
        ParseFromTextFormat(outputFileStream, response);
        UNIT_ASSERT(response.GetError().GetCode() == S_ALREADY);

        NFs::Remove(createVolumeRequestFile.GetName());
        NFs::Remove(outputFileName);
    }

    Y_UNIT_TEST(ShouldReadWholeVolumeIfReadAllOptionIsSet)
    {
        auto client = std::make_shared<TTestService>();

        const ui64 volumeBlocksCount = 4096;
        ui32 mountVolumeCounter = 0;
        ui32 unmountVolumeCounter = 0;
        ui32 readBlocksCounter = 0;
        TString sessionId = CreateGuidAsString();
        TString mountToken = CreateGuidAsString();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(
                request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(request->GetToken() == mountToken);

            ++mountVolumeCounter;

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(volumeBlocksCount);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            ++unmountVolumeCounter;
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };
        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            UNIT_ASSERT(
                request->GetBlocksCount() ==
                Min(1024ul, volumeBlocksCount - 1024 * readBlocksCounter));
            UNIT_ASSERT(request->GetStartIndex() == 1024 * readBlocksCounter);
            ++readBlocksCounter;

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            for (ui64 i = 0; i < sglist.size(); ++i) {
                auto* dstPtr = const_cast<char*>(sglist[i].Data());
                memset(dstPtr, 0, sglist[i].Size());
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(TStringBuilder() << "--token=" << mountToken);
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--read-all");

        UNIT_ASSERT(ExecuteRequest("readblocks", argv, client));
        UNIT_ASSERT(mountVolumeCounter == 1);
        UNIT_ASSERT(unmountVolumeCounter == 1);
        UNIT_ASSERT(readBlocksCounter == 4);
    }

    Y_UNIT_TEST(ShouldRequireBlocksCountIfReadBlocksRequestHasNoReadAllFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--start-index=0");

        UNIT_ASSERT(!ExecuteRequest("readblocks", argv, client));
    }

    Y_UNIT_TEST(ShouldRequireStartIndexIfReadBlocksRequestHasNoReadAllFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--blocks-count=1");

        UNIT_ASSERT(!ExecuteRequest("readblocks", argv, client));
    }

    Y_UNIT_TEST(
        ShouldRefuseToReadBlocksIfReadAllFlagIsSpecifiedAlongWithProtoFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(5);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--read-all");
        argv.emplace_back("--proto");

        UNIT_ASSERT(!ExecuteRequest("readblocks", argv, client));
    }

    Y_UNIT_TEST(ShouldAddReplicaIndexToReadRequestHeadersIfOptionIsSet)
    {
        auto client = std::make_shared<TTestService>();

        const ui64 volumeBlocksCount = 4096;
        TString sessionId = CreateGuidAsString();
        TString mountToken = CreateGuidAsString();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(
                request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(request->GetToken() == mountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(volumeBlocksCount);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };

        const ui32 replicaIndex = 3;
        bool handlerCalled = false;
        client->ReadBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            handlerCalled = true;

            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(
                request->GetHeaders().GetReplicaIndex() == replicaIndex);

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            for (ui64 i = 0; i < sglist.size(); ++i) {
                auto* dstPtr = const_cast<char*>(sglist[i].Data());
                memset(dstPtr, 0, sglist[i].Size());
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

        TVector<TString> argv;
        argv.reserve(5);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(TStringBuilder() << "--token=" << mountToken);
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--read-all");
        argv.emplace_back("--replica-index=3");

        UNIT_ASSERT(ExecuteRequest("readblocks", argv, client));
        UNIT_ASSERT(handlerCalled);
    }

    Y_UNIT_TEST(ShouldSplitLargeWriteVolumeRequestIntoSeveralRequests)
    {
        auto client = std::make_shared<TTestService>();

        const ui64 volumeBlocksCount = 4096;
        ui32 mountVolumeCounter = 0;
        ui32 unmountVolumeCounter = 0;
        ui32 writeBlocksCounter = 0;
        TString sessionId = CreateGuidAsString();
        TString mountToken = CreateGuidAsString();

        TString blocks(volumeBlocksCount * DefaultBlockSize, 1);

        TFile writeBlocksRequestFile(
            "blocks",
            EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr);
        writeBlocksRequestFile.Write(blocks.data(), blocks.size());
        writeBlocksRequestFile.Flush();
        writeBlocksRequestFile.Close();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(
                request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(request->GetToken() == mountToken);

            ++mountVolumeCounter;

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(volumeBlocksCount);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            ++unmountVolumeCounter;
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };
        client->WriteBlocksLocalHandler =
            [&](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            auto expectedCount = Min<size_t>(
                1024,
                volumeBlocksCount - 1024 * writeBlocksCounter);
            UNIT_ASSERT_VALUES_EQUAL(
                expectedCount * DefaultBlockSize,
                SgListGetSize(sglist));
            UNIT_ASSERT(request->GetStartIndex() == 1024 * writeBlocksCounter);
            ++writeBlocksCounter;
            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

        TVector<TString> argv;
        argv.reserve(5);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--token=" + mountToken);
        argv.emplace_back("--disk-id=" + DefaultDiskId);
        argv.emplace_back("--start-index=0");
        argv.emplace_back("--input=blocks");

        UNIT_ASSERT(ExecuteRequest("writeblocks", argv, client));
        UNIT_ASSERT(mountVolumeCounter == 1);
        UNIT_ASSERT(unmountVolumeCounter == 1);
        UNIT_ASSERT(writeBlocksCounter == 4);

        NFs::Remove(writeBlocksRequestFile.GetName());
    }

    Y_UNIT_TEST(ShouldZeroWholeVolumeIfZeroAllOptionIsSet)
    {
        auto client = std::make_shared<TTestService>();

        const ui64 volumeBlocksCount = 4096;
        ui32 mountVolumeCounter = 0;
        ui32 unmountVolumeCounter = 0;
        ui32 zeroBlocksCounter = 0;
        TString sessionId = CreateGuidAsString();
        TString mountToken = CreateGuidAsString();

        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(
                request->GetVolumeMountMode() == NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT(request->GetToken() == mountToken);

            ++mountVolumeCounter;

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(volumeBlocksCount);

            return MakeFuture(response);
        };
        client->UnmountVolumeHandler =
            [&](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            ++unmountVolumeCounter;
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };
        client->ZeroBlocksHandler =
            [&](std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            UNIT_ASSERT(request->GetDiskId() == DefaultDiskId);
            UNIT_ASSERT(request->GetSessionId() == sessionId);
            UNIT_ASSERT(request->GetStartIndex() == 0);
            UNIT_ASSERT(request->GetBlocksCount() == volumeBlocksCount);
            ++zeroBlocksCounter;
            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(TStringBuilder() << "--token=" << mountToken);
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--zero-all");

        UNIT_ASSERT(ExecuteRequest("zeroblocks", argv, client));
        UNIT_ASSERT(mountVolumeCounter == 1);
        UNIT_ASSERT(unmountVolumeCounter == 1);
        UNIT_ASSERT(zeroBlocksCounter == 1);
    }

    Y_UNIT_TEST(ShouldRequireBlocksCountIfZeroBlocksRequestHasNoZeroAllFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--start-index=0");

        UNIT_ASSERT(!ExecuteRequest("zeroblocks", argv, client));
    }

    Y_UNIT_TEST(ShouldRequireStartIndexIfZeroBlocksRequestHasNoZeroAllFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--blocks-count=1");

        UNIT_ASSERT(!ExecuteRequest("zeroblocks", argv, client));
    }

    Y_UNIT_TEST(
        ShouldRefuseToZeroBlocksIfZeroAllFlagIsSpecifiedAlongWithProtoFlag)
    {
        auto client = std::make_shared<TTestService>();

        TVector<TString> argv;
        argv.reserve(5);
        argv.emplace_back(GetProgramName());
        argv.emplace_back(
            TStringBuilder() << "--token=" << CreateGuidAsString());
        argv.emplace_back(TStringBuilder() << "--disk-id=" << DefaultDiskId);
        argv.emplace_back("--zero-all");
        argv.emplace_back("--proto");

        UNIT_ASSERT(!ExecuteRequest("zeroblocks", argv, client));
    }

    Y_UNIT_TEST(ShouldDiscoveryInstancesWorks)
    {
        using NProto::EDiscoveryPortFilter;

        auto client = std::make_shared<TTestService>();

        client->DiscoverInstancesHandler = [&](auto request)
        {
            UNIT_ASSERT(request->GetLimit() == 1);
            UNIT_ASSERT(
                request->GetInstanceFilter() ==
                EDiscoveryPortFilter::DISCOVERY_SECURE_PORT);

            NProto::TDiscoverInstancesResponse response;

            return MakeFuture(response);
        };

        TVector<TString> argv{
            GetProgramName(),
            "--limit=1",
            "--instance-filter=secure"};

        UNIT_ASSERT(ExecuteRequest("discoverinstances", argv, client));
    }

    Y_UNIT_TEST(ShouldStopCommandByTimeout)
    {
        auto promise = NewPromise<NProto::TPingResponse>();

        auto client = std::make_shared<TTestService>();
        client->PingHandler = [&](std::shared_ptr<NProto::TPingRequest> request)
        {
            Y_UNUSED(request);
            return promise;
        };

        TVector<TString> argv{GetProgramName(), "--timeout=1"};

        UNIT_ASSERT(!ExecuteRequest("ping", argv, client));
    }

    Y_UNIT_TEST(ShouldSplitLargeCheckRangeRequestIntoSeveralRequests)
    {
        ui64 defaultMaxBlocksPerRequest = 1024;
        ui64 expectedNumberOfRequest = 10;
        ui32 checkRangeCounter = 0;
        auto blocksCount = defaultMaxBlocksPerRequest * expectedNumberOfRequest;
        auto client = std::make_shared<TTestService>();
        client->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            auto [diskId, range] =
                ParseCheckRangeRequestJson(request->GetInput());

            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, diskId);
            UNIT_ASSERT_VALUES_EQUAL(defaultMaxBlocksPerRequest, range.Size());
            UNIT_ASSERT_VALUES_EQUAL(1024 * checkRangeCounter, range.Start);
            ++checkRangeCounter;
            return MakeFuture<NProto::TExecuteActionResponse>();
        };
        client->StatVolumeHandler =
            [&](std::shared_ptr<NProto::TStatVolumeRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, request->GetDiskId());
            NProto::TStatVolumeResponse response;
            response.MutableVolume()->SetBlocksCount(20480);
            return MakeFuture(response);
        };
        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--disk-id=" + DefaultDiskId);
        argv.emplace_back("--start-index=0");
        argv.emplace_back(TStringBuilder() << "--blocks-count=" << blocksCount);
        UNIT_ASSERT(ExecuteRequest("checkrange", argv, client));
        UNIT_ASSERT_VALUES_EQUAL(expectedNumberOfRequest, checkRangeCounter);
    }

    Y_UNIT_TEST(ShouldUseZeroAsDefaultStartIndexInCheckRange)
    {
        ui64 defaultMaxBlocksPerRequest = 1024;
        auto blocksCount = defaultMaxBlocksPerRequest;
        auto client = std::make_shared<TTestService>();
        client->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            auto [diskId, range] =
                ParseCheckRangeRequestJson(request->GetInput());
            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, diskId);
            UNIT_ASSERT_VALUES_EQUAL(0, range.Start);
            return MakeFuture<NProto::TExecuteActionResponse>();
        };
        client->StatVolumeHandler =
            [&](const std::shared_ptr<NProto::TStatVolumeRequest>& request)
        {
            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, request->GetDiskId());
            NProto::TStatVolumeResponse response;
            response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
            return MakeFuture(response);
        };
        TVector<TString> argv;
        argv.reserve(3);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--disk-id=" + DefaultDiskId);
        argv.emplace_back(TStringBuilder() << "--blocks-count=" << blocksCount);
        UNIT_ASSERT(ExecuteRequest("checkrange", argv, client));
    }

    Y_UNIT_TEST(
        ShouldPerformCheckRangeForEntireDiskWhenBlocksCountIsNotSpecified)
    {
        ui64 blocksPerRequest = 1024;
        ui32 checkRangeCounter = 0;
        auto client = std::make_shared<TTestService>();

        client->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            auto [diskId, range] =
                ParseCheckRangeRequestJson(request->GetInput());

            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, diskId);
            UNIT_ASSERT_VALUES_EQUAL(
                checkRangeCounter * blocksPerRequest,
                range.Start);
            ++checkRangeCounter;
            return MakeFuture<NProto::TExecuteActionResponse>();
        };

        client->StatVolumeHandler =
            [&](std::shared_ptr<NProto::TStatVolumeRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, request->GetDiskId());
            NProto::TStatVolumeResponse response;
            response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
            return MakeFuture(response);
        };
        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--disk-id=" + DefaultDiskId);
        argv.emplace_back("--start-index=0");
        argv.emplace_back(
            TStringBuilder() << "--blocks-per-request=" << blocksPerRequest);
        UNIT_ASSERT(ExecuteRequest("checkrange", argv, client));
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlocksCount,
            checkRangeCounter * blocksPerRequest);
    }

    Y_UNIT_TEST(ShouldRepeatCheckRangeRequestForMirrorDisksErrors)
    {
        ui64 blocksPerRequest = 1024;
        auto client = std::make_shared<TTestService>();

        ui32 count = 0;
        client->ExecuteActionHandler =
            [&](std::shared_ptr<NProto::TExecuteActionRequest> request)
        {
            Y_UNUSED(request);
            count++;

            NProto::TExecuteActionResponse response;
            // E_ARGUMENT transforms to 2147483649
            response.MutableOutput()->append(
                "{\"Status\":{\"Code\":2147483649,\"Message\":\"E_ARGUMENT\"}}");

            return MakeFuture(std::move(response));
        };

        client->StatVolumeHandler =
            [&](std::shared_ptr<NProto::TStatVolumeRequest> request)
        {
            UNIT_ASSERT_VALUES_EQUAL(DefaultDiskId, request->GetDiskId());
            NProto::TStatVolumeResponse response;
            response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
            response.MutableVolume()->SetStorageMediaKind(
                NProto::STORAGE_MEDIA_SSD_MIRROR3);

            return MakeFuture(response);
        };
        TVector<TString> argv;
        argv.reserve(4);
        argv.emplace_back(GetProgramName());
        argv.emplace_back("--disk-id=" + DefaultDiskId);
        argv.emplace_back("--start-index=0");
        argv.emplace_back(
            TStringBuilder() << "--blocks-per-request=" << blocksPerRequest);
        argv.emplace_back(
            TStringBuilder() << "--blocks-count=" << blocksPerRequest);
        UNIT_ASSERT(ExecuteRequest("checkrange", argv, client));
        UNIT_ASSERT_VALUES_EQUAL(2, count);
    }
}

}   // namespace NCloud::NBlockStore::NClient
