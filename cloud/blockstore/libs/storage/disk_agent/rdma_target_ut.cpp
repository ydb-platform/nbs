#include "rdma_target.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/rdma_test/server_test.h>
#include <cloud/blockstore/libs/service/storage_test.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRdmaTargetTest)
{
    Y_UNIT_TEST(ShouldProcessRequests)
    {
        const TString clientId = "client_1";
        auto logging = CreateLoggingService(
            "console",
            TLogSettings{TLOG_RESOURCES});

        auto storage = std::make_shared<TTestStorage>();

        const auto blockRange = TBlockRange64::WithLength(0, 1024);

        TVector<TString> realData(blockRange.Size(), TString(4_KB, 0));

        storage->WriteBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);
            auto g = request->Sglist.Acquire();
            UNIT_ASSERT(g);

            for (ui32 i = 0; i < g.Get().size(); ++i) {
                realData[request->GetStartIndex() + i] =
                    g.Get()[i].AsStringBuf();
            }

            NProto::TWriteBlocksLocalResponse response;
            return NThreading::MakeFuture(response);
        };

        storage->ReadBlocksLocalHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        {
            Y_UNUSED(callContext);

            auto g = request->Sglist.Acquire();
            UNIT_ASSERT(g);

            for (ui32 i = 0; i < request->GetBlocksCount(); ++i) {
                memcpy(
                    const_cast<char*>(g.Get()[i].Data()),
                    realData[request->GetStartIndex() + i].Data(),
                    4_KB);
            }

            NProto::TReadBlocksLocalResponse response;
            return NThreading::MakeFuture(response);
        };

        storage->ZeroBlocksHandler = [&] (
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        {
            Y_UNUSED(callContext);

            for (ui32 i = 0; i < request->GetBlocksCount(); ++i) {
                realData[request->GetStartIndex() + i] = TString(4_KB, 0);
            }

            NProto::TZeroBlocksResponse response;
            return NThreading::MakeFuture(response);
        };

        auto server = std::make_shared<TRdmaServerTest>();

        THashMap<TString, TStorageAdapterPtr> devices;
        devices["vasya"] = std::make_shared<TStorageAdapter>(
            storage,
            4_KB,
            true,
            0);
        TVector<TString> uuids;
        for (const auto& [key, value]: devices) {
            uuids.push_back(key);
        }
        auto deviceClient = std::make_shared<TDeviceClient>(
            TDuration::MilliSeconds(100),
            uuids);
        deviceClient->AcquireDevices(
            uuids,
            clientId,
            TInstant::Now(),
            NProto::VOLUME_ACCESS_READ_WRITE,
            0,
            "vol0",
            0);

        NProto::TRdmaEndpoint config;
        config.SetHost("host");
        config.SetPort(11111);
        auto rdmaTarget = CreateRdmaTarget(
            config,
            std::move(logging),
            server,
            deviceClient,
            devices);

        rdmaTarget->Start();

        TString data(blockRange.Size() * 4_KB, 'A');
        TBlockChecksum checksum;
        checksum.Extend(data.Data(), data.Size());

        TString zeroData(blockRange.Size() * 4_KB, 0);
        TBlockChecksum zeroChecksum;
        zeroChecksum.Extend(zeroData.Data(), zeroData.Size());

        // writing some data

        NProto::TWriteDeviceBlocksRequest writeRequest;
        writeRequest.SetDeviceUUID("vasya");
        writeRequest.SetBlockSize(4_KB);
        writeRequest.SetStartIndex(blockRange.Start);
        writeRequest.MutableHeaders()->SetClientId(clientId);

        for (ui32 i = 0; i < blockRange.Size(); ++i) {
            *writeRequest.MutableBlocks()->AddBuffers() = TString(4_KB, 'A');
        }

        auto writeResponse =
            server->ProcessRequest("host", 11111, writeRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            writeResponse.GetError().GetCode(),
            writeResponse.GetError().GetMessage());

        // reading this data

        NProto::TReadDeviceBlocksRequest readRequest;
        readRequest.SetDeviceUUID("vasya");
        readRequest.SetBlockSize(4_KB);
        readRequest.SetStartIndex(blockRange.Start);
        readRequest.SetBlocksCount(blockRange.Size());
        readRequest.MutableHeaders()->SetClientId(clientId);

        auto readResponse = server->ProcessRequest("host", 11111, readRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            readResponse.GetError().GetCode(),
            readResponse.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            readResponse.GetError().GetCode(),
            readResponse.GetError().GetMessage());

        UNIT_ASSERT_VALUES_EQUAL(
            blockRange.Size(),
            readResponse.GetBlocks().BuffersSize());

        UNIT_ASSERT_VALUES_EQUAL(
            TString(4_KB, 'A'),
            readResponse.GetBlocks().GetBuffers(0));

        UNIT_ASSERT_VALUES_EQUAL(
            TString(4_KB, 'A'),
            readResponse.GetBlocks().GetBuffers(1023));

        // checksumming this data

        NProto::TChecksumDeviceBlocksRequest checksumRequest;
        checksumRequest.SetDeviceUUID("vasya");
        checksumRequest.SetBlockSize(4_KB);
        checksumRequest.SetStartIndex(blockRange.Start);
        checksumRequest.SetBlocksCount(blockRange.Size());
        checksumRequest.MutableHeaders()->SetClientId(clientId);

        auto checksumResponse =
            server->ProcessRequest("host", 11111, checksumRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            checksumResponse.GetError().GetCode(),
            checksumResponse.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            checksumResponse.GetError().GetCode(),
            checksumResponse.GetError().GetMessage());

        UNIT_ASSERT_VALUES_EQUAL(
            checksum.GetValue(),
            checksumResponse.GetChecksum());

        // zeroing this data

        NProto::TZeroDeviceBlocksRequest zeroRequest;

        zeroRequest.SetDeviceUUID("vasya");
        zeroRequest.SetBlockSize(4_KB);
        zeroRequest.SetStartIndex(blockRange.Start);
        zeroRequest.SetBlocksCount(blockRange.Size());
        zeroRequest.MutableHeaders()->SetClientId(clientId);

        auto zeroResponse = server->ProcessRequest("host", 11111, zeroRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            zeroResponse.GetError().GetCode(),
            zeroResponse.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            zeroResponse.GetError().GetCode(),
            zeroResponse.GetError().GetMessage());

        // checksumming it again

        checksumResponse =
            server->ProcessRequest("host", 11111, checksumRequest);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            checksumResponse.GetError().GetCode(),
            checksumResponse.GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            checksumResponse.GetError().GetCode(),
            checksumResponse.GetError().GetMessage());

        UNIT_ASSERT_VALUES_EQUAL(
            zeroChecksum.GetValue(),
            checksumResponse.GetChecksum());

        rdmaTarget->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NStorage
