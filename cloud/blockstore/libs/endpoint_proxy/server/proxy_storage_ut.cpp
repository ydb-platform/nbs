#include "proxy_storage.h"

#include <cloud/blockstore/libs/service/service_test.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/core/future.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TProxyStorageTest)
{
    Y_UNIT_TEST(ShouldProxyRequestsAndTrackStats)
    {
        using TZero = std::shared_ptr<NProto::TZeroBlocksRequest>;
        using TRead = std::shared_ptr<NProto::TReadBlocksLocalRequest>;
        using TWrite = std::shared_ptr<NProto::TWriteBlocksLocalRequest>;

        auto service = std::make_shared<TTestService>();
        auto zeroPromise = NThreading::NewPromise<NProto::TZeroBlocksResponse>();
        service->ZeroBlocksHandler = [&] (TZero) {
            return zeroPromise.GetFuture();
        };
        auto readPromise =
            NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
        service->ReadBlocksLocalHandler = [&] (TRead) {
            return readPromise.GetFuture();
        };
        auto writePromise =
            NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
        service->WriteBlocksLocalHandler = [&] (TWrite) {
            return writePromise.GetFuture();
        };
        auto requestStats = CreateProxyRequestStats();
        auto proxyStorage = CreateProxyStorage(service, requestStats, 4_KB);

        const auto& stats = requestStats->GetInternalStats();
        const auto& zeroStats =
            stats[static_cast<ui32>(EBlockStoreRequest::ZeroBlocks)];
        const auto& readStats =
            stats[static_cast<ui32>(EBlockStoreRequest::ReadBlocks)];
        const auto& writeStats =
            stats[static_cast<ui32>(EBlockStoreRequest::WriteBlocks)];

        // ZeroBlocks Success

        {
            auto zeroBlocks = std::make_shared<NProto::TZeroBlocksRequest>();
            zeroBlocks->SetStartIndex(1024);
            zeroBlocks->SetBlocksCount(128);
            auto f = proxyStorage->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                zeroBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, zeroStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(512_KB, zeroStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, zeroStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(512_KB, zeroStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                zeroStats.E(EDiagnosticsErrorKind::Success));

            zeroPromise.SetValue({});
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, zeroStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(512_KB, zeroStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, zeroStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, zeroStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                zeroStats.E(EDiagnosticsErrorKind::Success));
        }

        // ZeroBlocks Error

        {
            zeroPromise = NThreading::NewPromise<NProto::TZeroBlocksResponse>();
            auto zeroBlocks = std::make_shared<NProto::TZeroBlocksRequest>();
            zeroBlocks->SetStartIndex(1024);
            zeroBlocks->SetBlocksCount(128);
            auto f = proxyStorage->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                zeroBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, zeroStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(1_MB, zeroStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, zeroStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(512_KB, zeroStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                zeroStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                zeroStats.E(EDiagnosticsErrorKind::ErrorRetriable));

            NProto::TZeroBlocksResponse result;
            result.MutableError()->SetCode(E_REJECTED);
            zeroPromise.SetValue(result);
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, zeroStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(1_MB, zeroStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, zeroStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, zeroStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                zeroStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                zeroStats.E(EDiagnosticsErrorKind::ErrorRetriable));
        }

        // ReadBlocks Success

        {
            auto readBlocks =
                std::make_shared<NProto::TReadBlocksLocalRequest>();
            TGuardedBuffer<TString> buffer(TString(4_KB, 0));
            readBlocks->Sglist = buffer.GetGuardedSgList();
            readBlocks->SetStartIndex(1024);
            auto f = proxyStorage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                readBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, readStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, readStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, readStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, readStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readStats.E(EDiagnosticsErrorKind::Success));

            readPromise.SetValue({});
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, readStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, readStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, readStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, readStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readStats.E(EDiagnosticsErrorKind::Success));
        }

        // ReadBlocks Error

        {
            readPromise =
                NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
            auto readBlocks =
                std::make_shared<NProto::TReadBlocksLocalRequest>();
            TGuardedBuffer<TString> buffer(TString(4_KB, 0));
            readBlocks->Sglist = buffer.GetGuardedSgList();
            readBlocks->SetStartIndex(1024);
            auto f = proxyStorage->ReadBlocksLocal(
                MakeIntrusive<TCallContext>(),
                readBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, readStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, readStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, readStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, readStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                readStats.E(EDiagnosticsErrorKind::ErrorRetriable));

            NProto::TReadBlocksLocalResponse result;
            result.MutableError()->SetCode(E_REJECTED);
            readPromise.SetValue(result);
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, readStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, readStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, readStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, readStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                readStats.E(EDiagnosticsErrorKind::ErrorRetriable));
        }

        // WriteBlocks Success

        {
            auto writeBlocks =
                std::make_shared<NProto::TWriteBlocksLocalRequest>();
            TGuardedBuffer<TString> buffer(TString(4_KB, 0));
            writeBlocks->Sglist = buffer.GetGuardedSgList();
            writeBlocks->SetStartIndex(1024);
            auto f = proxyStorage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                writeBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, writeStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, writeStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, writeStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, writeStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeStats.E(EDiagnosticsErrorKind::Success));

            writePromise.SetValue({});
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(1, writeStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, writeStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, writeStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, writeStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeStats.E(EDiagnosticsErrorKind::Success));
        }

        // WriteBlocks Error

        {
            writePromise =
                NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
            auto writeBlocks =
                std::make_shared<NProto::TWriteBlocksLocalRequest>();
            TGuardedBuffer<TString> buffer(TString(4_KB, 0));
            writeBlocks->Sglist = buffer.GetGuardedSgList();
            writeBlocks->SetStartIndex(1024);
            auto f = proxyStorage->WriteBlocksLocal(
                MakeIntrusive<TCallContext>(),
                writeBlocks);
            UNIT_ASSERT(!f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, writeStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, writeStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(1, writeStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(4_KB, writeStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                writeStats.E(EDiagnosticsErrorKind::ErrorRetriable));

            NProto::TWriteBlocksLocalResponse result;
            result.MutableError()->SetCode(E_REJECTED);
            writePromise.SetValue(result);
            UNIT_ASSERT(f.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(2, writeStats.Count.load());
            UNIT_ASSERT_VALUES_EQUAL(8_KB, writeStats.RequestBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(0, writeStats.Inflight.load());
            UNIT_ASSERT_VALUES_EQUAL(0, writeStats.InflightBytes.load());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeStats.E(EDiagnosticsErrorKind::Success));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                writeStats.E(EDiagnosticsErrorKind::ErrorRetriable));
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
