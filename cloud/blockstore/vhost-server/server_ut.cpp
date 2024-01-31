#include "server.h"

#include "backend_aio.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>

#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/folder/tempdir.h>
#include <util/generic/size_literals.h>
#include <util/system/file.h>

#include <span>

using namespace NCloud::NBlockStore::NVHostServer;

using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    static constexpr ui32 QueueCount = 8;
    static constexpr ui32 QueueIndex = 4;
    static constexpr ui64 ChunkCount = 3;
    static constexpr ui64 ChunkByteCount = 16_KB;
    static constexpr ui64 TotalByteCount = ChunkByteCount * ChunkCount;
    static constexpr ui64 SectorSize = 512;

    const i64 HeaderSize = 4_KB;
    const i64 PaddingSize = 1_KB;

    const TString SocketPath = "server_ut.vhost";
    const TString Serial = "server_ut";

    NCloud::ILoggingServicePtr Logging;
    std::shared_ptr<IServer> Server;
    TVector<TFile> Files;
    TTempDir TempDir;

    TOptions Options {
        .SocketPath = SocketPath,
        .Serial = Serial,
        .NoSync = true,
        .NoChmod = true,
        .QueueCount = QueueCount,
    };

    NVHost::TClient Client {SocketPath, { .QueueCount = QueueCount }};

    TMonotonicBufferResource Memory;

public:
    void StartServer()
    {
        Server = CreateServer(Logging, CreateAioBackend(Logging));

        Options.Layout.reserve(ChunkCount);
        Files.reserve(ChunkCount);
        for (ui32 i = 0; i != ChunkCount; ++i) {
            auto& file = Files.emplace_back(
                TempDir.Path() / ("nrd_" + ToString(i)),
                EOpenModeFlag::CreateAlways);

            Options.Layout.push_back({
                .DevicePath = file.GetName(),
                .ByteCount = ChunkByteCount
            });

            file.Resize(ChunkByteCount);
        }

        Server->Start(Options);

        UNIT_ASSERT(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void StartServerWithSplitDevices()
    {
        Server = CreateServer(Logging, CreateAioBackend(Logging));

        // H - header
        // D - device
        // P - padding
        //
        // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]

        TVector<i64> offsets(ChunkCount);
        std::generate_n(
            offsets.begin(),
            ChunkCount,
            [&, offset = HeaderSize] () mutable {
                return std::exchange(offset, offset + PaddingSize + ChunkByteCount);
            });

        std::swap(offsets.front(), offsets.back());

        auto& file = Files.emplace_back("nrd_0", EOpenModeFlag::CreateAlways);

        const size_t fileSize = HeaderSize
            + ChunkCount * ChunkByteCount
            + PaddingSize * (ChunkCount - 1);

        file.Resize(fileSize);

        // fill the header
        {
            char header[HeaderSize];
            std::memset(header, 'H', HeaderSize);
            file.Pwrite(header, HeaderSize, 0);
        }

        // fill the space between devices
        {
            char padding[PaddingSize];
            std::memset(padding, 'P', PaddingSize);

            i64 offset = HeaderSize + ChunkByteCount;
            for (ui32 i = 0; i != ChunkCount - 1; ++i) {
                file.Pwrite(padding, PaddingSize, offset);

                offset += PaddingSize + ChunkByteCount;
            }
        }

        Options.Layout.reserve(ChunkCount);

        for (ui32 i = 0; i != ChunkCount; ++i) {
            Options.Layout.push_back({
                .DevicePath = file.GetName(),
                .ByteCount = ChunkByteCount,
                .Offset = offsets[i]
            });
        }

        Server->Start(Options);

        UNIT_ASSERT(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void SetUp(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);

        Logging = NCloud::CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_DEBUG});
    }

    void TearDown(NUnitTest::TTestContext& context) override
    {
        Y_UNUSED(context);

        Client.DeInit();
        Server->Stop();
        Server.reset();

        Files.clear();
        Options.Layout.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T, typename ... Ts>
std::span<char> Create(TMonotonicBufferResource& mem, Ts&& ... args)
{
    std::span buf = mem.Allocate(sizeof(T), alignof(T));
    if (buf.empty()) {
        return {};
    }

    new (buf.data()) T {std::forward<Ts>(args)...};

    return buf;
}

auto Hdr(TMonotonicBufferResource& mem, virtio_blk_req_hdr hdr)
{
    return Create<virtio_blk_req_hdr>(mem, hdr);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST_F(ShouldGetDeviceID, TFixture)
    {
        StartServer();

        std::span hdr = Hdr(Memory, { .type = VIRTIO_BLK_T_GET_ID });
        std::span serial = Memory.Allocate(VIRTIO_BLK_DISKID_LENGTH);
        std::span status = Memory.Allocate(1);

        const ui32 len = Client.WriteAsync(
            QueueIndex,
            { hdr },
            { serial, status }).GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(serial.size() + status.size(), len);
        UNIT_ASSERT_VALUES_EQUAL(Serial, TStringBuf(serial.data()));
        UNIT_ASSERT_VALUES_EQUAL(0, status[0]);
    }

    Y_UNIT_TEST_F(ShouldWriteUnaligned, TFixture)
    {
        StartServer();

        const ui64 sectorsPerChunk = ChunkByteCount / SectorSize;
        const ui64 sectorCount = sectorsPerChunk * ChunkCount;

        std::span hdr = Hdr(Memory, { .type = VIRTIO_BLK_T_OUT });
        std::span sector = Memory.Allocate(SectorSize);
        std::span status = Memory.Allocate(1);

        // write data
        for (ui64 i = 0; i != sectorCount; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector = i;

            memset(sector.data(), 'A' + i, sector.size_bytes());

            const ui32 len = Client.WriteAsync(
                QueueIndex,
                { hdr, sector },
                { status }
            ).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(status.size(), len);
            UNIT_ASSERT_VALUES_EQUAL(0, status[0]);
        }

        // validate
        TString buffer;
        buffer.resize(SectorSize);
        for (ui64 i = 0; i != sectorCount; ++i) {
            const TString expectedData(SectorSize, 'A' + i);

            auto& file = Files[i / sectorsPerChunk];
            file.Load(&buffer[0], buffer.size());
            UNIT_ASSERT_VALUES_EQUAL(expectedData, buffer);
        }

        TSimpleStats prevStats;
        auto stats = Server->GetStats(prevStats);

        UNIT_ASSERT_VALUES_EQUAL(0, stats.CompFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.SubFailed);
        UNIT_ASSERT_VALUES_EQUAL(sectorCount, stats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(sectorCount, stats.Dequeued);
        UNIT_ASSERT_VALUES_EQUAL(sectorCount, stats.Submitted);

        const auto& read = stats.Requests[0];
        UNIT_ASSERT_VALUES_EQUAL(0, read.Count);
        UNIT_ASSERT_VALUES_EQUAL(0, read.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(0, read.Errors);
        UNIT_ASSERT_VALUES_EQUAL(0, read.Unaligned);

        const auto& write = stats.Requests[1];
        UNIT_ASSERT_VALUES_EQUAL(sectorCount, write.Count);
        UNIT_ASSERT_VALUES_EQUAL(TotalByteCount, write.Bytes);
        UNIT_ASSERT_VALUES_EQUAL(0, write.Errors);
        UNIT_ASSERT_VALUES_EQUAL(sectorCount, write.Unaligned);
    }

    Y_UNIT_TEST_F(ShouldWriteToSplitDevices, TFixture)
    {
        StartServerWithSplitDevices();

        // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]

        // initial verification
        {
            char header[HeaderSize];
            TFile file { Files[0].GetName(), EOpenModeFlag::OpenAlways };

            file.Load(header, HeaderSize);
            UNIT_ASSERT_VALUES_EQUAL(
                HeaderSize, std::count(header, header + HeaderSize, 'H'));

            char padding[PaddingSize];
            TVector<char> buffer(ChunkByteCount);

            for (ui32 i = 0; i != ChunkCount; ++i) {
                file.Load(buffer.data(), ChunkByteCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    ChunkByteCount, std::count(buffer.begin(), buffer.end(), 0));

                if (i + 1 != ChunkCount) {
                    file.Load(padding, PaddingSize);
                    UNIT_ASSERT_VALUES_EQUAL(
                        PaddingSize,
                        std::count(padding, padding + PaddingSize, 'P'));
                }
            }
        }

        // disk:   [ --- Dn-1 --- | --- D1 --- | ... | --- Dn-2 --- | --- D0 --- ]
        // write:        ^------------^
        //           offset: ChunkByteCount/2
        //           size:   ChunkByteCount
        {
            std::span hdr = Hdr(Memory, { .type = VIRTIO_BLK_T_OUT });
            std::span buffer = Memory.Allocate(ChunkByteCount);
            std::span status = Memory.Allocate(1);

            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())
                ->sector = ChunkByteCount / 2 / SectorSize;

            std::memset(buffer.data(), 'X', ChunkByteCount);

            const ui32 len = Client.WriteAsync(
                QueueIndex,
                { hdr, buffer },
                { status }
            ).GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL(status.size(), len);
            UNIT_ASSERT_VALUES_EQUAL(0, status[0]);
        }

        // check the write
        {
            char header[HeaderSize];
            TFile file { Files[0].GetName(), EOpenModeFlag::OpenAlways };

            file.Load(header, HeaderSize);
            UNIT_ASSERT_VALUES_EQUAL(
                HeaderSize, std::count(header, header + HeaderSize, 'H'));

            char padding[PaddingSize];
            TVector<char> buffer(ChunkByteCount);

            for (ui32 i = 0; i != ChunkCount; ++i) {
                file.Load(buffer.data(), ChunkByteCount);

                switch (i) {
                    case ChunkCount - 1: {
                        const char* p = buffer.data();
                        const auto none = std::count(p, p + ChunkByteCount / 2, 0);
                        const auto data = std::count(
                            p + ChunkByteCount / 2,
                            p + ChunkByteCount,
                            'X');

                        UNIT_ASSERT_VALUES_EQUAL(ChunkByteCount / 2, none);
                        UNIT_ASSERT_VALUES_EQUAL(ChunkByteCount / 2, data);
                        break;
                    }
                    case 1: {
                        const char* p = buffer.data();
                        const auto data = std::count(p, p + ChunkByteCount / 2, 'X');
                        const auto none = std::count(
                            p + ChunkByteCount / 2,
                            p + ChunkByteCount,
                            0);

                        UNIT_ASSERT_VALUES_EQUAL(ChunkByteCount / 2, data);
                        UNIT_ASSERT_VALUES_EQUAL(ChunkByteCount / 2, none);
                        break;
                    }
                    default: {
                        UNIT_ASSERT_VALUES_EQUAL(
                            ChunkByteCount,
                            std::count(buffer.begin(), buffer.end(), 0));
                        break;
                    }
                }

                if (i + 1 != ChunkCount) {
                    file.Load(padding, PaddingSize);
                    UNIT_ASSERT_VALUES_EQUAL(
                        PaddingSize,
                        std::count(padding, padding + PaddingSize, 'P'));
                }
            }
        }
    }

    Y_UNIT_TEST_F(ShouldHandleMutlipleQueues, TFixture)
    {
        StartServer();

        const ui32 requestCount = 10;
        const ui64 sectorsPerChunk = ChunkByteCount / SectorSize;
        const ui64 sectorCount = sectorsPerChunk * ChunkCount;

        TVector<std::span<char>> statuses;
        TVector<NThreading::TFuture<ui32>> futures;

        for (ui64 i = 0; i != requestCount; ++i) {
            std::span hdr = Hdr(Memory, {
                .type = VIRTIO_BLK_T_OUT,
                .sector = i % sectorCount
            });
            std::span sector = Memory.Allocate(SectorSize);
            std::span status = Memory.Allocate(1);

            UNIT_ASSERT_VALUES_EQUAL(SectorSize, sector.size());
            UNIT_ASSERT_VALUES_EQUAL(1, status.size());

            memset(sector.data(), 'A' + i % 26, sector.size_bytes());

            statuses.push_back(status);
            futures.push_back(Client.WriteAsync(
                i % QueueCount,
                { hdr, sector },
                { status }
            ));
        }

        WaitAll(futures).Wait();

        TSimpleStats prevStats;
        auto stats = Server->GetStats(prevStats);

        UNIT_ASSERT_VALUES_EQUAL(requestCount, stats.Submitted);
        UNIT_ASSERT_VALUES_EQUAL(requestCount, stats.Completed);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.CompFailed);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.SubFailed);

        for (ui32 i = 0; i != requestCount; ++i) {
            const ui32 len = futures[i].GetValueSync();

            UNIT_ASSERT_VALUES_EQUAL_C(
                statuses[i].size(), len,
                sectorsPerChunk << " | " << i);
            UNIT_ASSERT_VALUES_EQUAL(0, statuses[i][0]);
        }
    }
}
