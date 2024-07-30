#include "server.h"

#include "backend_aio.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryptor.h>
#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>

#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/size_literals.h>
#include <util/system/file.h>

#include <vhost/blockdev.h>

#include <span>

namespace NCloud::NBlockStore::NVHostServer {

using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey("1234567890123456789012345678901");

class TServerTest
    : public testing::TestWithParam<NProto::EEncryptionMode>
{
public:
    static constexpr ui32 QueueCount = 8;
    static constexpr ui32 QueueIndex = 4;
    static constexpr ui64 ChunkCount = 3;
    static constexpr ui64 ChunkByteCount = 16_KB;
    static constexpr ui64 TotalByteCount = ChunkByteCount * ChunkCount;
    static constexpr ui64 SectorSize = VHD_SECTOR_SIZE;
    static constexpr ui64 TotalSectorCount = TotalByteCount / SectorSize;

    static constexpr i64 HeaderSize = 4_KB;
    static constexpr i64 PaddingSize = 1_KB;

    const TString SocketPath = "server_ut.vhost";
    const TString Serial = "server_ut";

    NCloud::ILoggingServicePtr Logging;
    std::shared_ptr<IServer> Server;
    TVector<TFile> Files;
    IEncryptorPtr Encryptor;

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
    TServerTest()
    {
        if (GetParam() == NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
            Encryptor =
                CreateAesXtsEncryptor(TEncryptionKey(DefaultEncryptionKey));
        }
    }

    void StartServer()
    {
        Server = CreateServer(
            Logging,
            CreateAioBackend(
                NThreading::MakeFuture<IEncryptorPtr>(Encryptor),
                Logging));

        Options.Layout.reserve(ChunkCount);
        Files.reserve(ChunkCount);
        for (ui32 i = 0; i != ChunkCount; ++i) {
            auto& file = Files.emplace_back(
                "nrd_" + ToString(i),
                EOpenModeFlag::CreateAlways);

            Options.Layout.push_back({
                .DevicePath = file.GetName(),
                .ByteCount = ChunkByteCount
            });

            file.Resize(ChunkByteCount);
        }

        Server->Start(Options);

        ASSERT_TRUE(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void StartServerWithSplitDevices()
    {
        Server = CreateServer(
            Logging,
            CreateAioBackend(
                NThreading::MakeFuture<IEncryptorPtr>(Encryptor),
                Logging));

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

        ASSERT_TRUE(Client.Init());

        Memory = TMonotonicBufferResource {Client.GetMemory()};
    }

    void SetUp() override
    {
        Logging = NCloud::CreateLoggingService(
            "console",
            {.FiltrationLevel = TLOG_DEBUG});
    }

    void TearDown() override
    {
        Client.DeInit();
        Server->Stop();
        Server.reset();

        Files.clear();
        Options.Layout.clear();
    }

    TSimpleStats GetStats(ui64 expectedCompleted) const
    {
        // Without I/O, stats are synced every second and only if there is a
        // pending GetStats call. The first call to GetStats might not bring the
        // latest stats; therefore, you need at least two calls so that the AIO
        // backend will sync the stats.

        TSimpleStats prevStats;
        TSimpleStats stats;
        for (int i = 0; i != 5; ++i) {
            stats = Server->GetStats(prevStats);
            if (stats.Completed == expectedCompleted) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        return stats;
    }

    TString LoadSectorAndDecrypt(ui64 sector)
    {
        const ui64 sectorsPerChunk = ChunkByteCount / SectorSize;
        const ui64 chunkIndex = sector/ sectorsPerChunk;
        const auto& chunkLayout = Options.Layout[chunkIndex];

        auto it = FindIf(
            Files,
            [&](const TFile& f)
            { return f.GetName() == chunkLayout.DevicePath; });
        if (it == Files.end()) {
            return "File " + chunkLayout.DevicePath + " not found";
        }
        auto & file = *it;

        const ui64 fileOffset =
            chunkLayout.Offset + (sector % sectorsPerChunk) * SectorSize;

        TString buffer;
        buffer.resize(SectorSize);
        file.Seek(fileOffset, SeekDir::sSet);
        file.Load(&buffer[0], buffer.size());

        if (Encryptor && !IsAllZeroes(buffer.data(), buffer.size())) {
            Encryptor->Decrypt(
                TBlockDataRef(buffer.data(), buffer.size()),
                TBlockDataRef(buffer.data(), buffer.size()),
                sector);
        }
        return buffer;
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

TEST_P(TServerTest, ShouldGetDeviceID)
{
    StartServer();

    std::span hdr = Hdr(Memory, { .type = VIRTIO_BLK_T_GET_ID });
    std::span serial = Memory.Allocate(VIRTIO_BLK_DISKID_LENGTH);
    std::span status = Memory.Allocate(1);

    const ui32 len = Client.WriteAsync(
        QueueIndex,
        { hdr },
        { serial, status }).GetValueSync();

    EXPECT_EQ(serial.size() + status.size(), len);
    EXPECT_EQ(Serial, TStringBuf(serial.data()));
    EXPECT_EQ(0, status[0]);
}


TEST_P(TServerTest, ShouldWriteUnaligned)
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

        EXPECT_EQ(status.size(), len);
        EXPECT_EQ(0, status[0]);
    }

    // validate
    for (ui64 i = 0; i != sectorCount; ++i) {
        const TString expectedData(SectorSize, 'A' + i);
        const TString realData = LoadSectorAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    const auto stats = GetStats(sectorCount);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(sectorCount, stats.Completed);
    EXPECT_EQ(sectorCount, stats.Dequeued);
    EXPECT_EQ(sectorCount, stats.Submitted);

    const auto& read = stats.Requests[0];
    EXPECT_EQ(0u, read.Count);
    EXPECT_EQ(0u, read.Bytes);
    EXPECT_EQ(0u, read.Errors);
    EXPECT_EQ(0u, read.Unaligned);

    const auto& write = stats.Requests[1];
    EXPECT_EQ(sectorCount, write.Count);
    EXPECT_EQ(TotalByteCount, write.Bytes);
    EXPECT_EQ(0u, write.Errors);
    EXPECT_EQ(sectorCount, write.Unaligned);
}

TEST_P(TServerTest, ShouldWriteToSplitDevices)
{
    StartServerWithSplitDevices();

    std::vector<ui8> sectorsFill(TotalSectorCount);

    // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]
    auto verifyLayoutAndData = [&]()
    {
        char header[HeaderSize];
        TFile file { Files[0].GetName(), EOpenModeFlag::OpenAlways };

        // Check header
        file.Load(header, HeaderSize);
        EXPECT_EQ(HeaderSize, std::count(header, header + HeaderSize, 'H'));

        const TString paddingData(PaddingSize, 'P');
        // Check paddings
        for (ui32 i = 0; i != ChunkCount; ++i) {
            // Skip sectors data
            file.Seek(ChunkByteCount, SeekDir::sCur);

            // Check padding
            if (i + 1 != ChunkCount) {
                TString realPadding(PaddingSize, 0);
                file.Load(const_cast<char*>(realPadding.data()), PaddingSize);
                EXPECT_EQ(paddingData, realPadding);
            }
        }

        // Check sectors
        for (ui32 i = 0; i < TotalSectorCount; ++i) {
            const TString expectedData(SectorSize, sectorsFill[i]);
            const TString realData = LoadSectorAndDecrypt(i);
            EXPECT_EQ(expectedData, realData);
        }
    };
    // initial verification
    verifyLayoutAndData();

    // disk:   [ --- Dn-1 --- | --- D1 --- | ... | --- Dn-2 --- | --- D0 --- ]
    // write:        ^------------^
    //           offset: ChunkByteCount/2
    //           size:   ChunkByteCount
    {
        std::span hdr = Hdr(Memory, { .type = VIRTIO_BLK_T_OUT });
        std::span buffer = Memory.Allocate(ChunkByteCount);
        std::span status = Memory.Allocate(1);

        const ui64 startSector = ChunkByteCount / 2 / SectorSize;
        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector = startSector;

        for (ui32 i = 0; i < ChunkByteCount / SectorSize; ++i) {
            const ui8 sectorFill = (startSector + i) % 256;
            sectorsFill[startSector + i] = sectorFill;
            std::memset(
                buffer.data() + i * SectorSize,
                sectorFill,
                SectorSize);
        }

        const ui32 len = Client.WriteAsync(
            QueueIndex,
            { hdr, buffer },
            { status }
        ).GetValueSync();

        EXPECT_EQ(status.size(), len);
        EXPECT_EQ(0, status[0]);
    }

    // verification after cross-chunk write
    verifyLayoutAndData();
}

TEST_P(TServerTest, ShouldHandleMultipleQueues)
{
    StartServer();

    std::vector<ui8> sectorsFill(TotalSectorCount);
    const ui32 requestCount = 10;

    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;

    for (ui64 i = 0; i != requestCount; ++i) {
        ui64 startSector = i % TotalSectorCount;
        std::span hdr = Hdr(Memory, {
            .type = VIRTIO_BLK_T_OUT,
            .sector = startSector
        });
        std::span sector = Memory.Allocate(SectorSize);
        std::span status = Memory.Allocate(1);

        EXPECT_EQ(SectorSize, sector.size());
        EXPECT_EQ(1u, status.size());

        ui8 sectorFill = 'A' + i % 26;
        memset(sector.data(), sectorFill, sector.size_bytes());
        sectorsFill[startSector] = sectorFill;

        statuses.push_back(status);
        futures.push_back(Client.WriteAsync(
            i % QueueCount,
            { hdr, sector },
            { status }
        ));
    }

    WaitAll(futures).Wait();

    const auto stats = GetStats(requestCount);

    EXPECT_EQ(requestCount, stats.Submitted);
    EXPECT_EQ(requestCount, stats.Completed);
    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);

    for (ui32 i = 0; i != requestCount; ++i) {
        const ui32 len = futures[i].GetValueSync();

        EXPECT_EQ(statuses[i].size(), len);
        EXPECT_EQ(char(0), statuses[i][0]);
    }

    // Check sectors
    for (ui32 i = 0; i < TotalSectorCount; ++i) {
        const TString expectedData(SectorSize, sectorsFill[i]);
        const TString realData = LoadSectorAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TServerTest,
    testing::Values(
        NProto::EEncryptionMode::NO_ENCRYPTION,
        NProto::EEncryptionMode::ENCRYPTION_AES_XTS));

}   // namespace NCloud::NBlockStore::NVHostServer
