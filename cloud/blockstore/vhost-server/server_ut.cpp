#include "server.h"

#include "backend_aio.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryptor.h>
#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/system/file.h>
#include <util/system/tempfile.h>

#include <vhost/blockdev.h>

#include <span>

namespace NCloud::NBlockStore::NVHostServer {

using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey("1234567890123456789012345678901");

using TSectorsInRequest = size_t;
using TUnaligned = bool;
using TTestParams =
    std::tuple<NProto::EEncryptionMode, TSectorsInRequest, TUnaligned>;

class TMockEncryptor: public IEncryptor
{
public:
    enum class EBehaviour
    {
        ReturnError,
        EncryptToAllZeroes,
    };

private:
    EBehaviour Behaviour;

public:
    explicit TMockEncryptor(EBehaviour behaviour)
        : Behaviour(behaviour)
    {}

    bool Encrypt(
        const TBlockDataRef& src,
        const TBlockDataRef& dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(blockIndex);
        switch (Behaviour) {
            case EBehaviour::ReturnError: {
                return false;
            }
            case EBehaviour::EncryptToAllZeroes:{
                memset(const_cast<char*>(dst.Data()), 0, dst.Size());
                return true;
            }
        }
    }

    bool Decrypt(
        const TBlockDataRef& src,
        const TBlockDataRef& dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(dst);
        Y_UNUSED(blockIndex);
        return false;
    }
};

class TServerTest
    : public testing::TestWithParam<TTestParams>
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

    const NProto::EEncryptionMode EncryptionMode = std::get<0>(GetParam());
    const size_t SectorsPerRequest = std::get<1>(GetParam());
    const bool Unaligned =  std::get<2>(GetParam());
    const TString SocketPath = "server_ut.vhost";
    const TString Serial = "server_ut";

    NCloud::ILoggingServicePtr Logging;
    std::shared_ptr<IServer> Server;
    TVector<TTempFileHandle> Files;
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
        if (EncryptionMode == NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
            Encryptor =
                CreateAesXtsEncryptor(TEncryptionKey(DefaultEncryptionKey));
        }
    }

    void StartServer()
    {
        Server = CreateServer(Logging, CreateAioBackend(Encryptor, Logging));

        Options.Layout.reserve(ChunkCount);
        Files.reserve(ChunkCount);
        for (ui32 i = 0; i != ChunkCount; ++i) {
            auto& file = Files.emplace_back(MakeTempName());

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
        Server = CreateServer(Logging, CreateAioBackend(Encryptor, Logging));

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

        auto& file = Files.emplace_back(MakeTempName());

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
        if (Server) {
            Client.DeInit();
            Server->Stop();
            Server.reset();
        }
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

    TString LoadRawSector(ui64 sector)
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

        return buffer;
    }

    TString LoadSectorAndDecrypt(ui64 sector)
    {
        TString buffer = LoadRawSector(sector);
        if (Encryptor && !IsAllZeroes(buffer.data(), buffer.size())) {
            Encryptor->Decrypt(
                TBlockDataRef(buffer.data(), buffer.size()),
                TBlockDataRef(buffer.data(), buffer.size()),
                sector);
        }
        return buffer;
    }

    bool SaveRawSector(ui64 sector, const TString& data)
    {
        const ui64 sectorsPerChunk = ChunkByteCount / SectorSize;
        const ui64 chunkIndex = sector/ sectorsPerChunk;
        const auto& chunkLayout = Options.Layout[chunkIndex];

        auto it = FindIf(
            Files,
            [&](const TFile& f)
            { return f.GetName() == chunkLayout.DevicePath; });
        if (it == Files.end()) {
            return false;
        }
        auto & file = *it;

        const ui64 fileOffset =
            chunkLayout.Offset + (sector % sectorsPerChunk) * SectorSize;

        if (data.size() != SectorSize) {
            return false;
        }
        file.Seek(fileOffset, SeekDir::sSet);
        file.Write(&data[0], data.size());

        return true;
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

TString MakeRandomPattern(size_t size) {
    TString result;
    result.resize(size);

    for (size_t i = 0; i < size; ++i) {
        result[i] = RandomNumber<ui8>(255);
    }
    return result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST_P(TServerTest, ShouldGetDeviceID)
{
    StartServer();

    std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_GET_ID});
    std::span serial = Memory.Allocate(VIRTIO_BLK_DISKID_LENGTH);
    std::span status = Memory.Allocate(1);

    const ui32 len =
        Client.WriteAsync(QueueIndex, {hdr}, {serial, status}).GetValueSync();

    EXPECT_EQ(serial.size() + status.size(), len);
    EXPECT_EQ(Serial, TStringBuf(serial.data()));
    EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
}

TEST_P(TServerTest, ShouldReadAndWrite)
{
    StartServer();

    auto getFillChar = [](size_t sector) -> ui8
    {
        return ('A' + sector) % 256;
    };
    auto makePatten = [&](size_t startSector) -> TString
    {
        TString result;
        result.resize(SectorSize * SectorsPerRequest);
        for (size_t i = 0; i < SectorsPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + SectorSize * i,
                getFillChar(startSector + i),
                SectorSize);
        }
        return result;
    };

    // write data
    size_t writesCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer = Memory.Allocate(
            SectorSize * SectorsPerRequest,
            Unaligned ? 1 : SectorSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector = i;
            TString expectedData = makePatten(i);
            memcpy(
                writeBuffer.data(),
                expectedData.data(),
                expectedData.size());
            auto writeOp =
                Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
            EXPECT_EQ(status.size(), writeOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
            ++writesCount;
        }
    }

    // read data
    size_t readsCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
        std::span readBuffer = Memory.Allocate(
            SectorSize * SectorsPerRequest,
            Unaligned ? 1 : SectorSize);
        std::span status = Memory.Allocate(1);
        for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector = i;
            auto readOp =
                Client.WriteAsync(QueueIndex, {hdr}, {readBuffer, status});
            EXPECT_EQ(status.size() + readBuffer.size(), readOp.GetValueSync());
            EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);

            TString readData(readBuffer.data(), readBuffer.size());
            EXPECT_EQ(makePatten(i), readData);
            ++readsCount;
        }
    }

    // validate storage
    for (ui64 i = 0; i != TotalSectorCount; ++i) {
        const TString expectedData(SectorSize, getFillChar(i));
        const TString realData = LoadSectorAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    // validate stats
    const auto splittedReads = (SectorsPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = (SectorsPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writesCount + readsCount + splittedReads + splittedWrites;
    const auto stats = GetStats(expectedTotalRequestCount);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readsCount, read.Count);
        EXPECT_EQ(readsCount * SectorsPerRequest * SectorSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readsCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writesCount, write.Count);
        EXPECT_EQ(writesCount * SectorsPerRequest * SectorSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(
            Unaligned ? writesCount - splittedWrites : 0,
            write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldWriteToSplitDevices)
{
    if (SectorsPerRequest != 1) {
        return;
    }
    StartServerWithSplitDevices();

    std::vector<ui8> sectorsFill(TotalSectorCount);

    // layout: [ H | --- D --- | P | --- D --- | P | --- D --- | ... ]
    auto verifyLayoutAndData = [&]()
    {
        char header[HeaderSize];
        TFile file{Files[0].GetName(), EOpenModeFlag::OpenAlways};

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
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span buffer =
            Memory.Allocate(ChunkByteCount, Unaligned ? 1 : SectorSize);
        std::span status = Memory.Allocate(1);

        const ui64 startSector = ChunkByteCount / 2 / SectorSize;
        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector = startSector;

        for (ui32 i = 0; i < ChunkByteCount / SectorSize; ++i) {
            const ui8 sectorFill = (startSector + i) % 256;
            sectorsFill[startSector + i] = sectorFill;
            std::memset(buffer.data() + i * SectorSize, sectorFill, SectorSize);
        }

        auto writeOp = Client.WriteAsync(QueueIndex, {hdr, buffer}, {status});
        const ui32 len = writeOp.GetValueSync();

        EXPECT_EQ(status.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);
    }

    // verification after cross-chunk write
    verifyLayoutAndData();
}

TEST_P(TServerTest, ShouldHandleMultipleQueues)
{
    if (!Unaligned) {
        // TODO fix data-race
        return;
    }
    StartServer();

    std::vector<ui8> sectorsFill(TotalSectorCount);
    const ui32 requestCount = 10;

    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;

    for (ui64 i = 0; i != requestCount; ++i) {
        ui64 startSector = (i * SectorsPerRequest) % TotalSectorCount;
        std::span hdr =
            Hdr(Memory, {.type = VIRTIO_BLK_T_OUT, .sector = startSector});
        std::span writeBuffer = Memory.Allocate(
            SectorSize * SectorsPerRequest,
            Unaligned ? 1 : SectorSize);
        std::span status = Memory.Allocate(1);

        EXPECT_EQ(SectorSize * SectorsPerRequest, writeBuffer.size());
        EXPECT_EQ(1u, status.size());

        ui8 sectorFill = 'A' + i % 26;
        memset(writeBuffer.data(), sectorFill, writeBuffer.size_bytes());
        for (size_t j = 0; j < SectorsPerRequest; ++j) {
            sectorsFill[startSector + j] = sectorFill;
        }

        statuses.push_back(status);
        futures.push_back(
            Client.WriteAsync(i % QueueCount, {hdr, writeBuffer}, {status}));
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

TEST_P(TServerTest, ShouldWriteMultipleAndReadByOne)
{
    if (SectorsPerRequest == 1) {
        return;
    }
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(SectorSize, Unaligned ? 1 : SectorSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
        const TString pattern = MakeRandomPattern(writeBuffer.size_bytes());

        // write SectorsPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector = i;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
        writeCount++;

        // read one sector at a time
        for (size_t j = 0; j < SectorsPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector = i + j;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);
            std::string_view readData(readBuffer.data(), readBuffer.size());
            std::string_view expectedData(
                pattern.data() + SectorSize * j,
                SectorSize);
            EXPECT_EQ(expectedData, readData);
            ++readCount;
        }
    }

    // validate stats
    const auto splittedReads = 0;
    const auto splittedWrites = (SectorsPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto stats = GetStats(expectedTotalRequestCount);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * SectorSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * SectorsPerRequest * SectorSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldWriteByOneAndReadMultiple)
{
    if (SectorsPerRequest == 1) {
        return;
    }
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(SectorSize, Unaligned ? 1 : SectorSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;

    for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
        const TString pattern =
            MakeRandomPattern(SectorSize * SectorsPerRequest);

        // write one sectors at a time
        for (size_t j = 0; j < SectorsPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector = i + j;
            memcpy(
                writeBuffer.data(),
                pattern.data() + SectorSize * j,
                writeBuffer.size_bytes());
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(writeStatus.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
            ++writeCount;
        }

        // read SectorsPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector = i;
        auto read_result =
            Client.WriteAsync(QueueIndex, {hdr_r}, {readBuffer, readStatus});
        const ui32 len = read_result.GetValueSync();
        EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);
        std::string_view readData(readBuffer.data(), readBuffer.size());
        std::string_view expectedData(pattern.data(), readBuffer.size());
        EXPECT_EQ(expectedData, readData);
        ++readCount;
    }

    // validate stats
    const auto splittedReads = (SectorsPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = 0;
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto stats = GetStats(expectedTotalRequestCount);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * SectorsPerRequest * SectorSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * SectorSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldHandleWrongSectorIndex)
{
    StartServer();

    {
        std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer = Memory.Allocate(
            SectorSize * SectorsPerRequest,
            Unaligned ? 1 : SectorSize);
        std::span writeStatus = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            TotalSectorCount;
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
    }

    {
        std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
        std::span readBuffer = Memory.Allocate(
            SectorSize * SectorsPerRequest,
            Unaligned ? 1 : SectorSize);
        std::span readStatus = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
            TotalSectorCount;
        auto read_result =
            Client.WriteAsync(QueueIndex, {hdr_r}, {readBuffer, readStatus});
        const ui32 len = read_result.GetValueSync();
        EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, readStatus[0]);
    }
    // validate stats
    const auto stats = GetStats(0);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(0u, stats.Completed);
    EXPECT_EQ(0u, stats.Dequeued);
    EXPECT_EQ(0u, stats.Submitted);
}

TEST_P(TServerTest, ShouldStoreEncryptedZeroes)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span writeStatus = Memory.Allocate(1);
    const auto pattern = TString(writeBuffer.size_bytes(), 0);

    for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
        // write SectorsPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector = i;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);

        for (size_t j = 0; j < SectorsPerRequest; ++j) {
            const auto rawSector = LoadRawSector(i + j);
            const bool shouldReadZeroes =
                EncryptionMode == NProto::EEncryptionMode::NO_ENCRYPTION;
            EXPECT_EQ(
                shouldReadZeroes,
                IsAllZeroes(rawSector.data(), rawSector.size()));
        }
    }
}

TEST_P(TServerTest, ShouldStatEncryptorErrors)
{
    if (EncryptionMode != NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
        return;
    }

    Encryptor = std::make_shared<TMockEncryptor>(
        TMockEncryptor::EBehaviour::ReturnError);
    StartServer();

    // Fill storage with random data
    {
        auto randomSector = MakeRandomPattern(SectorSize);
        for (size_t i = 0; i < TotalSectorCount; ++i) {
            SaveRawSector(i, randomSector);
        }
    }

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
        // check writes
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector = i;
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(1u, len);
            EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
            ++writeCount;
        }

        // check reads
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector = i;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_IOERR, readStatus[0]);
            ++readCount;
        }
    }

    // validate stats
    const auto splittedReads = (SectorsPerRequest - 1) * (ChunkCount - 1);
    const auto stats = GetStats(readCount + splittedReads);

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(readCount + splittedReads, stats.Completed);
    EXPECT_EQ(readCount + splittedReads, stats.Dequeued);
    EXPECT_EQ(readCount + splittedReads, stats.Submitted);
    EXPECT_EQ(readCount + writeCount, stats.EncryptorErrors);
}

TEST_P(TServerTest, ShouldStatAllZeroesBlocks)
{
    if (EncryptionMode != NProto::EEncryptionMode::ENCRYPTION_AES_XTS) {
        return;
    }

    Encryptor = std::make_shared<TMockEncryptor>(
        TMockEncryptor::EBehaviour::EncryptToAllZeroes);
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer = Memory.Allocate(
        SectorSize * SectorsPerRequest,
        Unaligned ? 1 : SectorSize);
    std::span writeStatus = Memory.Allocate(1);

    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalSectorCount - SectorsPerRequest; ++i) {
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector = i;
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(1u, len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
        ++writeCount;
    }

    // validate stats
    {
        const auto stats = GetStats(0);

        EXPECT_EQ(0u, stats.CompFailed);
        EXPECT_EQ(0u, stats.SubFailed);
        EXPECT_EQ(0u, stats.Completed);
        EXPECT_EQ(0u, stats.Dequeued);
        EXPECT_EQ(0u, stats.Submitted);
        EXPECT_EQ(writeCount, stats.EncryptorErrors);
    }

    // validate crit events
    {
        constexpr ui64 cyclesPerMs = 2000000;
        TSimpleStats prevStats;
        TSimpleStats curStats;
        TStringStream ss;
        DumpStats(curStats, prevStats, TDuration::Seconds(1), ss, cyclesPerMs);
        NJson::TJsonValue json;
        NJson::ReadJsonTree(ss.Str(), &json, true);

        ASSERT_EQ(true, json.Has("crit_events"));
        for (const auto& event: json["crit_events"].GetArray()) {
            const auto& name = event["name"].GetString();
            const auto& message = event["message"].GetString();
            EXPECT_EQ("EncryptorGeneratedZeroBlock", name);
            EXPECT_EQ(
                true,
                message.StartsWith("Encryptor has generated a zero block #"));
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    ValueParametrized,
    TServerTest,
    testing::Combine(
        testing::Values(
            NProto::EEncryptionMode::NO_ENCRYPTION,
            NProto::EEncryptionMode::ENCRYPTION_AES_XTS),
        testing::Values(1, 2, 4),      // Sectors per request
        testing::Values(true, false)   // Unaligned
        ));

}   // namespace NCloud::NBlockStore::NVHostServer
