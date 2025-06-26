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
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/tempfile.h>

#include <vhost/blockdev.h>

#include <span>

IOutputStream& operator<<(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EEncryptionMode mode)
{
    switch (mode) {
        case NCloud::NBlockStore::NProto::NO_ENCRYPTION:
            out << "NO_ENCRYPTION";
            return out;
        case NCloud::NBlockStore::NProto::ENCRYPTION_AES_XTS:
            out << "ENCRYPTION_AES_XTS";
            return out;
        case NCloud::NBlockStore::NProto::ENCRYPTION_AT_REST:
            out << "ENCRYPTION_AT_REST";
            return out;
        default:
            break;
    }
    Y_DEBUG_ABORT_UNLESS(false);
    return out;
}

namespace NCloud::NBlockStore::NVHostServer {


using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey("1234567890123456789012345678901");

using TBlocksPerRequest = size_t;
using TBlockSize = ui32;
using TUnaligned = bool;
using TTestParams = std::
    tuple<NProto::EEncryptionMode, TBlocksPerRequest, TBlockSize, TUnaligned>;

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

    NProto::TError Encrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(blockIndex);
        switch (Behaviour) {
            case EBehaviour::ReturnError: {
                return MakeError(E_FAIL, "Oh no!");
            }
            case EBehaviour::EncryptToAllZeroes:{
                memset(const_cast<char*>(dst.Data()), 0, dst.Size());
                return {};
            }
        }
    }

    NProto::TError Decrypt(
        TBlockDataRef src,
        TBlockDataRef dst,
        ui64 blockIndex) override
    {
        Y_UNUSED(src);
        Y_UNUSED(dst);
        Y_UNUSED(blockIndex);
        return MakeError(E_NOT_IMPLEMENTED);
    }
};

class TServerTest
    : public testing::TestWithParam<TTestParams>
{
public:
    static constexpr ui32 QueueCount = 8;
    static constexpr ui32 QueueIndex = 4;
    static constexpr ui64 ChunkCount = 3;
    static constexpr ui64 ChunkByteCount = 128_KB;
    static constexpr ui64 TotalByteCount = ChunkByteCount * ChunkCount;
    static constexpr ui64 SectorSize = VHD_SECTOR_SIZE;
    static constexpr ui64 TotalSectorCount = TotalByteCount / SectorSize;

    static constexpr i64 HeaderSize = 4_KB;
    static constexpr i64 PaddingSize = 1_KB;

    const NProto::EEncryptionMode EncryptionMode = std::get<0>(GetParam());
    const size_t BlocksPerRequest = std::get<1>(GetParam());
    const ui32 BlockSize = std::get<2>(GetParam());
    const ui64 BlocksPerChunk = ChunkByteCount / BlockSize;
    const ui32 SectorsPerBlock = BlockSize / SectorSize;
    const size_t SectorsPerRequest = SectorsPerBlock * BlocksPerRequest;
    const ui64 TotalBlockCount = TotalByteCount / BlockSize;
    const ui64 RequestSize = BlockSize * BlocksPerRequest;
    const bool Unaligned = std::get<3>(GetParam());
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
        .BlockSize = BlockSize,
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

        TVector<ui64> offsets(ChunkCount);
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
        ASSERT_GE(BlockSize, SectorSize);
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

    TCompleteStats GetStats(ui64 expectedCompleted) const
    {
        return GetStats(
            [expectedCompleted](const TCompleteStats& stats)
            { return stats.SimpleStats.Completed == expectedCompleted; });
    }

    TCompleteStats GetStats(
        std::function<bool(const TCompleteStats&)> func) const
    {
        // Without I/O, stats are synced every second and only if there is a
        // pending GetStats call. The first call to GetStats might not bring the
        // latest stats; therefore, you need at least two calls so that the AIO
        // backend will sync the stats.

        TSimpleStats prevStats;
        TCompleteStats stats;
        for (int i = 0; i != 5; ++i) {
            // Save critical events from previous attempt.
            auto critEvents = std::move(stats.CriticalEvents);

            stats = Server->GetStats(prevStats);

            // Combine critical events from previous and current attempt.
            if (critEvents) {
                for (auto& critEvent: stats.CriticalEvents) {
                    critEvents.push_back(std::move(critEvent));
                }
                stats.CriticalEvents = std::move(critEvents);
            }

            // Check that the current attempt to get statistics has brought
            // everything we need.
            if (func(stats)) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        return stats;
    }

    TString LoadRawBlock(ui64 block)
    {
        const ui64 chunkIndex = block / BlocksPerChunk;
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
            chunkLayout.Offset + (block % BlocksPerChunk) * BlockSize;

        TString buffer;
        buffer.resize(BlockSize);
        file.Seek(fileOffset, SeekDir::sSet);
        file.Load(&buffer[0], buffer.size());

        return buffer;
    }

    TString LoadBlockAndDecrypt(ui64 block)
    {
        TString buffer = LoadRawBlock(block);
        if (Encryptor && !IsAllZeroes(buffer.data(), buffer.size())) {
            Y_DEBUG_ABORT_UNLESS(buffer.size() % SectorSize == 0);
            for (ui32 i = 0; i < SectorsPerBlock; ++i) {
                Encryptor->Decrypt(
                    TBlockDataRef(buffer.data() + (i * SectorSize), SectorSize),
                    TBlockDataRef(buffer.data() + (i * SectorSize), SectorSize),
                    i + block * SectorsPerBlock);
            }
        }
        return buffer;
    }

    bool SaveRawBlock(ui64 block, const TString& data)
    {
        const ui64 chunkIndex = block / BlocksPerChunk;
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
            chunkLayout.Offset + (block % BlocksPerChunk) * BlockSize;

        if (data.size() != BlockSize) {
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

    auto getFillChar = [](size_t block) -> ui8
    {
        const TString allowedChars{
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"};
        return allowedChars[block % allowedChars.size()];
    };
    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                getFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    };

    // write data
    size_t writesCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer = Memory.Allocate(
            RequestSize,
            Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
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
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
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
    for (ui64 i = 0; i != TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, getFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writesCount + readsCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readsCount, read.Count);
        EXPECT_EQ(readsCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readsCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writesCount, write.Count);
        EXPECT_EQ(writesCount * RequestSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(
            Unaligned ? writesCount - splittedWrites : 0,
            write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldWriteToSplitDevices)
{
    // The test does not depend on this value.
    if (BlocksPerRequest != 1) {
        return;
    }

    StartServerWithSplitDevices();

    TVector<char> blocksFill(TotalBlockCount);

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
            // Skip blocks data
            file.Seek(ChunkByteCount, SeekDir::sCur);

            // Check padding
            if (i + 1 != ChunkCount) {
                TString realPadding(PaddingSize, 0);
                file.Load(const_cast<char*>(realPadding.data()), PaddingSize);
                EXPECT_EQ(paddingData, realPadding);
            }
        }

        // Check blocks
        for (ui32 i = 0; i < TotalBlockCount; ++i) {
            const TString expectedData(BlockSize, blocksFill[i]);
            const TString realData = LoadBlockAndDecrypt(i);
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
            Memory.Allocate(ChunkByteCount, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        const ui64 startBlock = ChunkByteCount / 2 / BlockSize;
        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
            startBlock * SectorsPerBlock;

        for (ui32 i = 0; i < BlocksPerChunk; ++i) {
            const char blockFill = 'A' + (startBlock + i) % 26;
            blocksFill[startBlock + i] = blockFill;
            std::memset(buffer.data() + i * BlockSize, blockFill, BlockSize);
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

    TVector<char> blocksFill(TotalBlockCount);
    const ui32 requestCount = 10;

    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;

    for (ui64 i = 0; i != requestCount; ++i) {
        ui64 startBlock = (i * BlocksPerRequest) % TotalBlockCount;
        std::span hdr = Hdr(
            Memory,
            {.type = VIRTIO_BLK_T_OUT, .sector = startBlock * SectorsPerBlock});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        EXPECT_EQ(RequestSize, writeBuffer.size());
        EXPECT_EQ(1u, status.size());

        ui8 bufferFill = 'A' + i % 26;
        memset(writeBuffer.data(), bufferFill, writeBuffer.size_bytes());
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            blocksFill[startBlock + j] = bufferFill;
        }

        statuses.push_back(status);
        futures.push_back(
            Client.WriteAsync(i % QueueCount, {hdr, writeBuffer}, {status}));
    }

    WaitAll(futures).Wait();

    const auto completeStats = GetStats(requestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(requestCount, stats.Submitted);
    EXPECT_EQ(requestCount, stats.Completed);
    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);

    for (ui32 i = 0; i != requestCount; ++i) {
        const ui32 len = futures[i].GetValueSync();

        EXPECT_EQ(statuses[i].size(), len);
        EXPECT_EQ(char(0), statuses[i][0]);
    }

    // Check blocks
    for (ui32 i = 0; i < TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, blocksFill[i]);
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }
}

TEST_P(TServerTest, ShouldWriteMultipleAndReadByOne)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; i++) {
        const TString pattern = MakeRandomPattern(writeBuffer.size_bytes());

        // write BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
        writeCount++;

        // read one block at a time
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                (i + j) * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(readStatus.size() + readBuffer.size(), len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);
            std::string_view readData(readBuffer.data(), readBuffer.size());
            std::string_view expectedData(
                pattern.data() + BlockSize * j,
                BlockSize);
            EXPECT_EQ(expectedData, readData);
            ++readCount;
        }
    }

    // validate stats
    const auto splittedReads = 0;
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * BlockSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * BlocksPerRequest * BlockSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldWriteByOneAndReadMultiple)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;

    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        const TString pattern = MakeRandomPattern(RequestSize);

        // write one block at a time
        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                (i + j) * SectorsPerBlock;
            memcpy(
                writeBuffer.data(),
                pattern.data() + BlockSize * j,
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

        // read BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
            i * SectorsPerBlock;
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
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = 0;
    const auto expectedTotalRequestCount =
        writeCount + readCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readCount, read.Count);
        EXPECT_EQ(readCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writeCount, write.Count);
        EXPECT_EQ(writeCount * BlockSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(Unaligned ? writeCount - splittedWrites : 0, write.Unaligned);
    }
}

TEST_P(TServerTest, ShouldHandleWrongSectorIndex)
{
    StartServer();

    {
        std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
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
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
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
    const auto completeStats = GetStats(0);
    const auto& stats = completeStats.SimpleStats;

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
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);
    const auto pattern = TString(writeBuffer.size_bytes(), 0);

    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        // write BlocksPerRequest at once
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        memcpy(writeBuffer.data(), pattern.data(), writeBuffer.size_bytes());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(writeStatus.size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);

        for (size_t j = 0; j < BlocksPerRequest; ++j) {
            const auto rawBlock = LoadRawBlock(i + j);
            const bool shouldReadZeroes =
                EncryptionMode == NProto::EEncryptionMode::NO_ENCRYPTION;
            EXPECT_EQ(
                shouldReadZeroes,
                IsAllZeroes(rawBlock.data(), rawBlock.size()));
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
        TString randomBlock = MakeRandomPattern(BlockSize);
        for (size_t i = 0; i < TotalBlockCount; ++i) {
            ASSERT_TRUE(SaveRawBlock(i, randomBlock));
        }
    }

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);

    size_t readCount = 0;
    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        // check writes
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                i * SectorsPerBlock;
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
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                i * SectorsPerBlock;
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
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto completeStats = GetStats(readCount + splittedReads);
    const auto& stats = completeStats.SimpleStats;

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
    std::span writeBuffer =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    size_t writeCount = 0;
    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
        reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
            i * SectorsPerBlock;
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr_w, writeBuffer}, {writeStatus});
        const ui32 len = writeOp.GetValueSync();
        EXPECT_EQ(1u, len);
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, writeStatus[0]);
        ++writeCount;
    }

    // validate stats
    const auto completeStats = GetStats(
        [](const TCompleteStats& stats) {
            return stats.CriticalEvents.size() != 0 &&
                   stats.SimpleStats.EncryptorErrors != 0;
        });
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(0u, stats.Completed);
    EXPECT_EQ(0u, stats.Dequeued);
    EXPECT_EQ(0u, stats.Submitted);
    EXPECT_EQ(writeCount, stats.EncryptorErrors);

    // validate crit events
    EXPECT_EQ(writeCount, completeStats.CriticalEvents.size());
    for (const auto& [sensorName, message]: completeStats.CriticalEvents) {
        EXPECT_EQ("EncryptorGeneratedZeroBlock", sensorName);
        EXPECT_EQ(
            true,
            message.StartsWith("Encryptor has generated a zero block #"));
    }
}

TEST_P(TServerTest, ShouldReadAndWriteWithPteFlushEnabled)
{
    Options.PteFlushByteThreshold = 4096;
    StartServer();

    auto getFillChar = [](size_t block) -> ui8
    {
        const TString allowedChars{
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"};
        return allowedChars[block % allowedChars.size()];
    };
    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                getFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    };

    // write data
    size_t writesCount = 0;
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
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
        std::span readBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest; ++i) {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
                i * SectorsPerBlock;
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
    for (ui64 i = 0; i < TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, getFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }

    // validate stats
    const auto splittedReads = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto splittedWrites = (BlocksPerRequest - 1) * (ChunkCount - 1);
    const auto expectedTotalRequestCount =
        writesCount + readsCount + splittedReads + splittedWrites;
    const auto completeStats = GetStats(expectedTotalRequestCount);
    const auto& stats = completeStats.SimpleStats;

    EXPECT_EQ(0u, stats.CompFailed);
    EXPECT_EQ(0u, stats.SubFailed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Completed);
    EXPECT_EQ(expectedTotalRequestCount, stats.Dequeued);
    EXPECT_EQ(expectedTotalRequestCount, stats.Submitted);
    {
        const auto& read = stats.Requests[0];
        EXPECT_EQ(readsCount, read.Count);
        EXPECT_EQ(readsCount * RequestSize, read.Bytes);
        EXPECT_EQ(0u, read.Errors);
        EXPECT_EQ(Unaligned ? readsCount - splittedReads : 0, read.Unaligned);
    }
    {
        const auto& write = stats.Requests[1];
        EXPECT_EQ(writesCount, write.Count);
        EXPECT_EQ(writesCount * RequestSize, write.Bytes);
        EXPECT_EQ(0u, write.Errors);
        EXPECT_EQ(
            Unaligned ? writesCount - splittedWrites : 0,
            write.Unaligned);
    }
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TServerTest,
    testing::Combine(
        testing::Values(
            NProto::EEncryptionMode::NO_ENCRYPTION,
            NProto::EEncryptionMode::ENCRYPTION_AES_XTS),
        testing::Values(1, 2, 4),       // Blocks per request
        testing::Values(512_B, 4_KB),   // Block size
        testing::Values(true, false)    // Unaligned
        ),
    [](const testing::TestParamInfo<TTestParams>& info)
    {
        const auto name =
            TStringBuilder()
            << std::get<0>(info.param) << "_bc" << std::get<1>(info.param)
            << "_bs" << std::get<2>(info.param) << "_"
            << (std::get<3>(info.param) ? "unaligned" : "aligned");
        return std::string(name);
    });

}   // namespace NCloud::NBlockStore::NVHostServer
