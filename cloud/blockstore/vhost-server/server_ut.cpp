#include "server.h"

#include "backend.h"
#include "backend_aio.h"
#include "backend_null.h"
#include "backend_rdma.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/encryption/encryptor.h>
#include <cloud/blockstore/libs/service/storage_provider.h>
#include <cloud/blockstore/libs/service/storage_test.h>
#include <cloud/blockstore/libs/service_local/compound_storage.h>

#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>

#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/gtest/gtest.h>
#include <library/cpp/threading/future/subscription/wait_all.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/tempfile.h>
#include <util/system/thread.h>

#include <vhost/blockdev.h>

#include <atomic>
#include <exception>
#include <span>
#include <thread>

IOutputStream& operator<<(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EEncryptionMode mode)
{
    const auto& s = NCloud::NBlockStore::NProto::EEncryptionMode_Name(mode);
    Y_DEBUG_ABORT_UNLESS(s);
    out << s;

    return out;
}

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore::NVHostServer {


using NVHost::TMonotonicBufferResource;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui8 GetFillChar(size_t block)
{
    constexpr char AllowedChars[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    return AllowedChars[block % strlen(AllowedChars)];
}

////////////////////////////////////////////////////////////////////////////////

const TString DefaultEncryptionKey("1234567890123456789012345678901");

using TBlocksPerRequest = size_t;
using TBlockSize = ui32;
using TUnaligned = bool;
using TThreadCount = size_t;
using TTestParams = std::tuple<
    NProto::EEncryptionMode,
    TBlocksPerRequest,
    TBlockSize,
    TUnaligned,
    TThreadCount>;

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
    const size_t ThreadCount = std::get<4>(GetParam());
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
    IBackendPtr Backend;
    std::atomic<ui64> NowNs = 0;
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

    void StartServer(bool addNonExistingDevice = false)
    {
        Backend = CreateAioBackend(
            Encryptor, Logging, ThreadCount, [this] { return NowNs.load(); });
        Server = CreateServer(Logging, Backend);

        Options.Layout.reserve(ChunkCount);
        Files.reserve(ChunkCount);

        if (addNonExistingDevice) {
            Options.Layout.push_back(
                {.DevicePath = "NonExistingDevice",
                 .ByteCount = ChunkByteCount});
        }

        for (ui32 i = 0; i != ChunkCount - addNonExistingDevice; ++i) {
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
        Backend = CreateAioBackend(
            Encryptor, Logging, ThreadCount, [this] { return NowNs.load(); });
        Server = CreateServer(Logging, Backend);

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
        Backend.reset();
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

    TString MakePattern(size_t startBlock) const
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
                BlockSize);
        }
        return result;
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
            TString expectedData = MakePattern(i);
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
            EXPECT_EQ(MakePattern(i), readData);
            ++readsCount;
        }
    }

    // validate storage
    for (ui64 i = 0; i != TotalBlockCount; ++i) {
        const TString expectedData(BlockSize, GetFillChar(i));
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

TEST_P(TServerTest, ShouldResponseWithEIOOnRequestsToNonExistingDevice)
{
    StartServer(true);

    // write data
    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        reinterpret_cast<virtio_blk_req_hdr*>(hdr.data())->sector =
            0 * SectorsPerBlock;
        TString expectedData = MakePattern(0);
        memcpy(writeBuffer.data(), expectedData.data(), expectedData.size());
        auto writeOp =
            Client.WriteAsync(QueueIndex, {hdr, writeBuffer}, {status});
        EXPECT_EQ(status.size(), writeOp.GetValueSync());
        EXPECT_EQ(VIRTIO_BLK_S_IOERR, status[0]);
    }

    const auto depth = Backend->GetIoDepthStats();
    ASSERT_TRUE(depth);
    EXPECT_EQ(0u, depth->Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_TRUE(depth->Continuous);
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

TEST_P(TServerTest, ShouldReadAndWriteMultipleBuffers)
{
    StartServer();

    std::span hdr_w = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
    std::span writeBuffer1 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeBuffer2 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span writeStatus = Memory.Allocate(1);

    std::span hdr_r = Hdr(Memory, {.type = VIRTIO_BLK_T_IN});
    std::span readBuffer1 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readBuffer2 =
        Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
    std::span readStatus = Memory.Allocate(1);


    for (ui64 i = 0; i <= TotalBlockCount - BlocksPerRequest * 2; ++i) {
        const TString pattern1 = MakeRandomPattern(RequestSize);
        const TString pattern2 = MakeRandomPattern(RequestSize);

        // check writes
        {
            memcpy(writeBuffer1.data(), pattern1.data(), writeBuffer1.size_bytes());
            memcpy(writeBuffer2.data(), pattern2.data(), writeBuffer2.size_bytes());

            reinterpret_cast<virtio_blk_req_hdr*>(hdr_w.data())->sector =
                i * SectorsPerBlock;
            auto writeOp = Client.WriteAsync(
                QueueIndex,
                {hdr_w, writeBuffer1, writeBuffer2},
                {writeStatus});
            const ui32 len = writeOp.GetValueSync();
            EXPECT_EQ(1u, len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, writeStatus[0]);
        }

        // check reads
        {
            reinterpret_cast<virtio_blk_req_hdr*>(hdr_r.data())->sector =
                i * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdr_r},
                {readBuffer1, readBuffer2, readStatus});
            const ui32 len = readOp.GetValueSync();
            EXPECT_EQ(
                readStatus.size() + readBuffer1.size() + readBuffer2.size(),
                len);
            EXPECT_EQ(VIRTIO_BLK_S_OK, readStatus[0]);

            TString readData1(readBuffer1.data(), readBuffer1.size());
            EXPECT_EQ(pattern1, readData1);
            TString readData2(readBuffer2.data(), readBuffer2.size());
            EXPECT_EQ(pattern2, readData2);
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

    const auto depth = Backend->GetIoDepthStats();
    ASSERT_TRUE(depth);
    EXPECT_EQ(0u, depth->Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(0u, depth->Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_TRUE(depth->Continuous);
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

    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
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
        const TString expectedData(BlockSize, GetFillChar(i));
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
        testing::Values(true, false),   // Unaligned
        testing::Values(0, 2)),
    [](const testing::TestParamInfo<TTestParams>& info)
    {
        const auto name = TStringBuilder()
                          << std::get<0>(info.param) << "_bc"
                          << std::get<1>(info.param) << "_bs"
                          << std::get<2>(info.param) << "_"
                          << (std::get<3>(info.param) ? "unaligned" : "aligned")
                          << "_tc" << std::get<4>(info.param);
        return std::string(name);
    });

class TSlowEncryptor: public IEncryptor
{
private:
    std::atomic<ui64> SleepTimeMillis;
    std::atomic<ui64> TotalSleepTimeMillis;

    TAdaptiveLock ThreadIdsLock;
    THashSet<ui64> ThreadIds;

    IEncryptorPtr Encryptor;

public:
    explicit TSlowEncryptor(TDuration sleepTime, IEncryptorPtr encryptor)
        : SleepTimeMillis(sleepTime.MilliSeconds())
        , Encryptor(std::move(encryptor))
    {}

    NProto::TError
    Encrypt(TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        const auto sleepTime = SleepTimeMillis.load();
        Sleep(TDuration::MilliSeconds(sleepTime));
        TotalSleepTimeMillis += sleepTime;

        with_lock(ThreadIdsLock) {
            ThreadIds.insert(TThread::CurrentThreadId());
        }

        return Encryptor->Encrypt(src, dst, blockIndex);
    }

    NProto::TError
    Decrypt(TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        const auto sleepTime = SleepTimeMillis.load();
        Sleep(TDuration::MilliSeconds(sleepTime));
        TotalSleepTimeMillis += sleepTime;

        with_lock (ThreadIdsLock) {
            ThreadIds.insert(TThread::CurrentThreadId());
        }

        return Encryptor->Decrypt(src, dst, blockIndex);
    }

    TDuration GetTotalSleepTime() const
    {
        return TDuration::MilliSeconds(TotalSleepTimeMillis.load());
    }

    void SetSleepTime(TDuration sleepTime)
    {
        SleepTimeMillis.store(sleepTime.MilliSeconds());
    }

    THashSet<ui64> GetThreadIds() const
    {
        with_lock(ThreadIdsLock) {
            return ThreadIds;
        }
    }

    void ClearThreadIds()
    {
        with_lock (ThreadIdsLock) {
            ThreadIds.clear();
        }
    }
};

class TSlowEncryptorServerTest: public TServerTest
{
public:
    TSlowEncryptorServerTest()
    {
        Encryptor = std::make_shared<TSlowEncryptor>(
            TDuration::MilliSeconds(100),
            Encryptor);
    }

    TDuration GetTotalSleepTime() const
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        return slowEncryptor->GetTotalSleepTime();
    }

    void SetSleepTime(TDuration sleepTime)
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        slowEncryptor->SetSleepTime(sleepTime);
    }

    THashSet<ui64> GetThreadIds() const
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        return slowEncryptor->GetThreadIds();
    }

    void ClearThreadIds()
    {
        auto* slowEncryptor = dynamic_cast<TSlowEncryptor*>(Encryptor.get());
        EXPECT_TRUE(slowEncryptor != nullptr);
        slowEncryptor->ClearThreadIds();
    }
};

TEST_P(TSlowEncryptorServerTest, ShouldDecryptDataInParallel)
{
    StartServer();

    auto makePatten = [&](size_t startBlock) -> TString
    {
        TString result;
        result.resize(RequestSize);
        for (size_t i = 0; i < BlocksPerRequest; ++i) {
            memset(
                const_cast<char*>(result.data()) + BlockSize * i,
                GetFillChar(startBlock + i),
                BlockSize);
        }
        return result;
    };

    SetSleepTime(TDuration::Zero());

    {
        std::span hdr = Hdr(Memory, {.type = VIRTIO_BLK_T_OUT});
        std::span writeBuffer =
            Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);

        for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
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
        }
    }

    SetSleepTime(TDuration::MilliSeconds(100));
    ClearThreadIds();

    // read data
    {
        TVector<std::span<char>> hdrs;
        TVector<std::span<char>> readBuffers;
        TVector<std::span<char>> statuses;
        TVector<NThreading::TFuture<ui32>> readOpsResult;

        for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
            hdrs.push_back(Hdr(Memory, {.type = VIRTIO_BLK_T_IN}));
            readBuffers.push_back(
                Memory.Allocate(RequestSize, Unaligned ? 1 : BlockSize));
            statuses.push_back(Memory.Allocate(1));

            reinterpret_cast<virtio_blk_req_hdr*>(hdrs.back().data())->sector =
                i * SectorsPerBlock;
            auto readOp = Client.WriteAsync(
                QueueIndex,
                {hdrs.back()},
                {readBuffers.back(), statuses.back()});
            readOpsResult.push_back(readOp);
        }

        auto waitResult = NThreading::WaitAll(readOpsResult);
        waitResult.Wait();

        for (size_t i = 0; i < readOpsResult.size(); ++i) {
            auto readOpResult = readOpsResult[i].GetValueSync();
            EXPECT_EQ(statuses[i].size() + readBuffers[i].size(), readOpResult);
            EXPECT_EQ(VIRTIO_BLK_S_OK, statuses[i][0]);
            TString readData(readBuffers[i].data(), readBuffers[i].size());
            EXPECT_EQ(makePatten(i), readData);
        }

        auto threadIdsCalledDecrypt = GetThreadIds();
        EXPECT_EQ(threadIdsCalledDecrypt.size(), ThreadCount);
    }

    SetSleepTime(TDuration::Zero());

    // validate storage
    for (ui64 i = 0; i < ui64(ThreadCount); ++i) {
        const TString expectedData(BlockSize, GetFillChar(i));
        const TString realData = LoadBlockAndDecrypt(i);
        EXPECT_EQ(expectedData, realData);
    }
}

TEST_P(TSlowEncryptorServerTest, ShouldWaitForAllThreadPoolTasksBeforeStop)
{
    StartServer();

    const ui64 requestCount = ThreadCount * 5;

    auto makePattern = [&](size_t block) -> TString
    {
        return TString(BlockSize, GetFillChar(block));
    };

    // Write blocks directly to the underlying devices. Encrypt them first so
    // that when the server reads + decrypts, we get the original plaintext.
    {
        SetSleepTime(TDuration::Zero());

        TString encrypted;
        encrypted.resize(BlockSize);
        for (ui64 i = 0; i < requestCount; ++i) {
            const TString plaintext = makePattern(i);
            const auto err = Encryptor->Encrypt(
                TBlockDataRef(plaintext.data(), BlockSize),
                TBlockDataRef(encrypted.data(), BlockSize),
                i * SectorsPerBlock);
            ASSERT_FALSE(HasError(err));
            ASSERT_TRUE(SaveRawBlock(i, encrypted));
        }
    }

    SetSleepTime(TDuration::MilliSeconds(100));

    TVector<std::span<char>> readBuffers;
    TVector<std::span<char>> statuses;
    TVector<NThreading::TFuture<ui32>> futures;
    for (ui64 i = 0; i < requestCount; ++i) {
        std::span hdr = Hdr(
            Memory,
            {.type = VIRTIO_BLK_T_IN, .sector = i * SectorsPerBlock});
        std::span readBuffer =
            Memory.Allocate(BlockSize, Unaligned ? 1 : BlockSize);
        std::span status = Memory.Allocate(1);
        readBuffers.push_back(readBuffer);
        statuses.push_back(status);
        futures.push_back(
            Client.WriteAsync(i % QueueCount, {hdr}, {readBuffer, status}));
    }

    // Give AIO time to complete the reads and enqueue decryption tasks onto
    // the thread pool. Each decryption sleeps 100ms, so most tasks remain
    // queued at this point.
    Sleep(TDuration::MilliSeconds(200));

    Server->Stop();
    Server.reset();

    EXPECT_EQ(
        TDuration::MilliSeconds(requestCount * 100),
        GetTotalSleepTime());

    ASSERT_TRUE(
        NThreading::WaitAll(futures).Wait(TDuration::Seconds(5)))
        << "client did not observe all completed requests";

    for (size_t i = 0; i < futures.size(); ++i) {
        ASSERT_TRUE(futures[i].HasValue())
            << "future " << i << " did not resolve";
        const ui32 len = futures[i].GetValueSync();
        EXPECT_EQ(statuses[i].size() + readBuffers[i].size(), len);
        EXPECT_EQ(VIRTIO_BLK_S_OK, statuses[i][0]);
        TString readData(readBuffers[i].data(), readBuffers[i].size());
        EXPECT_EQ(makePattern(i), readData);
    }

    Client.DeInit();
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TSlowEncryptorServerTest,
    testing::Combine(
        testing::Values(NProto::EEncryptionMode::ENCRYPTION_AES_XTS),
        testing::Values(1, 2, 4),       // Blocks per request
        testing::Values(512_B),         // Block size
        testing::Values(true, false),   // Unaligned
        testing::Values(2, 4)           // Thread Count
        ),
    [](const testing::TestParamInfo<TTestParams>& info)
    {
        const auto name = TStringBuilder()
                          << std::get<0>(info.param) << "_bc"
                          << std::get<1>(info.param) << "_bs"
                          << std::get<2>(info.param) << "_"
                          << (std::get<3>(info.param) ? "unaligned" : "aligned")
                          << "_tc" << std::get<4>(info.param);
        return std::string(name);
    });

namespace {

class TBlockingDecryptEncryptor final: public IEncryptor
{
private:
    const IEncryptorPtr Impl;
    NThreading::TPromise<void> Entered = NThreading::NewPromise();
    NThreading::TPromise<void> Released = NThreading::NewPromise();

public:
    explicit TBlockingDecryptEncryptor(IEncryptorPtr impl)
        : Impl(std::move(impl))
    {}

    NProto::TError Encrypt(
        TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        return Impl->Encrypt(src, dst, blockIndex);
    }

    NProto::TError Decrypt(
        TBlockDataRef src, TBlockDataRef dst, ui64 blockIndex) override
    {
        Entered.TrySetValue();
        Released.GetFuture().Wait();
        return Impl->Decrypt(src, dst, blockIndex);
    }

    bool WaitEntered()
    {
        return Entered.GetFuture().Wait(TDuration::Seconds(5));
    }

    void Release()
    {
        Released.TrySetValue();
    }
};

class TIoDepthAioServerTest: public TServerTest
{
public:
    void CheckPendingRead(bool compound)
    {
        auto encryptor = std::make_shared<TBlockingDecryptEncryptor>(Encryptor);
        Encryptor = encryptor;
        StartServer();

        // Always release the worker before TearDown, including failed asserts.
        Y_DEFER
        {
            encryptor->Release();
        };

        const ui64 startBlock = compound ? BlocksPerChunk / 2 : 0;
        const ui64 bytes = compound ? ChunkByteCount : BlockSize;
        for (ui64 i = 0; i < bytes / BlockSize; ++i) {
            ASSERT_TRUE(SaveRawBlock(startBlock + i, TString(BlockSize, 'X')));
        }

        const auto hdr = Hdr(
            Memory,
            {.type = VIRTIO_BLK_T_IN, .sector = startBlock * SectorsPerBlock});
        const auto data = Memory.Allocate(bytes, BlockSize);
        const auto status = Memory.Allocate(1);
        auto future = Client.WriteAsync(QueueIndex, {hdr}, {data, status});

        ASSERT_TRUE(encryptor->WaitEntered());
        EXPECT_FALSE(future.HasValue());
        NowNs.store(2'000'000'000ULL);

        const auto pending = Backend->GetIoDepthStats();
        ASSERT_TRUE(pending);
        EXPECT_TRUE(pending->Continuous);
        EXPECT_EQ(1u, pending->Lanes[VHD_BDEV_READ].Current);
        EXPECT_EQ(2'000'000u, pending->Lanes[VHD_BDEV_READ].IntegralUs);
        EXPECT_EQ(0u, pending->Lanes[VHD_BDEV_WRITE].Current);

        encryptor->Release();
        ASSERT_TRUE(future.Wait(TDuration::Seconds(5)));
        EXPECT_EQ(bytes + status.size(), future.GetValueSync());
        EXPECT_EQ(VIRTIO_BLK_S_OK, status[0]);

        const auto completed = Backend->GetIoDepthStats();
        ASSERT_TRUE(completed);
        EXPECT_EQ(0u, completed->Lanes[VHD_BDEV_READ].Current);
        EXPECT_EQ(2'000'000u, completed->Lanes[VHD_BDEV_READ].IntegralUs);
        EXPECT_EQ(pending->Generation, completed->Generation);
        EXPECT_TRUE(completed->Continuous);

        Server->Stop();
        Server.reset();
        Client.DeInit();
        const auto drained = Backend->GetIoDepthStats();
        ASSERT_TRUE(drained);
        EXPECT_EQ(0u, drained->Lanes[VHD_BDEV_READ].Current);
        EXPECT_TRUE(drained->Continuous);
    }
};

class TTestRdmaStorageProvider final: public IStorageProvider
{
private:
    const IStoragePtr Storage;

public:
    explicit TTestRdmaStorageProvider(IStoragePtr storage)
        : Storage(std::move(storage))
    {}

    NThreading::TFuture<IStoragePtr> CreateStorage(
        const NProto::TVolume& volume,
        const TString& clientId, NProto::EVolumeAccessMode accessMode) override
    {
        Y_UNUSED(volume);
        Y_UNUSED(clientId);
        Y_UNUSED(accessMode);
        return NThreading::MakeFuture<IStoragePtr>(Storage);
    }
};

class TIoDepthRdmaServerTest: public testing::Test
{
public:
    const TString SocketPath = MakeTempName();
    const ui32 BlockSize = 4_KB;
    std::atomic<ui64> NowNs = 0;
    std::atomic<ui32> ReadAttempts = 0;
    ILoggingServicePtr Logging;
    std::shared_ptr<TTestStorage> Storage = std::make_shared<TTestStorage>();
    IBackendPtr Backend;
    std::shared_ptr<IServer> Server;
    NVHost::TClient Client{SocketPath, {.QueueCount = 1}};
    TMonotonicBufferResource Memory;

    NThreading::TPromise<NProto::TReadBlocksLocalResponse> ReadResponse =
        NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
    NThreading::TPromise<NProto::TReadBlocksLocalResponse> RetryResponse =
        NThreading::NewPromise<NProto::TReadBlocksLocalResponse>();
    NThreading::TPromise<NProto::TWriteBlocksLocalResponse> WriteResponse =
        NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
    NThreading::TPromise<void> ReadArrived = NThreading::NewPromise();
    NThreading::TPromise<void> RetryArrived = NThreading::NewPromise();
    NThreading::TPromise<void> WriteArrived = NThreading::NewPromise();

    void SetUp() override
    {
        Logging =
            CreateLoggingService("console", {.FiltrationLevel = TLOG_DEBUG});
        Storage->DoAllocations = true;
        Storage->ReadBlocksLocalHandler = [this](auto, auto)
        {
            if (++ReadAttempts == 1) {
                ReadArrived.TrySetValue();
                return ReadResponse.GetFuture();
            }
            RetryArrived.TrySetValue();
            return RetryResponse.GetFuture();
        };
        Storage->WriteBlocksLocalHandler = [this](auto, auto)
        {
            WriteArrived.TrySetValue();
            return WriteResponse.GetFuture();
        };
    }

    void StartServer(IStoragePtr compoundStorage = {})
    {
        Backend = CreateRdmaBackend(
            Logging,
            std::make_shared<TTestRdmaStorageProvider>(
                compoundStorage ? compoundStorage : Storage),
            [this] { return NowNs.load(); });
        Server = CreateServer(Logging, Backend);
        TOptions options{
            .SocketPath = SocketPath,
            .DiskId = "io-depth-rdma-test",
            .Serial = "io-depth-rdma-test",
            .DeviceBackend = "rdma",
            .Layout =
                {{.DevicePath = "rdma://localhost:10020/test-device",
                  .ByteCount = 1_MB}},
            .NoSync = true,
            .NoChmod = true,
            .BlockSize = BlockSize,
            .QueueCount = 1};
        if (compoundStorage) {
            options.Layout = {
                {.DevicePath = "rdma://localhost:10020/first-device",
                 .ByteCount = BlockSize},
                {.DevicePath = "rdma://localhost:10020/second-device",
                 .ByteCount = BlockSize}};
        }
        Server->Start(options);
        ASSERT_TRUE(Client.Init());
        Memory = TMonotonicBufferResource{Client.GetMemory()};
    }

    void TearDown() override
    {
        // Resolve every test gate before drain, including an assertion failure.
        NProto::TReadBlocksLocalResponse read;
        *read.MutableError() = MakeError(E_CANCELLED);
        ReadResponse.TrySetValue(read);
        RetryResponse.TrySetValue(read);
        NProto::TWriteBlocksLocalResponse write;
        *write.MutableError() = MakeError(E_CANCELLED);
        WriteResponse.TrySetValue(write);
        if (Server) {
            Client.DeInit();
            Server->Stop();
            Server.reset();
        }
        Backend.reset();
    }

    struct TPendingRequest
    {
        std::span<char> Status;
        NThreading::TFuture<ui32> Future;
    };

    TPendingRequest Send(bool write, ui32 blockCount = 1)
    {
        const auto hdr =
            Hdr(Memory,
                {.type = static_cast<ui32>(
                     write ? VIRTIO_BLK_T_OUT : VIRTIO_BLK_T_IN)});
        const auto data = Memory.Allocate(BlockSize * blockCount, BlockSize);
        const auto status = Memory.Allocate(1);
        if (write) {
            memset(data.data(), 'W', data.size());
            return {status, Client.WriteAsync(0, {hdr, data}, {status})};
        }
        return {status, Client.WriteAsync(0, {hdr}, {data, status})};
    }

    TIoDepthSnapshot Snapshot()
    {
        auto result = Backend->GetIoDepthStats();
        EXPECT_TRUE(result);
        return result ? *result : TIoDepthSnapshot{};
    }

    void CompleteRead(bool error = false)
    {
        NProto::TReadBlocksLocalResponse response;
        if (error) {
            *response.MutableError() = MakeError(E_IO);
        }
        ReadResponse.SetValue(std::move(response));
    }
};

class TNoCompletionBackend final: public IBackend
{
public:
    ui64 NowNs = 0;
    TIoDepthTracker Depth{
        2,
        [this]
        {
            return NowNs;
        }};

    vhd_bdev_info Init(const TOptions&) override
    {
        return {};
    }

    void Start() override
    {}

    void Stop() override
    {}

    void ProcessQueue(ui32, vhd_request_queue*, TSimpleStats&) override
    {}

    std::optional<TSimpleStats> GetCompletionStats(TDuration) override
    {
        return std::nullopt;
    }

    std::optional<TIoDepthSnapshot> GetIoDepthStats() override
    {
        return Depth.Snapshot();
    }
};

}   // namespace

TEST_P(TIoDepthAioServerTest, ShouldKeepReadActiveDuringDecrypt)
{
    CheckPendingRead(false);
}

TEST_P(TIoDepthAioServerTest, ShouldCountCompoundReadAsOneParent)
{
    CheckPendingRead(true);
}

INSTANTIATE_TEST_SUITE_P(
    ,
    TIoDepthAioServerTest,
    testing::Values(
        TTestParams{NProto::ENCRYPTION_AES_XTS, 1, 4_KB, false, 2}));

TEST_F(TIoDepthRdmaServerTest, ShouldMeasureReadWithoutCompletions)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));

    NowNs.store(60'000'000'000ULL);
    const auto pending = Snapshot();
    EXPECT_EQ(1u, pending.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(60'000'000u, pending.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_EQ(0u, pending.Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_FALSE(request.Future.HasValue());

    const auto stats = Server->GetStats({});
    EXPECT_EQ(0u, stats.SimpleStats.Completed);
    ASSERT_TRUE(stats.IoDepth);
    EXPECT_EQ(60'000'000u, stats.IoDepth->Lanes[VHD_BDEV_READ].IntegralUs);

    CompleteRead();
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_OK, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(60'000'000u, completed.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_EQ(pending.Generation, completed.Generation);
    EXPECT_TRUE(completed.Continuous);

    Server->Stop();
    Server.reset();
    Client.DeInit();
    EXPECT_EQ(0u, Snapshot().Lanes[VHD_BDEV_READ].Current);
}

TEST_F(TIoDepthRdmaServerTest, ShouldSeparateDirectionsAndFinishErrors)
{
    StartServer();
    auto read = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(1'000'000'000ULL);
    auto write = Send(true);
    ASSERT_TRUE(WriteArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(3'000'000'000ULL);

    const auto pending = Snapshot();
    EXPECT_EQ(1u, pending.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(1u, pending.Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_EQ(3'000'000u, pending.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_EQ(2'000'000u, pending.Lanes[VHD_BDEV_WRITE].IntegralUs);

    CompleteRead(true);
    NProto::TWriteBlocksLocalResponse response;
    *response.MutableError() = MakeError(E_IO);
    WriteResponse.SetValue(response);
    ASSERT_TRUE(read.Future.Wait(TDuration::Seconds(5)));
    ASSERT_TRUE(write.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, read.Status[0]);
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, write.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_TRUE(completed.Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldCountRetriesAsOneParent)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(2'000'000'000ULL);
    NProto::TReadBlocksLocalResponse rejected;
    *rejected.MutableError() = MakeError(E_REJECTED);
    ReadResponse.SetValue(rejected);
    ASSERT_TRUE(RetryArrived.GetFuture().Wait(TDuration::Seconds(5)));
    EXPECT_EQ(2u, ReadAttempts.load());
    EXPECT_FALSE(request.Future.HasValue());

    NowNs.store(5'000'000'000ULL);
    const auto pending = Snapshot();
    EXPECT_EQ(1u, pending.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(5'000'000u, pending.Lanes[VHD_BDEV_READ].IntegralUs);
    RetryResponse.SetValue(NProto::TReadBlocksLocalResponse{});
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_OK, request.Status[0]);
    EXPECT_EQ(0u, Snapshot().Lanes[VHD_BDEV_READ].Current);
}

TEST_F(TIoDepthRdmaServerTest, ShouldFinishFinalErrorAfterRetry)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(1'000'000'000ULL);
    NProto::TReadBlocksLocalResponse rejected;
    *rejected.MutableError() = MakeError(E_REJECTED);
    ReadResponse.SetValue(rejected);
    ASSERT_TRUE(RetryArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(4'000'000'000ULL);
    EXPECT_EQ(1u, Snapshot().Lanes[VHD_BDEV_READ].Current);
    NProto::TReadBlocksLocalResponse failed;
    *failed.MutableError() = MakeError(E_IO);
    RetryResponse.SetValue(failed);
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(4'000'000u, completed.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_TRUE(completed.Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldDrainPendingReadBeforeStop)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    auto stopStarted = NThreading::NewPromise();
    auto stopped = NThreading::NewPromise();
    std::thread stopper(
        [this, stopStarted, stopped]() mutable
        {
            stopStarted.SetValue();
            Server->Stop();
            stopped.SetValue();
        });
    Y_DEFER
    {
        NProto::TReadBlocksLocalResponse cancelled;
        *cancelled.MutableError() = MakeError(E_CANCELLED);
        ReadResponse.TrySetValue(cancelled);
        stopper.join();
        Server.reset();
        Client.DeInit();
    };
    ASSERT_TRUE(stopStarted.GetFuture().Wait(TDuration::Seconds(5)));
    EXPECT_FALSE(stopped.GetFuture().HasValue());
    NowNs.store(3'000'000'000ULL);
    EXPECT_EQ(1u, Snapshot().Lanes[VHD_BDEV_READ].Current);
    CompleteRead();
    ASSERT_TRUE(stopped.GetFuture().Wait(TDuration::Seconds(5)));
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    const auto drained = Snapshot();
    EXPECT_EQ(0u, drained.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(3'000'000u, drained.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_TRUE(drained.Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldKeepOldCompletionInItsGeneration)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    const auto oldSnapshot = Snapshot();

    auto replacement = CreateRdmaBackend(
        Logging,
        std::make_shared<TTestRdmaStorageProvider>(Storage),
        [this] { return NowNs.load(); });
    TOptions options{
        .SocketPath = SocketPath,
        .Serial = "replacement",
        .Layout =
            {{.DevicePath = "rdma://localhost:10020/test-device",
              .ByteCount = 1_MB}},
        .BlockSize = BlockSize,
        .QueueCount = 1};
    replacement->Init(options);
    const auto newSnapshot = replacement->GetIoDepthStats();
    ASSERT_TRUE(newSnapshot);
    EXPECT_NE(oldSnapshot.Generation, newSnapshot->Generation);

    NowNs.store(2'000'000'000ULL);
    CompleteRead();
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    const auto oldCompleted = Snapshot();
    EXPECT_EQ(oldSnapshot.Generation, oldCompleted.Generation);
    EXPECT_EQ(0u, oldCompleted.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(2'000'000u, oldCompleted.Lanes[VHD_BDEV_READ].IntegralUs);

    const auto replacementAfter = replacement->GetIoDepthStats();
    ASSERT_TRUE(replacementAfter);
    EXPECT_EQ(newSnapshot->Generation, replacementAfter->Generation);
    EXPECT_EQ(0u, replacementAfter->Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(0u, replacementAfter->Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_TRUE(replacementAfter->Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldFinishExceptionalStorageFuture)
{
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(ReadArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs.store(1'000'000'000ULL);
    ReadResponse.SetException(std::make_exception_ptr(TServiceError(E_IO)));
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(1'000'000u, completed.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_TRUE(completed.Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldFinishCompoundReadWithExceptionalChild)
{
    auto compound =
        NServer::CreateCompoundStorage({Storage, Storage}, {1, 2}, BlockSize,
                                       "io-depth-rdma-test",
                                       {}, CreateServerStatsStub());
    StartServer(compound);
    auto request = Send(false, 2);
    ASSERT_TRUE(RetryArrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs = 1'000'000'000ULL;
    ReadResponse.SetException(std::make_exception_ptr(TServiceError(E_IO)));
    EXPECT_FALSE(request.Future.HasValue());
    EXPECT_EQ(1u, Snapshot().Lanes[VHD_BDEV_READ].Current);

    NowNs = 2'000'000'000ULL;
    RetryResponse.SetValue(NProto::TReadBlocksLocalResponse{});
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(2'000'000u, completed.Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_TRUE(completed.Continuous);
    NowNs = 3'000'000'000ULL;
    EXPECT_EQ(2'000'000u, Snapshot().Lanes[VHD_BDEV_READ].IntegralUs);
    Server->Stop();
    Server.reset();
    Client.DeInit();
}

TEST_F(TIoDepthRdmaServerTest, ShouldFinishCompoundWriteWithExceptionalChild)
{
    auto second = NThreading::NewPromise<NProto::TWriteBlocksLocalResponse>();
    auto arrived = NThreading::NewPromise<void>();
    auto attempts = std::make_shared<std::atomic<ui32>>(0);
    Storage->WriteBlocksLocalHandler =
        [first = WriteResponse, second, arrived, attempts](auto, auto) mutable
    {
        if (attempts->fetch_add(1) == 0) {
            return first.GetFuture();
        }
        arrived.TrySetValue();
        return second.GetFuture();
    };
    Y_DEFER
    {
        second.TrySetValue(
            NProto::TWriteBlocksLocalResponse(TErrorResponse(E_CANCELLED)));
    };
    auto compound =
        NServer::CreateCompoundStorage({Storage, Storage}, {1, 2}, BlockSize,
                                       "io-depth-rdma-test",
                                       {}, CreateServerStatsStub());
    StartServer(compound);
    auto request = Send(true, 2);
    ASSERT_TRUE(arrived.GetFuture().Wait(TDuration::Seconds(5)));
    NowNs = 1'000'000'000ULL;
    WriteResponse.SetException(std::make_exception_ptr(TServiceError(E_IO)));
    EXPECT_FALSE(request.Future.HasValue());
    EXPECT_EQ(1u, Snapshot().Lanes[VHD_BDEV_WRITE].Current);

    NowNs = 2'000'000'000ULL;
    second.SetValue(NProto::TWriteBlocksLocalResponse{});
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_WRITE].Current);
    EXPECT_EQ(2'000'000u, completed.Lanes[VHD_BDEV_WRITE].IntegralUs);
    EXPECT_TRUE(completed.Continuous);
    NowNs = 3'000'000'000ULL;
    EXPECT_EQ(2'000'000u, Snapshot().Lanes[VHD_BDEV_WRITE].IntegralUs);
    Server->Stop();
    Server.reset();
    Client.DeInit();
}

TEST_F(TIoDepthRdmaServerTest, ShouldHandleInlineCompletion)
{
    Storage->ReadBlocksLocalHandler = [](auto, auto)
    {
        return NThreading::MakeFuture(NProto::TReadBlocksLocalResponse{});
    };
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_OK, request.Status[0]);
    const auto completed = Snapshot();
    EXPECT_EQ(0u, completed.Lanes[VHD_BDEV_READ].Current);
    EXPECT_TRUE(completed.Continuous);
}

TEST_F(TIoDepthRdmaServerTest, ShouldFinishSynchronousStorageException)
{
    Storage->ReadBlocksLocalHandler =
        [](auto, auto) -> NThreading::TFuture<NProto::TReadBlocksLocalResponse>
    {
        throw TServiceError(E_IO);
    };
    StartServer();
    auto request = Send(false);
    ASSERT_TRUE(request.Future.Wait(TDuration::Seconds(5)));
    EXPECT_EQ(VIRTIO_BLK_S_IOERR, request.Status[0]);
    EXPECT_EQ(0u, Snapshot().Lanes[VHD_BDEV_READ].Current);
    EXPECT_TRUE(Snapshot().Continuous);
}

TEST(TIoDepthServerTest, ShouldRefreshDepthWithoutCompletionStats)
{
    auto backend = std::make_shared<TNoCompletionBackend>();
    auto server = CreateServer(CreateLoggingService("console"), backend);
    backend->Depth.Started(VHD_BDEV_READ);
    backend->NowNs = 5'000'000'000ULL;
    TSimpleStats previous;
    previous.Completed = 7;

    const auto first = server->GetStats(previous);
    EXPECT_EQ(7u, first.SimpleStats.Completed);
    ASSERT_TRUE(first.IoDepth);
    EXPECT_EQ(1u, first.IoDepth->Lanes[VHD_BDEV_READ].Current);
    EXPECT_EQ(5'000'000u, first.IoDepth->Lanes[VHD_BDEV_READ].IntegralUs);

    backend->NowNs = 9'000'000'000ULL;
    const auto second = server->GetStats(previous);
    EXPECT_EQ(7u, second.SimpleStats.Completed);
    ASSERT_TRUE(second.IoDepth);
    EXPECT_EQ(9'000'000u, second.IoDepth->Lanes[VHD_BDEV_READ].IntegralUs);
    EXPECT_EQ(first.IoDepth->Generation, second.IoDepth->Generation);
    EXPECT_TRUE(second.IoDepth->Continuous);
}

TEST(TIoDepthServerTest, ShouldDistinguishUnsupportedBackendFromIdle)
{
    auto backend = CreateNullBackend(CreateLoggingService("console"));
    EXPECT_FALSE(backend->GetIoDepthStats());
}

}   // namespace NCloud::NBlockStore::NVHostServer
