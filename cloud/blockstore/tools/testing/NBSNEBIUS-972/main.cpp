#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/file_io_service.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/system/file.h>
#include <util/system/yassert.h>

#include <libaio.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <numeric>

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 BlockSize = 4096;

////////////////////////////////////////////////////////////////////////////////

struct TFree
{
    void operator () (void* ptr) const noexcept
    {
        std::free(ptr);
    }
};

using TMemory = std::unique_ptr<char, TFree>;

TMemory AlignedAlloc(ui64 bytes)
{
    TMemory buffer(static_cast<char*>(std::aligned_alloc(BlockSize, bytes)));

    return buffer;
}

////////////////////////////////////////////////////////////////////////////////

using TBlockIndex2Data = THashMap<ui64, ui64>;

struct TLoad
{
    TBlockIndex2Data BlockIndex2Data;

    TFileHandle File;
    NCloud::IFileIOServicePtr FileIO;

    explicit TLoad(const TString& filename)
        : File{
              filename,
              EOpenModeFlag::OpenExisting | EOpenModeFlag::RdWr |
                  EOpenModeFlag::DirectAligned | EOpenModeFlag::Sync}
        , FileIO{NCloud::CreateAIOService()}
    {
        Cout << "filename = " << filename << Endl;

        Y_ABORT_UNLESS(File.IsOpen());

        FileIO->Start();
    }

    ~TLoad()
    {
        FileIO->Stop();
    }

    void Run()
    {
        const ui64 fileSize = File.GetLength();
        Y_ABORT_UNLESS(fileSize > 0);
        const ui64 blockCount = fileSize / BlockSize;

        const ui64 rangeStart = 1000;
        const ui64 rangeEnd = 2000;
        const ui64 rangeSize = rangeEnd - rangeStart;
        const ui64 maxBlockCount = 128;

        Y_ABORT_UNLESS(maxBlockCount < rangeSize);
        Y_ABORT_UNLESS(blockCount > rangeSize);

        ui64 reads = 0;
        ui64 writes = 0;
        ui64 retries = 0;

        for (ui64 requestId = 0;; ++requestId) {
            if (requestId % 100 == 0) {
                Cout << "requestId = " << requestId << " "
                     << BlockIndex2Data.size() << " reads: " << reads
                     << " writes: " << writes << " retries: " << retries
                     << Endl;
            }

            const ui64 blockCount = 1 + RandomNumber<ui64>(maxBlockCount);
            const ui64 startIndex =
                rangeStart + RandomNumber<ui64>(rangeSize - blockCount + 1);

            if (RandomNumber<double>() < 0.5) {
                ++writes;

                for (;;) {
                    Write(startIndex, blockCount, requestId);

                    if (RandomNumber<double>() < 0.1) {
                        ++retries;
                        continue;
                    }
                    break;
                }
                continue;
            }
            ++reads;
            Read(startIndex, blockCount);
        }
    }

    void Write(ui64 startIndex, ui64 blockCount, ui64 blockData)
    {
        // Cerr << "= Write " << startIndex << ":" << blockCount << " "
        //      << blockData << Endl;

        const i64 offset = static_cast<i64>(startIndex) * BlockSize;
        const ui64 len = blockCount * BlockSize;

        TMemory buffer = AlignedAlloc(len);

        ui64* data = reinterpret_cast<ui64*>(buffer.get());
        std::fill_n(data, len / sizeof(ui64), blockData);

        auto future = FileIO->AsyncWrite(File, offset, {buffer.get(), len});

        const ui32 ret = future.GetValueSync();
        Y_ABORT_UNLESS(ret == len);

        for (ui64 i = 0; i != blockCount; ++i) {
            BlockIndex2Data[startIndex + i] = blockData;
        }
    }

    void Read(ui64 startIndex, ui64 blockCount)
    {
        const i64 offset = static_cast<i64>(startIndex) * BlockSize;
        const ui64 len = blockCount * BlockSize;

        TMemory buffer = AlignedAlloc(len);

        auto future = FileIO->AsyncRead(File, offset, {buffer.get(), len});
        const ui32 ret = future.GetValueSync();
        Y_ABORT_UNLESS(ret == len);

        const ui64 expectedCount = BlockSize / sizeof(ui64);

        for (ui64 i = 0; i != blockCount; ++i) {
            auto it = BlockIndex2Data.find(startIndex + i);
            const ui64 expectedData =
                it == BlockIndex2Data.end() ? 0 : it->second;
            ui64* data = reinterpret_cast<ui64*>(buffer.get() + i * BlockSize);
            const ui64 n = std::count(data, data + expectedCount, expectedData);

            if (n == expectedCount) {
                continue;
            }

            Cerr << "!!! Error in block #" << (startIndex + i) << Endl;
            Cerr << n << " != " << expectedCount << Endl;
            Cerr << "Expected " << expectedData << Endl;
            Cerr << "BlockIndex2Data (" << BlockIndex2Data.size() << "): " << Endl;

            TVector<std::pair<ui64, ui64>> values(
                BlockIndex2Data.begin(),
                BlockIndex2Data.end());

            SortBy(values, [] (const auto& p) {
                return p.first;
            });

            for (auto [index, value]: values) {
                Cerr << index << ": " << value << " " << Endl;
            }

            Y_ABORT();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    if (argc != 2) {
        return 1;
    }

    TLoad load{argv[1]};

    load.Run();

    return 0;
}
