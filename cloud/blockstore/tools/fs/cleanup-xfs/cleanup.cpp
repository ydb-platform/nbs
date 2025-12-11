#include "cleanup.h"

#include <cloud/storage/core/libs/common/format.h>

#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>
#include <util/thread/factory.h>

#include <utility>

////////////////////////////////////////////////////////////////////////////////

void Cleanup(
    IDev& dev,
    const TSuperBlock& sb,
    const TVector<TFreeList>& freesp,
    ui32 threads,
    bool verbose)
{
    const ui64 bufferSize = 4_MB;

    char* zero = static_cast<char*>(std::aligned_alloc(4_KB, bufferSize));
    Y_DEFER
    {
        std::free(zero);
    }

    std::memset(zero, 0, bufferSize);

    const ui64 headerSize = 4 * sb.SectorSize;

    if (sb.BlockSize > headerSize) {
        char* buffer =
            static_cast<char*>(std::aligned_alloc(4_KB, sb.BlockSize));
        Y_DEFER
        {
            std::free(buffer);
        }

        for (ui64 i = 1; i < sb.GroupCount; ++i) {
            const ui64 offset = i * sb.BlocksPerGroup * sb.BlockSize;

            if (verbose) {
                Cout << "[cleanup] cleanup AG #" << i
                     << " first block: " << offset << " ("
                     << offset / sb.BlockSize << ")" << Endl;
            }

            dev.Pload(buffer, sb.BlockSize, offset);
            std::memset(buffer + headerSize, 0, sb.BlockSize - headerSize);
            dev.Pwrite(buffer, sb.BlockSize, offset);
        }
    }

    // [len, offset]
    NThreading::TBlockingQueue<std::tuple<ui32, i64> > queue(threads);

    using TThread = THolder<IThreadFactory::IThread>;

    TVector<TThread> workers;
    workers.reserve(threads);

    for (ui32 i = 0; i != threads; ++i) {
        workers.push_back(SystemThreadFactory()->Run(
            [zero, &queue, &dev]()
            {
                while (auto task = queue.Pop()) {
                    auto [len, offset] = *task;

                    if (!len) {
                        break;
                    }

                    dev.Pwrite(zero, len, offset);
                }
            }));
    }

    for (auto [group, offset, count]: freesp) {
        ui64 offsetInBytes =
            (group * sb.BlocksPerGroup + offset) * sb.BlockSize;
        ui64 byteCount = count * sb.BlockSize;

        if (verbose) {
            Cout << "[cleanup] " << offsetInBytes << ":" << byteCount << " ("
                 << NCloud::FormatByteSize(byteCount) << ")" << " #" << group
                 << " " << offset << ":" << count << Endl;
        }

        while (byteCount) {
            const ui32 len = static_cast<ui32>(std::min(byteCount, bufferSize));

            queue.Push({len, offsetInBytes});

            offsetInBytes += len;
            byteCount -= len;
        }
    }

    for (ui32 i = 0; i != threads; ++i) {
        queue.Push({});
    }

    for (auto& w: workers) {
        w->Join();
    }
}
