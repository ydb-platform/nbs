// Tries to read stale data right after a completed append.
//
// The writer appends two bytes and, as soon as write() has returned, reopens
// the file and reads them back. Racer threads keep reopening and reading the
// end of the file, so that sooner or later an open + read pair lands while an
// append is still in flight. On a correct filesystem the writer always gets
// its bytes back. The program exits 1 the first time it does not, and 0 if
// that never happens within the time limit.
//
// The last line tells why a run was clean:
//   overlapped   racer reads that started after an append had been issued and
//                still saw the old end of file; 0 means the appends are too
//                fast to race with
//   open, write  average latency. A write several times slower than an open
//                is the setup that fails. An open as slow as a write means
//                opens wait for the commit (async open is off). A write
//                several times faster than an open means writes are
//                acknowledged before they are committed (write-back cache)
//
// See https://github.com/ydb-platform/nbs/issues/6714
//
//   stale-read-loop FILE [SECONDS] [RACERS]

#include <util/datetime/base.h>
#include <util/generic/scope.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

#include <atomic>
#include <thread>

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr i64 PageSize = 4096;
constexpr TStringBuf AppendData = "X\n";

TString FilePath;
std::atomic<i64> Size = 0;      // bytes whose write() has returned
std::atomic<i64> Pending = 0;   // the same plus the append in flight, if any
std::atomic<bool> ShouldStop = false;
std::atomic<ui64> OpenCount = 0;
std::atomic<ui64> OpenTimeUs = 0;
std::atomic<ui64> OverlappedCount = 0;

////////////////////////////////////////////////////////////////////////////////

// Opens the file, reads len bytes at offset and closes it again
i32 ReopenAndRead(void* buf, ui32 len, i64 offset)
{
    const auto start = TInstant::Now();
    TFile file(FilePath, OpenExisting | RdOnly);
    OpenTimeUs += (TInstant::Now() - start).MicroSeconds();
    ++OpenCount;
    return file.RawPread(buf, len, offset);
}

// An exception in a racer terminates the process, which fails the test as well
void RunRacer()
{
    char page[PageSize];
    while (!ShouldStop) {
        const i64 issued = Pending;
        const i64 offset = Size & ~(PageSize - 1);
        const i32 n = ReopenAndRead(page, PageSize, offset);
        if (n >= 0 && n < PageSize && offset + n < issued) {
            ++OverlappedCount;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    if (argc < 2) {
        Cerr << "Usage: " << argv[0] << " FILE [SECONDS] [RACERS]" << Endl;
        return 2;
    }
    FilePath = argv[1];
    const auto deadline =
        TDuration::Seconds(argc > 2 ? FromString<ui32>(argv[2]) : 60)
            .ToDeadLine();
    const ui32 racerCount = argc > 3 ? FromString<ui32>(argv[3]) : 4;

    TFile file(FilePath, CreateAlways | WrOnly | Sync);
    const i64 len = AppendData.size();

    ui64 appendCount = 0;
    TDuration writeTime;
    TDuration slowestWrite;
    bool stale = false;
    {
        TVector<std::thread> racers;
        Y_DEFER {
            ShouldStop = true;
            for (auto& racer: racers) {
                racer.join();
            }
        };
        for (ui32 i = 0; i < racerCount; ++i) {
            racers.emplace_back(RunRacer);
        }

        while (!stale && TInstant::Now() < deadline) {
            const i64 offset = Size;
            const auto start = TInstant::Now();
            Pending = offset + len;
            file.Pwrite(AppendData.data(), len, offset);
            const auto elapsed = TInstant::Now() - start;
            writeTime += elapsed;
            slowestWrite = Max(slowestWrite, elapsed);
            Size = offset + len;
            ++appendCount;

            char buf[AppendData.size()] = {};
            const i32 n = ReopenAndRead(buf, len, offset);
            if (n != len || TStringBuf(buf, n) != AppendData) {
                Cout << "STALE READ after " << appendCount
                    << " appends: at offset " << offset
                    << " expected 58 0a, read " << n << " bytes: "
                    << Sprintf("%02x %02x", ui8(buf[0]), ui8(buf[1])) << Endl;
                stale = true;
            }
        }
    }

    if (!stale) {
        Cout << "no stale read in " << appendCount << " appends" << Endl;
    }
    Cout << "racers=" << racerCount
        << " overlapped=" << OverlappedCount.load()
        << " open=" << (OpenCount ? OpenTimeUs / OpenCount : 0)
        << "us write="
        << (appendCount ? writeTime.MicroSeconds() / appendCount : 0)
        << "us (slowest " << slowestWrite.MicroSeconds() << "us)" << Endl;

    return stale ? 1 : 0;
}
