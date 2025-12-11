#include "cloud/storage/core/libs/common/format.h"
#include "util/generic/fwd.h"

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/threading/blocking_queue/blocking_queue.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/string/strip.h>
#include <util/system/file.h>
#include <util/system/fstat.h>
#include <util/system/mutex.h>
#include <util/system/shellcommand.h>
#include <util/system/types.h>
#include <util/thread/factory.h>

#include <cstddef>
#include <functional>
#include <memory>

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString SrcPath;
    TString DstPath;

    ui64 BufferSize = 4_MB;
    unsigned int Threads = 8;

    bool DryRun = false;
    bool Verbose = false;

    void Parse(int argc, char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption('s', "src-path")
            .RequiredArgument("PATH")
            .StoreResult(&SrcPath);

        opts.AddLongOption('d', "dst-path")
            .RequiredArgument("PATH")
            .StoreResult(&DstPath);

        opts.AddLongOption("buffer-size")
            .RequiredArgument("NUM")
            .DefaultValue(BufferSize)
            .StoreResult(&BufferSize);

        opts.AddLongOption("threads")
            .RequiredArgument("NUM")
            .DefaultValue(Threads)
            .StoreResult(&Threads);

        opts.AddLongOption("dry-run").NoArgument().StoreTrue(&DryRun);

        opts.AddLongOption('v', "verbose").NoArgument().StoreTrue(&Verbose);

        TOptsParseResultException res(&opts, argc, argv);
    }
};

ui64 GetDevSize(const TString& path)
{
    TShellCommand cmd("blockdev", {"--getsize64", path});

    auto output = cmd.Run().Wait().GetOutput();

    auto ec = cmd.GetExitCode();

    Y_ENSURE(!ec.Empty());
    Y_ENSURE(ec == 0, "blockdev: " << *ec);

    return FromString<ui64>(Strip(output));
}

////////////////////////////////////////////////////////////////////////////////

struct IDev
{
    virtual ~IDev() = default;
    virtual void Pload(void* buffer, ui32 byteCount, i64 offset) const = 0;
    virtual void
    Pwrite(const void* buffer, ui32 byteCount, i64 offset) const = 0;

    virtual i64 GetLength() const = 0;
};

struct TDummyDev: IDev
{
    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(byteCount);
        Y_UNUSED(offset);
    }

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        Y_UNUSED(buffer);
        Y_UNUSED(byteCount);
        Y_UNUSED(offset);
    }

    i64 GetLength() const override
    {
        return 93_GB;
    }
};

struct TDev: IDev
{
    TFile File;

    explicit TDev(const TString& devPath, bool readOnly)
        : File(
              devPath,
              readOnly ? (OpenExisting | DirectAligned | RdOnly)
                       : (OpenExisting | DirectAligned | RdWr | Sync))
    {}

    void Pload(void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pload(buffer, byteCount, offset);
    }

    void Pwrite(const void* buffer, ui32 byteCount, i64 offset) const override
    {
        File.Pwrite(buffer, byteCount, offset);
    }

    i64 GetLength() const override
    {
        TFileStat stat{File};

        if (stat.IsFile()) {
            return File.GetLength();
        }

        return GetDevSize(File.GetName());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFree
{
    template <typename T>
    void operator()(T* p) const
    {
        std::free(p);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TApp
{
    using TThread = THolder<IThreadFactory::IThread>;

    struct TTask
    {
        void* Buffer = nullptr;
        ui32 ByteCount = 0;
        i64 Offset = 0;
    };

private:
    const TOptions& Options;

    TVector<std::unique_ptr<void, TFree>> Buffers;

    TVector<TThread> Readers;
    TVector<TThread> Writers;

    NThreading::TBlockingQueue<void*> FreeBuffers;
    NThreading::TBlockingQueue<TTask> ReadQueue;
    NThreading::TBlockingQueue<TTask> WriteQueue;

    TMutex ErrorMtx;
    TString Error;

    i64 PrevWritten = 0;
    i64 PrevRead = 0;

    TAtomic Written = 0;
    TAtomic Read = 0;

    TInstant ProgressTs;

public:
    explicit TApp(const TOptions& options)
        : Options(options)
        , FreeBuffers(0)
        , ReadQueue(Options.Threads)
        , WriteQueue(Options.Threads)
    {}

    ~TApp()
    {
        Shutdown();
    }

    void Shutdown()
    {
        FreeBuffers.Stop();
        ReadQueue.Stop();
        WriteQueue.Stop();

        for (auto& r: Readers) {
            r->Join();
        }

        for (auto& w: Writers) {
            w->Join();
        }
    }

    int Run()
    {
        auto src = CreateDev(Options.SrcPath, true);
        auto dst = CreateDev(Options.DstPath, false);

        const i64 totalLength = src->GetLength();
        Y_ENSURE(totalLength <= dst->GetLength());

        for (unsigned int i = 0; i != Options.Threads; ++i) {
            Readers.push_back(SystemThreadFactory()->Run(
                [this, &file = *src]()
                { HandleErrors(&TApp::ReaderThread, file); }));
        }

        for (unsigned int i = 0; i != Options.Threads; ++i) {
            Writers.push_back(SystemThreadFactory()->Run(
                [this, &file = *dst]()
                { HandleErrors(&TApp::WriterThread, file); }));
        }

        Buffers.reserve(Options.Threads * 2);
        for (unsigned int i = 0; i != Options.Threads * 2; ++i) {
            Buffers.emplace_back(std::aligned_alloc(512, Options.BufferSize));
        }

        for (auto& b: Buffers) {
            FreeBuffers.Push(b.get());
        }

        // copy

        if (Options.Verbose) {
            Cout << "Copy " << Options.SrcPath << " to " << Options.DstPath
                 << ". Buffer size: "
                 << NCloud::FormatByteSize(Options.BufferSize) << Endl;
        }

        i64 offset = 0;
        while (offset != totalLength) {
            auto buffer = FreeBuffers.Pop();
            if (!buffer) {
                break;
            }

            const ui32 len = static_cast<ui32>(
                Min<i64>(totalLength - offset, Options.BufferSize));

            ReadQueue.Push({*buffer, len, offset});

            offset += len;

            ShowProgress(totalLength);
        }

        // wait for Readers

        for (size_t i = 0; i != Readers.size(); ++i) {
            ReadQueue.Push({});
        }

        for (auto& r: Readers) {
            r->Join();
        }
        Readers.clear();

        // wait for Writers

        for (size_t i = 0; i != Writers.size(); ++i) {
            WriteQueue.Push({});
        }

        for (auto& w: Writers) {
            w->Join();
        }
        Writers.clear();

        ShowProgress(totalLength);

        // done

        if (!Error.empty()) {
            Cerr << "[ERROR] " << Error << Endl;
            return 1;
        }

        if (Options.Verbose) {
            Cout << "Done" << Endl;
        }

        return 0;
    }

private:
    void ShowProgress(i64 totalLength)
    {
        if (!Options.Verbose) {
            return;
        }

        const auto w = AtomicGet(Written);
        const auto r = AtomicGet(Read);
        const auto p = w * 100 / totalLength;
        auto now = Now();

        if (!ProgressTs || now - ProgressTs >= TDuration::Seconds(5) ||
            totalLength == w)
        {
            ui64 ws = 0;
            ui64 rs = 0;

            auto dt = static_cast<double>((now - ProgressTs).Seconds());

            if (w) {
                ws = static_cast<ui64>((w - PrevWritten) / dt);
            }

            if (r) {
                rs = static_cast<ui64>((r - PrevRead) / dt);
            }

            Cout << "Written: " << NCloud::FormatByteSize(w) << " ("
                 << NCloud::FormatByteSize(ws) << "/s) "
                 << "Read: " << NCloud::FormatByteSize(r) << " ("
                 << NCloud::FormatByteSize(rs) << "/s) " << "Progress: " << p
                 << " % (Total " << NCloud::FormatByteSize(totalLength) << ")"
                 << Endl;

            ProgressTs = now;
            PrevWritten = w;
            PrevRead = r;
        }
    }

    std::unique_ptr<IDev> CreateDev(const TString& path, bool readOnly)
    {
        if (Options.DryRun) {
            return std::make_unique<TDummyDev>();
        }

        return std::make_unique<TDev>(path, readOnly);
    }

    template <typename F>
    void HandleErrors(F fn, IDev& file)
    {
        try {
            std::invoke(fn, this, file);
        } catch (...) {
            FreeBuffers.Stop();
            ReadQueue.Stop();
            WriteQueue.Stop();

            with_lock (ErrorMtx) {
                Error = CurrentExceptionMessage();
            }
        }
    }

    void ReaderThread(IDev& file)
    {
        while (auto task = ReadQueue.Pop()) {
            auto [buffer, byteCount, offset] = *task;

            if (!byteCount) {
                break;
            }

            file.Pload(buffer, byteCount, offset);
            AtomicAdd(Read, byteCount);

            WriteQueue.Push(*task);
        }
    }

    void WriterThread(IDev& file)
    {
        while (auto task = WriteQueue.Pop()) {
            auto [buffer, byteCount, offset] = *task;

            if (!byteCount) {
                break;
            }

            file.Pwrite(buffer, byteCount, offset);
            AtomicAdd(Written, byteCount);

            FreeBuffers.Push(buffer);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    try {
        TOptions options;
        options.Parse(argc, argv);

        TApp app(options);

        return app.Run();

    } catch (...) {
        Cerr << "[ERROR] " << CurrentExceptionMessage() << Endl;
    }

    return 1;
}
