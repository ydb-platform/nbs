#include "app.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/stream.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/thread.h>

#include <atomic>
#include <csignal>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TProducerThread: public ISimpleThread
{
private:
    TLog Log;
    const TOptions& Options;
    const ui32 ThreadId;
    TFastRng64 Rng;

    std::atomic<ui32> Depth = 0;
    std::atomic<ui64> FileCount = 0;
    std::atomic<ui64> DirCount = 0;
    std::atomic<ui64> SymlinkCount = 0;
    std::atomic<bool> Done = false;
    std::atomic<bool>& ShouldStop;

    TFsPath DirPath;
    ui64 NodeNo = 0;

public:
    TProducerThread(
            TLog log,
            const TOptions& options,
            ui32 threadId,
            std::atomic<bool>& shouldStop)
        : Log(std::move(log))
        , Options(options)
        , ThreadId(threadId)
        , Rng(ThreadId + options.Seed)
        , ShouldStop(shouldStop)
    {
        DirPath = TFsPath(Options.TestDir)
            / (TStringBuilder() << "producer_" << ThreadId);
        MakeDirIfNotExist(DirPath.c_str());
    }

    void* ThreadProc() override
    {
        double p = Options.SubdirProbability;
        TVector<TFsPath> dirs(1, DirPath);
        while (dirs.size() && !ShouldStop.load()) {
            TVector<TFsPath> newDirs;
            for (const auto& dir: dirs) {
                TVector<TFsPath> children;
                for (ui32 i = 0; i < Options.ChildrenCount; ++i) {
                    if (Rng.GenRandReal2() < p) {
                        auto child = CreateDir(dir);
                        newDirs.push_back(child);
                        children.push_back(std::move(child));
                        DirCount.fetch_add(1, std::memory_order_relaxed);
                    } else {
                        auto child = CreateFile(dir);
                        children.push_back(std::move(child));
                        FileCount.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                for (ui32 i = 0; i < Options.SymlinkChildrenCount; ++i) {
                    CreateSymlink(
                        dir,
                        children[i % children.size()].Basename());
                    SymlinkCount.fetch_add(1, std::memory_order_relaxed);
                }
            }

            dirs.swap(newDirs);
            p *= Options.DecayFactor;
            Depth.fetch_add(1, std::memory_order_relaxed);
        }

        Done.store(true);
        return nullptr;
    }

    const auto& GetDirPath() const
    {
        return DirPath;
    }

    ui32 GetDepth() const
    {
        return Depth.load(std::memory_order_relaxed);
    }

    ui64 GetFileCount() const
    {
        return FileCount.load(std::memory_order_relaxed);
    }

    ui64 GetDirCount() const
    {
        return DirCount.load(std::memory_order_relaxed);
    }

    ui64 GetSymlinkCount() const
    {
        return SymlinkCount.load(std::memory_order_relaxed);
    }

    bool IsDone() const
    {
        return Done.load();
    }

private:
    TFsPath CreateDir(const TFsPath& parent)
    {
        TString dirName =
            TStringBuilder() << "dir_" << ThreadId << "_" << NodeNo;
        ++NodeNo;
        TFsPath dirPath = parent / dirName;
        MakeDirIfNotExist(dirPath.c_str());
        return dirPath;
    }

    TFsPath CreateFile(const TFsPath& parent)
    {
        TString fileName =
            TStringBuilder() << "file_" << ThreadId << "_" << NodeNo;
        ++NodeNo;
        TFsPath filePath = parent / fileName;

        static constexpr EOpenMode OpenMode = CreateAlways | WrOnly | Seq;
        TFile f(filePath, OpenMode);
        return filePath;
    }

    void CreateSymlink(const TFsPath& parent, const TFsPath& target)
    {
        TString fileName =
            TStringBuilder() << "link_" << ThreadId << "_" << NodeNo;
        ++NodeNo;
        TFsPath filePath = parent / fileName;

        const bool created = NFs::SymLink(target, filePath);
        if (!created) {
            STORAGE_ERROR("Producer " << DirPath << " at depth " << GetDepth()
                << " failed to create link: " << filePath << " -> " << target);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    std::atomic<bool> ShouldStop{false};
    TVector<THolder<TProducerThread>> ProducerThreads;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(const TOptions& options)
    {
        // Create test directory
        MakeDirIfNotExist(options.TestDir.c_str());

        auto asyncLogger = CreateAsyncLogger();
        asyncLogger->Start();
        // capital 'L' letter needed for the logging macros
        TLog Log = CreateComponentLog(
            "BENCH",
            std::make_shared<TStreamLogBackend>(&Cerr),
            asyncLogger);

        // Create producer threads
        TVector<TProducerThread*> producerPtrs;
        for (ui32 i = 0; i < options.ProducerThreads; ++i) {
            auto p = MakeHolder<TProducerThread>(Log, options, i, ShouldStop);
            producerPtrs.push_back(p.Get());
            ProducerThreads.push_back(std::move(p));
        }

        // Run all threads
        for (auto& p: ProducerThreads) {
            p->Start();
        }

        // Wait for test duration
        while (!ShouldStop.load()) {
            Sleep(TDuration::Seconds(1));

            bool done = true;
            for (auto& p: ProducerThreads) {
                STORAGE_INFO("Producer " << p->GetDirPath() << " at depth "
                    << p->GetDepth()
                    << ", created " << p->GetFileCount() << " files"
                    << ", " << p->GetSymlinkCount() << " symlinks"
                    << ", " << p->GetDirCount() << " dirs");

                if (!p->IsDone()) {
                    done = false;
                }
            }

            if (done) {
                break;
            }
        }

        // Wait for threads to finish
        for (ui32 i = 0; i < options.ProducerThreads; ++i) {
            auto& p = ProducerThreads[i];
            p->Join();
            Cout << "Producer " << i << " depth=" << p->GetDepth()
                << ", files=" << p->GetFileCount()
                << ", symlinks=" << p->GetSymlinkCount()
                << ", dirs=" << p->GetDirCount()
                << Endl;
        }

        return 0;
    }

    void Stop()
    {
        ShouldStop.store(true);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ProcessSignal(int signum)
{
    if (signum == SIGINT || signum == SIGTERM) {
        AppStop();
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void ConfigureSignals()
{
    std::set_new_handler(abort);

    // make sure that errors can be seen by everybody :)
    (void) setvbuf(stdout, nullptr, _IONBF, 0);
    (void) setvbuf(stderr, nullptr, _IONBF, 0);

    // mask signals
    (void) signal(SIGPIPE, SIG_IGN);
    (void) signal(SIGINT, ProcessSignal);
    (void) signal(SIGTERM, ProcessSignal);
}

int AppMain(const TOptions& options)
{
    return TApp::GetInstance()->Run(options);
}

void AppStop()
{
    TApp::GetInstance()->Stop();
}

}   // namespace NCloud::NFileStore
