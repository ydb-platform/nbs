#include "app.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/stream.h>

#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/thread.h>
#include <util/datetime/base.h>

#include <atomic>
#include <csignal>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFileInfo
{
    TString Name;
    ui64 Ino = 0;
    ui64 Size = 0;
    TString Content;
};

struct TStats
{
    std::atomic<ui64> Creates{0};
    std::atomic<ui64> Unlinks{0};
    std::atomic<ui64> Renames{0};
    std::atomic<ui64> Lists{0};
};

////////////////////////////////////////////////////////////////////////////////

struct TFsPathHash
{
    ui64 operator()(const TFsPath& p) const
    {
        return CityHash64(p.GetPath().data(), p.GetPath().size());
    }
};

struct TValidationContext
{
    THashSet<TFsPath, TFsPathHash> UnlinkedPaths;
    THashSet<TFsPath, TFsPathHash> StolenPaths;
};

////////////////////////////////////////////////////////////////////////////////

class TProducerThread: public ISimpleThread
{
private:
    TLog Log;
    const TOptions& Options;
    const ui32 ThreadId;
    TStats& Stats;
    std::atomic<bool>& ShouldStop;

    TFsPath DirPath;
    TVector<TFileInfo> Files;
    TVector<TString> UnlinkedFiles;
    TVector<TString> FailedToUnlinkFiles;
    ui64 FileNo = 0;

public:
    TProducerThread(
            TLog log,
            const TOptions& options,
            ui32 threadId,
            TStats& stats,
            std::atomic<bool>& shouldStop)
        : Log(std::move(log))
        , Options(options)
        , ThreadId(threadId)
        , Stats(stats)
        , ShouldStop(shouldStop)
    {
        DirPath = TFsPath(Options.TestDir)
            / (TStringBuilder() << "producer_" << ThreadId);
    }

    void* ThreadProc() override
    {
        MakeDirIfNotExist(DirPath.c_str());

        while (!ShouldStop.load()) {
            // Create files
            if (Files.size() < Options.FilesPerProducer) {
                CreateFile();
            }

            // Randomly delete some files
            if (!Files.empty() && RandomNumber<ui32>(100) < 20) {
                DeleteRandomFile();
            }

            // Small sleep to avoid busy loop
            Sleep(TDuration::MilliSeconds(1));
        }

        return nullptr;
    }

    const auto& GetUnlinkedFiles() const
    {
        return UnlinkedFiles;
    }

    ui32 Validate(const TValidationContext& vc)
    {
        STORAGE_INFO("Producer " << ThreadId << " validating...");

        ui32 errors = 0;

        for (const auto& fileName: UnlinkedFiles) {
            TFsPath filePath = DirPath / fileName;

            if (NFs::Exists(filePath)) {
                STORAGE_ERROR("Unlinked file still exists: " << filePath);
                ++errors;
            }
        }

        for (const auto& fileName: FailedToUnlinkFiles) {
            TFsPath filePath = DirPath / fileName;

            if (!vc.StolenPaths.contains(filePath)) {
                STORAGE_ERROR("Failed to unlink non-stolen file: " << filePath);
                ++errors;
            }
        }

        for (const auto& file: Files) {
            TFsPath filePath = DirPath / file.Name;

            if (NFs::Exists(filePath)) {
                TFileStat stat(filePath);
                if (stat.INode != file.Ino) {
                    STORAGE_ERROR("Inode mismatch for " << filePath
                        << " expected: " << file.Ino
                        << " got: " << stat.INode);
                    ++errors;
                }
                if (stat.Size != file.Size) {
                    STORAGE_ERROR("Size mismatch for " << filePath
                        << " expected: " << file.Size
                        << " got: " << stat.Size);
                    ++errors;
                }

                TString content = TFileInput(filePath).ReadAll();
                if (content != file.Content) {
                    STORAGE_ERROR("Content mismatch for " << filePath);
                    ++errors;
                }

                continue;
            }

            if (!vc.StolenPaths.contains(filePath)) {
                STORAGE_ERROR("File missing: " << filePath);
                ++errors;
            }
        }

        STORAGE_INFO("Producer " << ThreadId << " validation complete");
        return errors;
    }

    const auto& GetDirPath() const
    {
        return DirPath;
    }

private:
    void CreateFile()
    {
        TString fileName = TStringBuilder() << "file_" << FileNo;
        ++FileNo;
        TFsPath filePath = DirPath / fileName;

        TString content(Options.FileSize, 'a' + (ThreadId % ('z' - 'a' + 1)));
        TFileOutput(filePath).Write(content);

        TFileStat stat(filePath);
        Files.push_back({
            fileName,
            stat.INode,
            stat.Size,
            content
        });

        Stats.Creates.fetch_add(1);
    }

    void DeleteRandomFile()
    {
        if (Files.empty()) {
            return;
        }

        ui32 idx = RandomNumber<ui32>(Files.size());
        auto& file = Files[idx];

        TFsPath filePath = DirPath / file.Name;
        const bool removed = NFs::Remove(filePath);
        if (removed) {
            UnlinkedFiles.push_back(file.Name);
            Stats.Unlinks.fetch_add(1);
        } else {
            FailedToUnlinkFiles.push_back(file.Name);
        }

        if (idx != Files.size() - 1) {
            DoSwap(Files[idx], Files.back());
        }

        Files.pop_back();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStealerThread: public ISimpleThread
{
private:
    TLog Log;
    const TOptions& Options;
    const ui32 ThreadId;
    TStats& Stats;
    std::atomic<bool>& ShouldStop;
    const TVector<TProducerThread*>& Producers;

    TFsPath DirPath;
    TVector<TFileInfo> StolenFiles;
    TVector<TFsPath> StolenPaths;
    TVector<TFsPath> FailedToStealPaths;

public:
    TStealerThread(
            TLog log,
            const TOptions& options,
            ui32 threadId,
            TStats& stats,
            std::atomic<bool>& shouldStop,
            const TVector<TProducerThread*>& producers)
        : Log(std::move(log))
        , Options(options)
        , ThreadId(threadId)
        , Stats(stats)
        , ShouldStop(shouldStop)
        , Producers(producers)
    {
        DirPath = TFsPath(Options.TestDir)
            / (TStringBuilder() << "stealer_" << ThreadId);
    }

    void* ThreadProc() override
    {
        MakeDirIfNotExist(DirPath.c_str());

        while (!ShouldStop.load()) {
            StealRandomFile();
            Sleep(TDuration::MilliSeconds(10));
        }

        return nullptr;
    }

    const auto& GetStolenPaths() const
    {
        return StolenPaths;
    }

    ui32 Validate(const TValidationContext& vc)
    {
        STORAGE_INFO("Stealer " << ThreadId << " validating...");

        ui32 errors = 0;

        for (const auto& filePath: FailedToStealPaths) {
            if (!vc.StolenPaths.contains(filePath)
                    && !vc.UnlinkedPaths.contains(filePath))
            {
                STORAGE_ERROR(
                    "Failed to steal non-stolen and non-unlinked file: "
                    << filePath);
                ++errors;
            }
        }

        for (const auto& file: StolenFiles) {
            TFsPath filePath = DirPath / file.Name;

            if (NFs::Exists(filePath)) {
                TFileStat stat(filePath);
                if (stat.INode != file.Ino) {
                    STORAGE_ERROR("Inode mismatch for stolen file " << filePath
                         << " expected: " << file.Ino
                         << " got: " << stat.INode);
                    ++errors;
                }
                if (stat.Size != file.Size) {
                    STORAGE_ERROR("Size mismatch for stolen file " << filePath
                         << " expected: " << file.Size
                         << " got: " << stat.Size);
                    ++errors;
                }

                TString content = TFileInput(filePath).ReadAll();
                if (content != file.Content) {
                    STORAGE_ERROR("Content mismatch for stolen file "
                        << filePath);
                    ++errors;
                }
            }
        }

        STORAGE_INFO("Stealer " << ThreadId << " validation complete");

        return errors;
    }

private:
    void StealRandomFile()
    {
        if (Producers.empty()) {
            return;
        }

        // Pick random producer
        ui32 producerIdx = RandomNumber<ui32>(Producers.size());
        const auto& producerDir = Producers[producerIdx]->GetDirPath();

        // List files in producer directory
        TVector<TString> files;
        producerDir.ListNames(files);
        Stats.Lists.fetch_add(1);

        if (files.empty()) {
            return;
        }

        // Pick random file
        ui32 fileIdx = RandomNumber<ui32>(files.size());
        TString fileName = files[fileIdx];

        TFsPath srcPath = producerDir / fileName;

        // Move to stealer directory
        TString newFileName = TStringBuilder()
            << "stolen_p" << producerIdx << "_" << fileName;
        TFsPath dstPath = DirPath / newFileName;

        bool renamed = NFs::Rename(srcPath, dstPath);
        if (renamed) {
            // Read file info after moving
            TFileStat stat(dstPath);
            TString content = TFileInput(dstPath).ReadAll();

            StolenFiles.push_back({
                newFileName,
                stat.INode,
                stat.Size,
                content
            });

            StolenPaths.push_back(srcPath);

            Stats.Renames.fetch_add(1);
        } else {
            FailedToStealPaths.push_back(srcPath);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TApp
{
private:
    std::atomic<bool> ShouldStop{false};
    TStats Stats;
    TVector<THolder<TProducerThread>> ProducerThreads;
    TVector<THolder<TStealerThread>> StealerThreads;
    TInstant StartTime;

public:
    static TApp* GetInstance()
    {
        return Singleton<TApp>();
    }

    int Run(const TOptions& options)
    {
        // Create test directory
        MakeDirIfNotExist(options.TestDir.c_str());

        StartTime = TInstant::Now();

        TLog log = CreateComponentLog(
            "BENCH",
            std::make_shared<TStreamLogBackend>(&Cerr),
            CreateAsyncLogger());

        // Create producer threads
        TVector<TProducerThread*> producerPtrs;
        for (ui32 i = 0; i < options.ProducerThreads; ++i) {
            auto p = MakeHolder<TProducerThread>(
                log, options, i, Stats, ShouldStop);
            producerPtrs.push_back(p.Get());
            p->Start();
            ProducerThreads.push_back(std::move(p));
        }

        // Create stealer threads
        for (ui32 i = 0; i < options.StealerThreads; ++i) {
            auto s = MakeHolder<TStealerThread>(
                log, options, i, Stats, ShouldStop, producerPtrs);
            s->Start();
            StealerThreads.push_back(std::move(s));
        }

        // Wait for test duration
        while (!ShouldStop.load()) {
            Sleep(TDuration::Seconds(1));

            auto elapsed = TInstant::Now() - StartTime;
            if (elapsed.Seconds() >= options.TestDurationSec) {
                break;
            }
        }

        // Stop all threads
        ShouldStop.store(true);

        // Wait for threads to finish
        for (auto& p: ProducerThreads) {
            p->Join();
        }
        for (auto& s: StealerThreads) {
            s->Join();
        }

        TInstant endTime = TInstant::Now();
        double durationSec = (endTime - StartTime).SecondsFloat();

        // Build validation context
        TValidationContext vc;
        for (const auto& p: ProducerThreads) {
            for (const auto& fileName: p->GetUnlinkedFiles()) {
                vc.UnlinkedPaths.insert(p->GetDirPath() / fileName);
            }
        }

        for (const auto& s: StealerThreads) {
            for (const auto& filePath: s->GetStolenPaths()) {
                vc.StolenPaths.insert(filePath);
            }
        }

        Cout << "\nValidating results..." << Endl;
        for (auto& p: ProducerThreads) {
            p->Validate(vc);
        }
        for (auto& s: StealerThreads) {
            s->Validate(vc);
        }

        Cout << "\n=== Test Results ===" << Endl;
        Cout << "Duration: " << durationSec << " seconds" << Endl;
        Cout << "Total Creates: " << Stats.Creates.load() << Endl;
        Cout << "Total Unlinks: " << Stats.Unlinks.load() << Endl;
        Cout << "Total Renames: " << Stats.Renames.load() << Endl;
        Cout << "Total Lists: " << Stats.Lists.load() << Endl;
        Cout << "\n=== RPS ===" << Endl;
        Cout << "Creates RPS: " << (Stats.Creates.load() / durationSec) << Endl;
        Cout << "Unlinks RPS: " << (Stats.Unlinks.load() / durationSec) << Endl;
        Cout << "Renames RPS: " << (Stats.Renames.load() / durationSec) << Endl;
        Cout << "Lists RPS: " << (Stats.Lists.load() / durationSec) << Endl;

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
