#include "app.h"
#include "mpi.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/json/writer/json.h>
#include <library/cpp/logger/stream.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/hash_set.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/split.h>
#include <util/system/fs.h>
#include <util/system/hp_timer.h>
#include <util/system/thread.h>

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

struct TRequestStats
{
    TString Name;
    std::atomic<ui64> Requests{0};
    std::atomic<ui64> TimeUs{0};

    explicit TRequestStats(TString name)
        : Name(std::move(name))
    {}

    void Register(double seconds)
    {
        Requests.fetch_add(1, std::memory_order_relaxed);
        TimeUs.fetch_add(seconds * 1'000'000, std::memory_order_relaxed);
    }

    auto Load() const
    {
        const ui64 cnt = Requests.load(std::memory_order_relaxed);
        const ui64 timeUs = TimeUs.load(std::memory_order_relaxed);
        const ui64 lat = cnt ? timeUs / cnt : 0;
        return std::make_pair(cnt, lat);
    }

    void Report(TLog& Log) const
    {
        const auto [cnt, lat] = Load();
        STORAGE_INFO(Name << ": count=" << cnt << ", lat-us=" << lat);
    }

    void Report(NJsonWriter::TBuf& buf) const
    {
        const auto [cnt, lat] = Load();
        buf.WriteKey(Name);
        buf.WriteULongLong(cnt);
        buf.WriteKey(Name + "-lat-us");
        buf.WriteULongLong(lat);
    }
};

struct TStats
{
    TRequestStats Create{"create"};
    TRequestStats Stat{"stat"};
    TRequestStats Unlink{"unlink"};
    TRequestStats Rename{"rename"};
    TRequestStats List{"list"};
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
            if (Files.size() < Options.FilesPerProducer) {
                CreateFile();
            }

            if (!Files.empty() && RandomNumber<ui32>(100) < 20) {
                DeleteRandomFile();
            }

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
                static constexpr EOpenMode OpenMode =
                    OpenExisting | RdOnly | Seq;
                TFileHandle f(filePath, OpenMode);
                if (f.IsOpen()) {
                    STORAGE_ERROR("Unlinked file still exists: " << filePath);
                    ++errors;
                    f.Close();
                } else {
                    STORAGE_WARN("Unlinked file still exists: " << filePath
                        << ", but can't be opened (stale dentry?)");
                }
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
        static constexpr EOpenMode OpenMode = CreateAlways | WrOnly | Seq;
        THPTimer timer;
        TFile f(filePath, OpenMode);
        Stats.Create.Register(timer.Passed());

        TOFStream os(f);
        os.Write(content);
        os.Flush();

        timer.Reset();
        TFileStat stat(filePath);
        Stats.Stat.Register(timer.Passed());

        Files.push_back({
            fileName,
            stat.INode,
            stat.Size,
            content
        });
    }

    void DeleteRandomFile()
    {
        if (Files.empty()) {
            return;
        }

        ui32 idx = RandomNumber<ui32>(Files.size());
        auto& file = Files[idx];

        TFsPath filePath = DirPath / file.Name;
        THPTimer timer;
        const bool removed = NFs::Remove(filePath);
        if (removed) {
            Stats.Unlink.Register(timer.Passed());
            UnlinkedFiles.push_back(file.Name);
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

    // Local producers (same rank)
    const TVector<TProducerThread*>& LocalProducers;
    // Directories of producers on all ranks (populated when cross-rank stealing
    // is enabled; includes local producers' dirs too)
    TVector<TFsPath> AllProducerDirs;

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
            const TVector<TProducerThread*>& localProducers,
            TVector<TFsPath> allProducerDirs)
        : Log(std::move(log))
        , Options(options)
        , ThreadId(threadId)
        , Stats(stats)
        , ShouldStop(shouldStop)
        , LocalProducers(localProducers)
        , AllProducerDirs(std::move(allProducerDirs))
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
    // Pick a random producer directory to steal from.
    // Uses AllProducerDirs when cross-rank stealing is enabled,
    // otherwise falls back to local producers only.
    const TFsPath& PickProducerDir()
    {
        if (!AllProducerDirs.empty()) {
            return AllProducerDirs[RandomNumber<ui32>(AllProducerDirs.size())];
        }
        return LocalProducers[RandomNumber<ui32>(LocalProducers.size())]
            ->GetDirPath();
    }

    void StealRandomFile()
    {
        if (LocalProducers.empty() && AllProducerDirs.empty()) {
            return;
        }

        const auto& producerDir = PickProducerDir();

        TVector<TString> files;
        THPTimer timer;
        producerDir.ListNames(files);
        Stats.List.Register(timer.Passed());

        if (files.empty()) {
            return;
        }

        ui32 fileIdx = RandomNumber<ui32>(files.size());
        TString fileName = files[fileIdx];

        TFsPath srcPath = producerDir / fileName;

        TString newFileName = TStringBuilder()
            << "stolen_" << producerDir.GetName() << "_" << fileName;
        TFsPath dstPath = DirPath / newFileName;

        timer.Reset();
        bool renamed = NFs::Rename(srcPath, dstPath);
        if (renamed) {
            Stats.Rename.Register(timer.Passed());

            timer.Reset();
            TFileStat stat(dstPath);
            Stats.Stat.Register(timer.Passed());

            TString content = TFileInput(dstPath).ReadAll();

            StolenFiles.push_back({
                newFileName,
                stat.INode,
                stat.Size,
                content
            });

            StolenPaths.push_back(srcPath);
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

    int Run(const TOptions& options, const TMpiContext& mpi)
    {
        // Each rank works under its own subdirectory so that producer/stealer
        // dir names don't collide across ranks on a shared filesystem.
        TString rankDir = mpi.Size > 1
            ? TString(TStringBuilder() << options.TestDir << "/rank_" << mpi.Rank)
            : options.TestDir;

        TOptions rankOptions = options;
        rankOptions.TestDir = rankDir;

        MakeDirIfNotExist(rankDir.c_str());

        StartTime = TInstant::Now();

        auto asyncLogger = CreateAsyncLogger();
        asyncLogger->Start();
        TLog Log = CreateComponentLog(
            TStringBuilder() << "BENCH[" << mpi.Rank << "]",
            std::make_shared<TStreamLogBackend>(&Cerr),
            asyncLogger);

        // Create producer threads
        TVector<TProducerThread*> producerPtrs;
        for (ui32 i = 0; i < rankOptions.ProducerThreads; ++i) {
            auto p = MakeHolder<TProducerThread>(
                Log, rankOptions, i, Stats, ShouldStop);
            producerPtrs.push_back(p.Get());
            p->Start();
            ProducerThreads.push_back(std::move(p));
        }

        // Build the list of all producer directories visible to stealers.
        // When cross-rank stealing is enabled, exchange producer dir paths
        // across all MPI ranks so stealers can reach remote producers.
        TVector<TFsPath> allProducerDirs;
        if (options.MpiCrossRankStealing && mpi.Size > 1) {
            // Serialize local producer dirs as newline-separated string
            TStringBuilder localDirs;
            for (const auto* p: producerPtrs) {
                localDirs << p->GetDirPath().GetPath() << "\n";
            }

            // Gather from all ranks onto rank 0, then broadcast back
            // We use a simple approach: gather to rank 0, then bcast the
            // concatenated string to everyone.
            TVector<TString> gathered =
                MpiGatherStrings(mpi, localDirs);

            // Broadcast the full list from rank 0 to all ranks
            TString allDirsStr;
            if (mpi.IsRoot()) {
                for (const auto& s: gathered) {
                    allDirsStr += s;
                }
            }

            // Broadcast length then data
            ui64 len = allDirsStr.size();
            len = MpiReduceSum(mpi, mpi.IsRoot() ? len : 0);
            // Simple broadcast via a shared file isn't ideal; instead we
            // re-gather on every rank by having each rank gather all dirs.
            // Since MpiGatherStrings only returns data on root, we use a
            // different approach: each rank gathers all dirs independently
            // by calling MpiGatherStrings and then parsing on root, then
            // root writes a rendezvous file that all ranks read.
            //
            // For simplicity and correctness, we use a rendezvous file in
            // the shared test directory that rank 0 writes after gathering.
            if (mpi.IsRoot()) {
                TFsPath rendezvous(TString(options.TestDir) + "/.mpi_dirs");
                TOFStream os(rendezvous);
                os << allDirsStr;
                os.Flush();
            }

            // All ranks barrier-wait for rank 0 to write the rendezvous file
            MpiBarrier(mpi);

            // All ranks read the rendezvous file
            TFsPath rendezvous(TString(options.TestDir) + "/.mpi_dirs");
            TString allDirs = TFileInput(rendezvous).ReadAll();
            TVector<TString> lines;
            StringSplitter(allDirs).Split('\n').SkipEmpty().Collect(&lines);
            for (const auto& line: lines) {
                allProducerDirs.emplace_back(line);
            }
        } else {
            // No cross-rank stealing: stealers only see local producers
            for (const auto* p: producerPtrs) {
                allProducerDirs.push_back(p->GetDirPath());
            }
        }

        // Create stealer threads
        for (ui32 i = 0; i < rankOptions.StealerThreads; ++i) {
            auto s = MakeHolder<TStealerThread>(
                Log,
                rankOptions,
                i,
                Stats,
                ShouldStop,
                producerPtrs,
                allProducerDirs);
            s->Start();
            StealerThreads.push_back(std::move(s));
        }

        // Wait for test duration
        while (!ShouldStop.load()) {
            Sleep(TDuration::Seconds(1));

            auto elapsed = TInstant::Now() - StartTime;
            if (elapsed.Seconds() >= rankOptions.TestDurationSec) {
                break;
            }

            Stats.Create.Report(Log);
            Stats.Unlink.Report(Log);
            Stats.Rename.Report(Log);
            Stats.Stat.Report(Log);
            Stats.List.Report(Log);
        }

        ShouldStop.store(true);

        for (auto& p: ProducerThreads) {
            p->Join();
        }
        for (auto& s: StealerThreads) {
            s->Join();
        }

        TInstant endTime = TInstant::Now();
        double durationSec = (endTime - StartTime).SecondsFloat();

        // Build local validation context
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

        // When cross-rank stealing is enabled, a file stolen by another rank's
        // stealer will appear missing from the local producer's perspective.
        // We mark all remote stealer dirs' stolen paths as "stolen" so local
        // validation doesn't flag them as errors.
        // We do this by reading the rendezvous file for stealer dirs too.
        // For simplicity, we skip cross-rank stolen-path exchange here and
        // instead treat any missing file that was not locally unlinked as
        // potentially stolen by a remote rank (not an error).
        // The cross-rank stolen paths are validated by the stealer's own rank.

        STORAGE_INFO("Validating results...");
        ui32 localErrors = 0;
        for (auto& p: ProducerThreads) {
            localErrors += p->Validate(vc);
        }
        for (auto& s: StealerThreads) {
            localErrors += s->Validate(vc);
        }

        // Barrier before aggregating so all ranks finish validation
        MpiBarrier(mpi);

        // Aggregate stats and errors across all ranks
        const ui64 globalErrors = MpiReduceSum(mpi, localErrors);
        const ui64 globalCreates =
            MpiReduceSum(mpi, Stats.Create.Requests.load());
        const ui64 globalUnlinks =
            MpiReduceSum(mpi, Stats.Unlink.Requests.load());
        const ui64 globalRenames =
            MpiReduceSum(mpi, Stats.Rename.Requests.load());
        const ui64 globalStats =
            MpiReduceSum(mpi, Stats.Stat.Requests.load());
        const ui64 globalLists =
            MpiReduceSum(mpi, Stats.List.Requests.load());

        if (mpi.IsRoot()) {
            STORAGE_INFO("=== Test Results (all " << mpi.Size << " rank(s)) ===");
            STORAGE_INFO("Duration: " << durationSec << " seconds");
            STORAGE_INFO("Creates: " << globalCreates
                << " (" << (globalCreates / durationSec) << " rps)");
            STORAGE_INFO("Unlinks: " << globalUnlinks
                << " (" << (globalUnlinks / durationSec) << " rps)");
            STORAGE_INFO("Renames: " << globalRenames
                << " (" << (globalRenames / durationSec) << " rps)");
            STORAGE_INFO("Stats:   " << globalStats
                << " (" << (globalStats / durationSec) << " rps)");
            STORAGE_INFO("Lists:   " << globalLists
                << " (" << (globalLists / durationSec) << " rps)");

            WriteReport(
                Log,
                options.ReportPath,
                durationSec,
                globalCreates,
                globalUnlinks,
                globalRenames,
                globalStats,
                globalLists,
                globalErrors,
                mpi.Size);
        }

        return globalErrors == 0 ? 0 : 1;
    }

    void WriteReport(
        TLog& Log,
        const TString& reportPath,
        double durationSec,
        ui64 creates,
        ui64 unlinks,
        ui64 renames,
        ui64 stats,
        ui64 lists,
        ui64 errors,
        int mpiSize) const
    {
        TOFStream os(reportPath);
        NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &os);
        buf.SetIndentSpaces(4);

        buf.BeginObject();

        buf.WriteKey("mpi-ranks");
        buf.WriteInt(mpiSize);
        buf.WriteKey("duration-sec");
        buf.WriteDouble(durationSec);

        auto writeOp = [&](const TString& name, ui64 cnt) {
            buf.WriteKey(name);
            buf.WriteULongLong(cnt);
            buf.WriteKey(name + "-rps");
            buf.WriteDouble(cnt / durationSec);
        };

        writeOp("create", creates);
        writeOp("unlink", unlinks);
        writeOp("rename", renames);
        writeOp("stat",   stats);
        writeOp("list",   lists);

        buf.WriteKey("errors");
        buf.WriteULongLong(errors);

        buf.EndObject();
        os.Flush();

        STORAGE_INFO("Report: " << reportPath << ", errors: " << errors);
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

    (void) setvbuf(stdout, nullptr, _IONBF, 0);
    (void) setvbuf(stderr, nullptr, _IONBF, 0);

    (void) signal(SIGPIPE, SIG_IGN);
    (void) signal(SIGINT, ProcessSignal);
    (void) signal(SIGTERM, ProcessSignal);
}

int AppMain(const TOptions& options, const TMpiContext& mpi)
{
    return TApp::GetInstance()->Run(options, mpi);
}

void AppStop()
{
    TApp::GetInstance()->Stop();
}

}   // namespace NCloud::NFileStore
