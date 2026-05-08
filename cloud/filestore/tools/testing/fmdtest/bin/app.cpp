#include "app.h"

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
    std::atomic<ui64> Weight{0};
    std::atomic<ui64> TimeUs{0};

    explicit TRequestStats(TString name)
        : Name(std::move(name))
    {}

    void Register(double weight, double seconds)
    {
        Requests.fetch_add(1, std::memory_order_relaxed);
        Weight.fetch_add(weight, std::memory_order_relaxed);
        TimeUs.fetch_add(seconds * 1'000'000, std::memory_order_relaxed);
    }

    struct TLoadStat
    {
        ui64 Cnt;
        ui64 Lat;
        ui64 Weight;
        ui64 WLat;
    };

    auto Load() const
    {
        const ui64 cnt = Requests.load(std::memory_order_relaxed);
        const ui64 w = Weight.load(std::memory_order_relaxed);
        const ui64 timeUs = TimeUs.load(std::memory_order_relaxed);
        const ui64 lat = cnt ? timeUs / cnt : 0;
        const ui64 wlat = w ? timeUs / w : 0;
        return TLoadStat{.Cnt = cnt, .Lat = lat, .Weight = w, .WLat = wlat};
    }

    void Report(TLog& Log) const
    {
        const auto loadStat = Load();
        TStringBuilder sb;
        sb << Name << ": count=" << loadStat.Cnt;
        sb << ", lat-us=" << loadStat.Lat;
        if (loadStat.Weight) {
            sb << ", weight=" << loadStat.Weight;
            sb << ", wlat-us=" << loadStat.WLat;
        }

        STORAGE_INFO(sb);
    }

    void Report(NJsonWriter::TBuf& buf) const
    {
        const auto loadStat = Load();
        buf.WriteKey(Name);
        buf.WriteULongLong(loadStat.Cnt);
        buf.WriteKey(Name + "-lat-us");
        buf.WriteULongLong(loadStat.Lat);
        if (loadStat.Weight) {
            buf.WriteKey(Name + "-w");
            buf.WriteULongLong(loadStat.Weight);
            buf.WriteKey(Name + "-wlat-us");
            buf.WriteULongLong(loadStat.WLat);
        }
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
    THashMap<ui64, TFileInfo> NodeId2FileInfo;
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
    TVector<TFileInfo> AllFiles;
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
        MakeDirIfNotExist(DirPath.c_str());
    }

    void* ThreadProc() override
    {
        while (!ShouldStop.load()) {
            // Create files
            if (Files.size() < Options.FilesPerProducer) {
                CreateFile();
            }

            // Randomly delete some files
            if (!Files.empty()
                    && RandomNumber<ui32>(100) < Options.UnlinkPercentage)
            {
                DeleteRandomFile();
            }

            // Small sleep to avoid busy loop
            Sleep(Options.ProducerSleepDuration);
        }

        return nullptr;
    }

    const auto& GetAllFiles() const
    {
        return AllFiles;
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
        Stats.Create.Register(0 /* weight */, timer.Passed());

        TOFStream os(f);
        os.Write(content);
        os.Flush();

        timer.Reset();
        TFileStat stat(filePath);
        Stats.Stat.Register(0 /* weight */, timer.Passed());

        Files.push_back({
            fileName,
            stat.INode,
            stat.Size,
            content
        });

        AllFiles.push_back({
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
            Stats.Unlink.Register(0 /* weight */, timer.Passed());
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
        MakeDirIfNotExist(DirPath.c_str());
    }

    void* ThreadProc() override
    {
        while (!ShouldStop.load()) {
            StealRandomFile();
            Sleep(Options.StealerSleepDuration);
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

        for (auto& file: StolenFiles) {
            TFsPath filePath = DirPath / file.Name;

            TFileStat stat(filePath);
            if (stat.INode != file.Ino) {
                STORAGE_ERROR("Inode mismatch for stolen file " << filePath
                        << " expected: " << file.Ino
                        << " got: " << stat.INode);
                ++errors;
            }

            const auto* fileInfo = vc.NodeId2FileInfo.FindPtr(file.Ino);
            if (!fileInfo) {
                STORAGE_ERROR("Stolen file " << filePath << ", " << file.Ino
                        << " not found in producer files");
                ++errors;
                continue;
            }

            file = *fileInfo;

            if (stat.Size != file.Size) {
                STORAGE_ERROR("Size mismatch for stolen file " << filePath
                        << " expected: " << file.Size
                        << " got: " << stat.Size);
                ++errors;
            }

            TString content = TFileInput(filePath).ReadAll();
            if (content != file.Content) {
                STORAGE_ERROR("Content mismatch for stolen file " << filePath);
                ++errors;
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
        THPTimer timer;
        producerDir.ListNames(files);
        Stats.List.Register(files.size() /* weight */, timer.Passed());

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

        timer.Reset();
        bool renamed = NFs::Rename(srcPath, dstPath);
        if (renamed) {
            Stats.Rename.Register(0 /* weight */, timer.Passed());

            //
            // Stat and file info after moving
            //

            timer.Reset();
            TFileStat stat(dstPath);
            Stats.Stat.Register(0 /* weight */, timer.Passed());

            StolenFiles.push_back({
                newFileName,
                stat.INode,
                0 /* Size */,
                "" /* Content */,
            });

            StolenPaths.push_back(srcPath);
        } else {
            FailedToStealPaths.push_back(srcPath);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListerThread: public ISimpleThread
{
private:
    TLog Log;
    const TOptions& Options;
    TStats& Stats;
    std::atomic<bool>& ShouldStop;
    const TVector<TProducerThread*>& Producers;

public:
    TListerThread(
            TLog log,
            const TOptions& options,
            TStats& stats,
            std::atomic<bool>& shouldStop,
            const TVector<TProducerThread*>& producers)
        : Log(std::move(log))
        , Options(options)
        , Stats(stats)
        , ShouldStop(shouldStop)
        , Producers(producers)
    {
    }

    void* ThreadProc() override
    {
        while (!ShouldStop.load()) {
            ListRandomDir();
            Sleep(Options.ListerSleepDuration);
        }

        return nullptr;
    }

private:
    void ListRandomDir()
    {
        if (Producers.empty()) {
            return;
        }

        // Pick random producer
        ui32 producerIdx = RandomNumber<ui32>(Producers.size());
        const auto& producerDir = Producers[producerIdx]->GetDirPath();

        // List files in producer directory
        TVector<TString> files;
        THPTimer timer;
        producerDir.ListNames(files);
        Stats.List.Register(files.size() /* weight */, timer.Passed());
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
    TVector<THolder<TListerThread>> ListerThreads;
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
            auto p = MakeHolder<TProducerThread>(
                Log, options, i, Stats, ShouldStop);
            producerPtrs.push_back(p.Get());
            ProducerThreads.push_back(std::move(p));
        }

        // Create stealer threads
        for (ui32 i = 0; i < options.StealerThreads; ++i) {
            auto s = MakeHolder<TStealerThread>(
                Log, options, i, Stats, ShouldStop, producerPtrs);
            StealerThreads.push_back(std::move(s));
        }

        // Create lister threads
        for (ui32 i = 0; i < options.ListerThreads; ++i) {
            auto s = MakeHolder<TListerThread>(
                Log, options, Stats, ShouldStop, producerPtrs);
            ListerThreads.push_back(std::move(s));
        }

        // Run all threads
        for (auto& p: ProducerThreads) {
            p->Start();
        }

        for (auto& s: StealerThreads) {
            s->Start();
        }

        for (auto& l: ListerThreads) {
            l->Start();
        }

        // Wait for test duration
        while (!ShouldStop.load()) {
            Sleep(TDuration::Seconds(1));

            auto elapsed = TInstant::Now() - StartTime;
            if (elapsed >= options.TestDuration) {
                break;
            }

            Stats.Create.Report(Log);
            Stats.Unlink.Report(Log);
            Stats.Rename.Report(Log);
            Stats.Stat.Report(Log);
            Stats.List.Report(Log);
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

        for (auto& l: ListerThreads) {
            l->Join();
        }

        TInstant endTime = TInstant::Now();
        double durationSec = (endTime - StartTime).SecondsFloat();

        // Build validation context
        TValidationContext vc;
        for (const auto& p: ProducerThreads) {
            for (const auto& fileInfo: p->GetAllFiles()) {
                vc.NodeId2FileInfo[fileInfo.Ino] = fileInfo;
            }

            for (const auto& fileName: p->GetUnlinkedFiles()) {
                vc.UnlinkedPaths.insert(p->GetDirPath() / fileName);
            }
        }

        for (const auto& s: StealerThreads) {
            for (const auto& filePath: s->GetStolenPaths()) {
                vc.StolenPaths.insert(filePath);
            }
        }

        STORAGE_INFO("Validating results...");
        ui32 errors = 0;
        for (auto& p: ProducerThreads) {
            errors += p->Validate(vc);
        }
        for (auto& s: StealerThreads) {
            errors += s->Validate(vc);
        }

        STORAGE_INFO("=== Test Results ===");
        STORAGE_INFO("Duration: " << durationSec << " seconds");
        Stats.Create.Report(Log);
        Stats.Unlink.Report(Log);
        Stats.Rename.Report(Log);
        Stats.Stat.Report(Log);
        Stats.List.Report(Log);

        WriteReport(Log, options.ReportPath, errors);

        return errors == 0 ? 0 : 1;
    }

    void WriteReport(TLog& Log, const TString& reportPath, ui32 errors) const
    {
        TOFStream os(reportPath);
        NJsonWriter::TBuf buf(NJsonWriter::HEM_DONT_ESCAPE_HTML, &os);
        buf.SetIndentSpaces(4);

        buf.BeginObject();
        Stats.Create.Report(buf);
        Stats.Unlink.Report(buf);
        Stats.Rename.Report(buf);
        Stats.Stat.Report(buf);
        Stats.List.Report(buf);
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
