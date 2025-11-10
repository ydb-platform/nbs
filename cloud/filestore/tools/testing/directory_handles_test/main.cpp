#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int NumTestPositions = 200;
constexpr int MinDirectories = 10000;
constexpr size_t MaxFilenameLen = 256;
constexpr int SleepIntervalMs = 100;
constexpr int MsToUs = 1000;
constexpr int NumParallelTests = 10;

////////////////////////////////////////////////////////////////////////////////

struct TDirEntry
{
    long Position;
    char Name[MaxFilenameLen];
};

struct TTestContext
{
    std::string Name;
    std::string DirectoryPath;
    std::string WaitFilePath;
    int TimeoutMs;
    int NumDirectories;

    DIR* DirHandle = nullptr;
    std::vector<TDirEntry> Entries;
    std::vector<int> TestPositions;
    bool Success = true;

    ~TTestContext()
    {
        if (DirHandle) {
            closedir(DirHandle);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

std::mutex g_OutputMutex;

////////////////////////////////////////////////////////////////////////////////

bool PrepareDirectoryData(TTestContext& ctx)
{
    // Open directory
    ctx.DirHandle = opendir(ctx.DirectoryPath.c_str());
    if (!ctx.DirHandle) {
        std::lock_guard<std::mutex> lock(g_OutputMutex);
        std::cerr << "[" << ctx.Name
                  << "] Failed to open directory: " << ctx.DirectoryPath
                  << std::endl;
        return false;
    }

    // Read and cache directory entries
    ctx.Entries.reserve(ctx.NumDirectories);
    struct dirent* entry;
    int entryCount = 0;

    while (entryCount < ctx.NumDirectories) {
        long pos = telldir(ctx.DirHandle);
        entry = readdir(ctx.DirHandle);

        if (!entry) {
            break;
        }

        if (entry->d_name[0] != '.') {
            TDirEntry dirEntry;
            dirEntry.Position = pos;
            std::strncpy(dirEntry.Name, entry->d_name, MaxFilenameLen - 1);
            dirEntry.Name[MaxFilenameLen - 1] = '\0';

            ctx.Entries.push_back(dirEntry);
            ++entryCount;
        }
    }

    if (ctx.Entries.empty()) {
        std::lock_guard<std::mutex> lock(g_OutputMutex);
        std::cerr << "[" << ctx.Name << "] No directory entries found"
                  << std::endl;
        return false;
    }

    // Generate random test positions using thread-safe random generator
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, ctx.Entries.size() - 1);

    ctx.TestPositions.resize(NumTestPositions);
    for (int i = 0; i < NumTestPositions; ++i) {
        ctx.TestPositions[i] = dis(gen);
    }

    return true;
}

bool TestDirectory(TTestContext& ctx)
{
    bool hasErrors = false;
    int timeoutRemaining = ctx.TimeoutMs;

    for (int i = 0; i < NumTestPositions; ++i) {
        int targetIdx = ctx.TestPositions[i];
        if (targetIdx >= static_cast<int>(ctx.Entries.size())) {
            continue;
        }

        // Seek to test position
        long seekPos = ctx.Entries[targetIdx].Position;
        seekdir(ctx.DirHandle, seekPos);

        // Wait for restart signal at second iteration
        if (i == 1) {
            struct stat waitSt;
            while (stat(ctx.WaitFilePath.c_str(), &waitSt) == -1 &&
                   timeoutRemaining > 0)
            {
                usleep(SleepIntervalMs * MsToUs);
                timeoutRemaining -= SleepIntervalMs;
            }

            if (timeoutRemaining <= 0) {
                std::lock_guard<std::mutex> lock(g_OutputMutex);
                std::cerr << "[" << ctx.Name
                          << "] Error: timeout waiting for file "
                          << ctx.WaitFilePath << std::endl;
                return false;
            }
        }

        // Read and verify entry
        struct dirent* entry = readdir(ctx.DirHandle);
        if (!entry) {
            std::lock_guard<std::mutex> lock(g_OutputMutex);
            std::cerr << "[" << ctx.Name
                      << "] Error: readdir returned NULL at iteration " << i
                      << ", position " << targetIdx << ", seekPos " << seekPos
                      << std::endl;
            hasErrors = true;
            continue;
        }

        if (std::strcmp(entry->d_name, ctx.Entries[targetIdx].Name) != 0) {
            std::lock_guard<std::mutex> lock(g_OutputMutex);
            std::cerr << "[" << ctx.Name << "] Error: mismatch at iteration "
                      << i << ", position " << targetIdx << ", seekPos "
                      << seekPos << " - expected '"
                      << ctx.Entries[targetIdx].Name << "', got '"
                      << entry->d_name << "'" << std::endl;
            hasErrors = true;
        }
    }

    return !hasErrors;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// this tool is used to confirm that we can use directory listing consistently
// despite vhost restart. the flow as follows:
// 1. create a directory with a lot of files
// 2. open the directory and read the files
// 3. restart the vhost
// 4. open the directory and read the files
// 5. check that the files are the same
// 6. repeat the process 10 times concurrently (this way internal cache data is
// interleaved)
int main(int argc, char** argv)
{
    if (argc != 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <directory> <wait_file> <timeout_ms> <num_directories>"
                  << std::endl;
        return 1;
    }

    std::string directoryPath = argv[1];
    std::string waitFilePath = argv[2];
    int timeoutMs = std::atoi(argv[3]);
    int numDirectories = std::atoi(argv[4]);

    if (numDirectories < MinDirectories) {
        std::cerr << "Error: Number of directories (" << numDirectories
                  << ") must be at least " << MinDirectories << std::endl;
        return 1;
    }

    if (timeoutMs <= 0) {
        std::cerr << "Error: Timeout must be positive" << std::endl;
        return 1;
    }

    // Create test contexts for the same directory (parallel testing)
    std::vector<TTestContext> contexts;

    for (int i = 1; i <= NumParallelTests; ++i) {
        TTestContext ctx;
        ctx.Name = "dir" + std::to_string(i);
        ctx.DirectoryPath = directoryPath;
        ctx.WaitFilePath = waitFilePath;
        ctx.TimeoutMs = timeoutMs;
        ctx.NumDirectories = numDirectories;
        contexts.push_back(std::move(ctx));
    }

    // Phase 1: Prepare directory data in parallel
    {
        std::vector<std::thread> threads;
        for (auto& ctx: contexts) {
            threads.emplace_back(
                [&ctx]()
                {
                    if (!PrepareDirectoryData(ctx)) {
                        ctx.Success = false;
                    }
                });
        }

        for (auto& thread: threads) {
            thread.join();
        }

        // Check if all preparations succeeded
        for (const auto& ctx: contexts) {
            if (!ctx.Success) {
                std::cerr << "[" << ctx.Name
                          << "] Failed to prepare directory data" << std::endl;
                return 1;
            }
        }
    }

    // Phase 2: Test directories in parallel
    {
        std::vector<std::thread> threads;
        for (auto& ctx: contexts) {
            threads.emplace_back([&ctx]()
                                 { ctx.Success = TestDirectory(ctx); });
        }

        for (auto& thread: threads) {
            thread.join();
        }
    }

    // Check results
    for (const auto& ctx: contexts) {
        if (!ctx.Success) {
            std::cout << "errors detected" << std::endl;
            return 1;
        }
    }

    std::cout << "Ok" << std::endl;

    return 0;
}
