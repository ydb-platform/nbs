// g++ -O3 -o open_close_bench open_close_bench.cpp

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <future>
#include <string>
#include <system_error>
#include <vector>

const auto makeFilename = [](const auto& i)
{
    return "file" + std::to_string(i + 1) + ".txt";
};

int main(int argc, char* argv[])
{
    bool keepOpen = false;
    bool remove = false;
    bool verbose = false;
    int fallocateBytes = 0;
    int n = 0;
    int parallel = 0;
    int sleepOpenClose = 0;
    int writeBytes = 0;

    int opt;

    const auto usage = [&argv]()
    {
        fprintf(
            stderr,
            "Usage: %s -n <number_of_files> [-j <jobs>] [-f "
            "<fallocate_bytes>] [-w <write_bytes>] [-v] [-r] [-k]\n"
            "-v \t Verbose\n"
            "-r \t Remove files after test\n"
            "-k \t Keep files open after create\n",
            argv[0]);
    };

    while ((opt = getopt(argc, argv, "vrkn:j:w:f:s:")) != -1) {
        switch (opt) {
            case 'v':
                verbose = true;
                break;
            case 'n':
                n = atoi(optarg);
                break;
            case 'j':
                parallel = atoi(optarg);
                break;
            case 'w':
                writeBytes = atoi(optarg);
                break;
            case 'f':
                fallocateBytes = atoi(optarg);
                break;
            case 's':
                sleepOpenClose = atoi(optarg);
                break;
            case 'r':
                remove = true;
                break;
            case 'k':
                keepOpen = true;
                break;
            default:
                usage();
                return EXIT_FAILURE;
        }
    }

    if (n <= 0) {
        fprintf(
            stderr,
            "Please specify a number of files using the -n option.\n");
        usage();
        return EXIT_FAILURE;
    }

    try {
        std::vector<int> fds(n);
        printf(
            "Number of files: %zu, parallel: %d, write: %d, fallocate: %d, "
            "keep: %d, sleep: %d, remove: %d\n",
            fds.size(),
            parallel,
            writeBytes,
            fallocateBytes,
            keepOpen,
            sleepOpenClose,
            remove);
        fflush(stdout);

        const auto forEach = [&parallel](auto& container, const auto func)
        {
            if (parallel <= 1) {
                size_t i = 0;
                for (auto& elem: container) {
                    func(i++, elem);
                }
                return;
            }

            const auto total = container.size();
            if (total <= 0) {
                return;
            }

            size_t threads = std::min<size_t>(parallel, total);
            std::vector<std::future<void>> futures;
            futures.reserve(threads);

            auto chunkStart = container.begin();
            size_t currentNumber = 0;
            for (size_t thread = 0; thread < threads; ++thread) {
                auto chunkEnd = chunkStart;
                const auto chunkSize =
                    total / threads + (thread < (total % threads) ? 1 : 0);
                std::advance(chunkEnd, chunkSize);

                futures.emplace_back(std::async(
                    std::launch::async,
                    [chunkStart, chunkEnd, &func, currentNumber]()
                    {
                        size_t thread = 0;
                        for (auto it = chunkStart; it != chunkEnd;
                             ++it, ++thread) {
                            func(currentNumber + thread, *it);
                        }
                    }));

                currentNumber += chunkSize;
                chunkStart = chunkEnd;
            }

            for (auto& fu: futures) {
                fu.get();
                // rethrow exceptions if any
            }
        };

        const auto bench = [&](const auto& name, const auto& func)
        {
            struct timespec startTime, endTime;
            clock_gettime(CLOCK_MONOTONIC, &startTime);

            func();

            clock_gettime(CLOCK_MONOTONIC, &endTime);
            double closeTime = (endTime.tv_sec - startTime.tv_sec) +
                               (endTime.tv_nsec - startTime.tv_nsec) / 1e9;
            printf("%s \t%.6f ms \t", name, closeTime * 1000);
            printf("avg %.6f ms\n", closeTime * 1000 / n);
            fflush(stdout);
        };

        bench(
            "Create",
            [&]()
            {
                forEach(
                    fds,
                    [&writeBytes, &verbose, &fallocateBytes, &keepOpen](
                        const auto i,
                        auto& fd)
                    {
                        const auto filename = makeFilename(i);
                        // Try to open the file in read-only mode to check if it
                        // exists
                        fd = open(filename.c_str(), O_RDONLY);
                        if (fd < 0) {
                            // File doesn't exist, create it
                            fd = open(
                                filename.c_str(),
                                O_WRONLY | O_CREAT,
                                S_IRUSR | S_IWUSR);
                            if (fd < 0) {
                                fprintf(
                                    stderr,
                                    "Error creating file %s : %s\n",
                                    filename.c_str(),
                                    strerror(errno));
                                throw std::system_error(
                                    errno,
                                    std::generic_category());
                            }

                            if (verbose) {
                                printf("File %s created.\n", filename.c_str());
                            }

                            if (fallocateBytes) {
                                const auto res =
                                    fallocate(fd, 0, 0, fallocateBytes);
                                if (res < 0) {
                                    fprintf(
                                        stderr,
                                        "Error fallocating file %s : %s\n",
                                        filename.c_str(),
                                        strerror(errno));
                                    close(fd);
                                    throw std::system_error(
                                        errno,
                                        std::generic_category());
                                }
                            }

                            if (writeBytes) {
                                std::string buf(writeBytes, 'A');
                                const auto written =
                                    write(fd, buf.data(), buf.size());
                                if (written < 0 ||
                                    static_cast<size_t>(written) != buf.size())
                                {
                                    fprintf(
                                        stderr,
                                        "Error writing file %s : %s\n",
                                        filename.c_str(),
                                        strerror(errno));
                                    close(fd);
                                    throw std::system_error(
                                        errno,
                                        std::generic_category());
                                }
                            }
                        }

                        if (!keepOpen) {
                            // If file already exists, close it
                            close(fd);
                        }
                    });
            });

        if (!keepOpen) {
            bench(
                "Open",
                [&]()
                {
                    forEach(
                        fds,
                        [verbose](const auto i, auto& fd)
                        {
                            const auto filename = makeFilename(i);
                            fd = open(filename.c_str(), O_RDONLY);
                            if (verbose) {
                                fprintf(
                                    stderr,
                                    "File %s opened n=%lu fd=%d \n",
                                    filename.c_str(),
                                    i,
                                    fd);
                            }
                            if (fd < 0) {
                                fprintf(
                                    stderr,
                                    "Error opening file %s : %s\n",
                                    filename.c_str(),
                                    strerror(errno));
                                throw std::system_error(
                                    errno,
                                    std::generic_category());
                            }
                        });
                });
        }

        if (sleepOpenClose) {
            printf("Sleeping %d seconds\n", sleepOpenClose);
            fflush(stdout);
            sleep(sleepOpenClose);
        }

        bench(
            "Close",
            [&]()
            {
                forEach(
                    fds,
                    [](const auto i, auto& fd)
                    {
                        const auto result = close(fd);
                        if (result < 0) {
                            fprintf(
                                stderr,
                                "Error closing file number %lu fd %d: %s "
                                "(%d)\n",
                                i,
                                fd,
                                strerror(errno),
                                result);
                            throw std::system_error(
                                errno,
                                std::generic_category());
                        }
                    });
            });

        if (remove) {
            bench(
                "Remove",
                [&]()
                {
                    forEach(
                        fds,
                        [](const auto i, auto& fd)
                        {
                            const auto filename = makeFilename(i);
                            const auto result = unlink(filename.c_str());
                            if (result < 0) {
                                fprintf(
                                    stderr,
                                    "Error closing file number %lu fd %d: %s "
                                    "(%d)\n",
                                    i,
                                    fd,
                                    strerror(errno),
                                    result);
                                throw std::system_error(
                                    errno,
                                    std::generic_category());
                            }
                        });
                });
        }
    } catch (const std::system_error& ex) {
        fprintf(
            stderr,
            "Exception: %d, %s, %s",
            ex.code().value(),
            ex.code().message().c_str(),
            ex.what());
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
