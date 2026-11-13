#include "guarded_sglist.h"
#include "sglist.h"

#include <library/cpp/digest/crc32c/crc32c.h>
#include <library/cpp/testing/unittest/registar.h>

#include <atomic>
#include <chrono>
#include <new>
#include <stdexcept>
#include <thread>

#if defined(__linux__)
#include <cerrno>
#include <csignal>
#include <sys/mman.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

void CheckBlockChecksums(
    const TVector<ui32>& checksums,
    TBlockDataRef data,
    ui32 blockSize)
{
    UNIT_ASSERT_VALUES_EQUAL(checksums.size(), data.Size() / blockSize);
    for (size_t i = 0; i < checksums.size(); ++i) {
        // ComputeDefaultDigest uses this same CRC32C entry point. Depending on
        // BlockStore diagnostics here would invert the common-library layering.
        UNIT_ASSERT_VALUES_EQUAL(
            checksums[i],
            Crc32c(data.Data() + i * blockSize, blockSize));
    }
}

void CheckCopy(
    ui32 blockSize,
    size_t blocksCount,
    const TVector<size_t>& fragmentSizes,
    size_t sourceAlignment,
    size_t destinationAlignment,
    bool zeroAlternateFragments = false)
{
    const size_t size = blockSize * blocksCount;
    TString source = TString::Uninitialized(size + sourceAlignment);
    char* const sourceData = source.begin() + sourceAlignment;
    for (size_t i = 0; i < size; ++i) {
        sourceData[i] = static_cast<char>((i * 17 + i / blockSize * 31) % 251);
    }

    TSgList fragments;
    fragments.emplace_back();
    size_t offset = 0;
    size_t index = 0;
    while (offset < size) {
        const size_t requested = fragmentSizes[index % fragmentSizes.size()];
        const size_t count = Min(requested, size - offset);
        UNIT_ASSERT(count);
        if (zeroAlternateFragments && index % 2) {
            fragments.push_back(TBlockDataRef::CreateZeroBlock(count));
        } else {
            fragments.emplace_back(sourceData + offset, count);
        }
        fragments.emplace_back();
        offset += count;
        ++index;
    }

    TString expected(size, '\0');
    UNIT_ASSERT_VALUES_EQUAL(
        SgListCopy(fragments, {expected.data(), expected.size()}),
        size);

    const size_t suffix = 19;
    TString destination(size + destinationAlignment + suffix, '\x5a');
    char* const destinationData = destination.begin() + destinationAlignment;
    const auto checksums = SgListCopyWithBlockChecksums(
        fragments,
        {destinationData, size},
        blockSize);

    UNIT_ASSERT_VALUES_EQUAL(
        TStringBuf(destinationData, size),
        TStringBuf(expected));
    UNIT_ASSERT_VALUES_EQUAL(
        TStringBuf(destination.data(), destinationAlignment),
        TString(destinationAlignment, '\x5a'));
    UNIT_ASSERT_VALUES_EQUAL(
        TStringBuf(destinationData + size, suffix),
        TString(suffix, '\x5a'));
    CheckBlockChecksums(checksums, {destinationData, size}, blockSize);
}

#if defined(__linux__)
// Model externally mutable guest memory with another process, not a C++ data
// race between threads. Only the source mapping and lock-free IPC are shared;
// the final destination belongs exclusively to the parent.
class TGuestMemory
{
private:
    struct TControl
    {
        std::atomic<ui32> Ready = 0;
        std::atomic<ui32> Stop = 0;
        std::atomic<ui32> Passes = 0;
    };

    static_assert(std::atomic<ui32>::is_always_lock_free);
    static constexpr size_t HeaderSize = 4096;

    const size_t PayloadSize;
    void* Mapping = MAP_FAILED;
    TControl* Control = nullptr;
    pid_t Child = -1;
    int ExitStatus = -1;

public:
    explicit TGuestMemory(size_t payloadSize)
        : PayloadSize(payloadSize)
    {
        Mapping = mmap(nullptr, HeaderSize + PayloadSize,
            PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (Mapping == MAP_FAILED) {
            throw std::runtime_error("guest-source mmap failed");
        }
        Control = new (Mapping) TControl;
        memset(static_cast<char*>(Mapping) + HeaderSize, '\x11', PayloadSize);
        Child = fork();
        if (Child == 0) {
            RunChild();
        }
        if (Child < 0) {
            Control->~TControl();
            munmap(Mapping, HeaderSize + PayloadSize);
            Mapping = MAP_FAILED;
            throw std::runtime_error("guest-source fork failed");
        }
    }

    TGuestMemory(const TGuestMemory&) = delete;
    TGuestMemory& operator=(const TGuestMemory&) = delete;

    ~TGuestMemory()
    {
        StopAndWait();
        if (Mapping != MAP_FAILED) {
            Control->~TControl();
            munmap(Mapping, HeaderSize + PayloadSize);
        }
    }

    const char* Data() const
    {
        return static_cast<const char*>(Mapping) + HeaderSize;
    }

    ui32 Passes() const
    {
        return Control->Passes.load(std::memory_order_acquire);
    }

    bool IsRunning()
    {
        if (Child < 0) {
            return false;
        }
        const pid_t result = waitpid(Child, &ExitStatus, WNOHANG);
        if (result == 0 || (result < 0 && errno == EINTR)) {
            return true;
        }
        Child = -1;
        return false;
    }

    void WaitReady()
    {
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::seconds(10);
        while (!Control->Ready.load(std::memory_order_acquire)) {
            if (!IsRunning() || std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("guest writer failed to become ready");
            }
            usleep(1000);
        }
    }

    void WaitForProgress(ui32 previous)
    {
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::seconds(10);
        while (Passes() == previous) {
            if (!IsRunning() || std::chrono::steady_clock::now() >= deadline) {
                throw std::runtime_error("guest writer stopped making progress");
            }
            usleep(1000);
        }
    }

    void StopAndWait() noexcept
    {
        if (Child < 0) {
            return;
        }
        Control->Stop.store(1, std::memory_order_release);
        const auto deadline = std::chrono::steady_clock::now() +
            std::chrono::seconds(5);
        while (std::chrono::steady_clock::now() < deadline) {
            if (!IsRunning()) {
                return;
            }
            usleep(1000);
        }
        // Also runs during assertion/exception unwinding. The child contains
        // no blocking operation; SIGKILL bounds cleanup if its stop is missed.
        kill(Child, SIGKILL);
        while (waitpid(Child, &ExitStatus, 0) < 0 && errno == EINTR) {}
        Child = -1;
    }

    int Status() const
    {
        return ExitStatus;
    }

private:
    [[noreturn]] void RunChild() noexcept
    {
        volatile unsigned char* const source =
            reinterpret_cast<volatile unsigned char*>(
                static_cast<char*>(Mapping) + HeaderSize);
        unsigned char value = 0x55;
        while (!Control->Stop.load(std::memory_order_acquire)) {
            for (size_t offset = 0; offset < PayloadSize; ++offset) {
                if (!(offset % 4096) &&
                    Control->Stop.load(std::memory_order_acquire))
                {
                    _exit(0);
                }
                source[offset] = value;
            }
            Control->Passes.fetch_add(1, std::memory_order_release);
            Control->Ready.store(1, std::memory_order_release);
            value = value == 0x55 ? 0xaa : 0x55;
        }
        // No allocations, locks, logging, exceptions, or C++ destruction in
        // the child after fork; _exit avoids inherited runtime destructors.
        _exit(0);
    }
};
#endif

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSgListBlockChecksumsTest)
{
    Y_UNIT_TEST(ShouldAcceptEmptyInputAndEmptyFragments)
    {
        const TSgList emptyFragments = {
            {}, TBlockDataRef::CreateZeroBlock(0), {}};
        UNIT_ASSERT(
            SgListCopyWithBlockChecksums(emptyFragments, {}, 4096).empty());
        UNIT_ASSERT(
            SgListCopyWithBlockChecksums(TBlockDataRefSpan{}, {}, 4096).empty());
    }

    Y_UNIT_TEST(ShouldCopySingleAndMultipleBlocks)
    {
        for (const ui32 blockSize: {512u, 4096u, 65536u, 131072u}) {
            for (const size_t count: {1u, 2u, 33u}) {
                CheckCopy(blockSize, count, {blockSize * count}, 0, 0);
            }
        }
    }

    Y_UNIT_TEST(ShouldCrossFragmentAndLogicalBlockBoundaries)
    {
        for (const ui32 blockSize: {512u, 4096u, 65536u, 131072u}) {
            CheckCopy(blockSize, 3, {blockSize - 1, 1}, 0, 0);
            CheckCopy(blockSize, 3, {1, blockSize - 1}, 0, 0);
            CheckCopy(blockSize, 7, {13, blockSize * 2 + 7, 1, 173}, 0, 0);
        }
    }

    Y_UNIT_TEST(ShouldSupportUnalignedSourceAndDestination)
    {
        for (const size_t sourceOffset: {0u, 1u, 7u, 15u, 63u}) {
            for (const size_t destinationOffset: {0u, 1u, 3u, 17u, 63u}) {
                CheckCopy(4096, 3, {1, 4095, 63, 4097},
                    sourceOffset, destinationOffset);
            }
        }
    }

    Y_UNIT_TEST(ShouldMaterializeZeroAndMixedSegments)
    {
        for (const ui32 blockSize: {512u, 4096u, 131072u}) {
            CheckCopy(blockSize, 5, {1, blockSize - 1, blockSize + 7},
                1, 3, true);
            const TSgList zeroes = {
                {}, TBlockDataRef::CreateZeroBlock(blockSize - 1), {},
                TBlockDataRef::CreateZeroBlock(blockSize + 1), {}};
            TString destination(blockSize * 2, '\x7f');
            const auto checksums = SgListCopyWithBlockChecksums(
                zeroes,
                {destination.begin(), destination.size()},
                blockSize);
            UNIT_ASSERT_VALUES_EQUAL(destination, TString(blockSize * 2, '\0'));
            CheckBlockChecksums(
                checksums, {destination.data(), destination.size()}, blockSize);
        }
    }

    Y_UNIT_TEST(ShouldKeepOwnedBytesAfterSourceChangesAndIsDestroyed)
    {
        constexpr ui32 blockSize = 4096;
        TString destination(blockSize * 3, '\0');
        TVector<ui32> checksums;
        {
            TString source(blockSize * 3, 'a');
            TGuardedSgList guarded({{source.begin(), source.size()}});
            {
                auto guard = guarded.Acquire();
                UNIT_ASSERT(guard);
                // Synchronously changing the client buffer under a guard is
                // legal: the guard provides access lifetime, not a snapshot.
                memset(source.begin(), 'b', source.size());
                checksums = SgListCopyWithBlockChecksums(
                    guard.Get(),
                    {destination.begin(), destination.size()},
                    blockSize);
                memset(source.begin(), 'c', source.size());
            }
            guarded.Close();
        }
        UNIT_ASSERT_VALUES_EQUAL(destination, TString(blockSize * 3, 'b'));
        CheckBlockChecksums(
            checksums, {destination.data(), destination.size()}, blockSize);
    }

#if defined(__linux__)
    Y_UNIT_TEST(ShouldChecksumFinalOwnedBytesWithAConcurrentGuestWriter)
    {
        constexpr ui32 blockSize = 4096;
        constexpr size_t size = blockSize * 512;
        TGuestMemory guest(size);
        guest.WaitReady();
        const ui32 passesBefore = guest.Passes();
        const TSgList fragments = {
            {}, {guest.Data(), blockSize - 1}, {},
            {guest.Data() + blockSize - 1, 1},
            {guest.Data() + blockSize, size - blockSize}, {}};
        TGuardedSgList guarded(fragments);

        for (const bool blockwise: {false, true}) {
            for (size_t repeat = 0; repeat < 8; ++repeat) {
                TString destination(size, '\0');
                TVector<ui32> checksums;
                {
                    auto guard = guarded.Acquire();
                    UNIT_ASSERT(guard);
                    if (blockwise) {
                        checksums = SgListCopyWithBlockChecksums(
                            guard.Get(),
                            {destination.begin(), destination.size()},
                            blockSize);
                    } else {
                        // A: the original safe copy, release guard, then CRC
                        // the private bytes while the guest keeps changing.
                        SgListCopy(guard.Get(),
                            {destination.begin(), destination.size()});
                    }
                }
                if (!blockwise) {
                    checksums.reserve(size / blockSize);
                    for (size_t offset = 0; offset < size; offset += blockSize) {
                        checksums.push_back(Crc32c(
                            destination.data() + offset, blockSize));
                    }
                }
                // No assertion about one atomic snapshot of the guest source:
                // mixed source generations are allowed. The persisted bytes
                // and their CRC must agree, and only the parent owns this dst.
                CheckBlockChecksums(checksums,
                    {destination.data(), destination.size()}, blockSize);
                UNIT_ASSERT(guest.IsRunning());
            }
        }
        guest.WaitForProgress(passesBefore);
        guest.StopAndWait();
        const int status = guest.Status();
        UNIT_ASSERT(WIFEXITED(status));
        UNIT_ASSERT_VALUES_EQUAL(WEXITSTATUS(status), 0);
        guarded.Close();
    }
#endif

    Y_UNIT_TEST(ShouldKeepCopyWhenCloseWaitsForTheCopyGuard)
    {
        constexpr ui32 blockSize = 4096;
        TString source(blockSize * 257, 'q');
        TString destination(source.size(), '\0');
        TGuardedSgList guarded({{source.begin(), source.size()}});
        std::atomic<bool> closeStarted = false;
        std::atomic<bool> closeFinished = false;
        bool waitedForGuard = false;
        TVector<ui32> checksums;
        std::thread closer;
        {
            auto guard = guarded.Acquire();
            UNIT_ASSERT(guard);
            closer = std::thread([&] {
                closeStarted.store(true, std::memory_order_release);
                guarded.Close();
                closeFinished.store(true, std::memory_order_release);
            });
            while (!closeStarted.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            waitedForGuard = !closeFinished.load(std::memory_order_acquire);
            checksums = SgListCopyWithBlockChecksums(
                guard.Get(),
                {destination.begin(), destination.size()},
                blockSize);
        }
        closer.join();
        UNIT_ASSERT(waitedForGuard);
        UNIT_ASSERT(closeFinished.load(std::memory_order_acquire));
        UNIT_ASSERT(!guarded.Acquire());
        UNIT_ASSERT_VALUES_EQUAL(destination, source);
        CheckBlockChecksums(
            checksums, {destination.data(), destination.size()}, blockSize);
    }

    Y_UNIT_TEST(ShouldNotAccessSourceAfterItHasBeenClosed)
    {
        TString source(4096, 'q');
        TGuardedSgList guarded({{source.begin(), source.size()}});
        guarded.Close();
        source.clear();
        const auto guard = guarded.Acquire();
        UNIT_ASSERT(!guard);
        // The caller must take this cancellation branch before invoking the
        // helper. The helper deliberately owns neither a guard nor the source.
    }
}

}   // namespace NCloud
