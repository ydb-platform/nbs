#include "latency_sli.h"

#include <util/generic/vector.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstdio>

namespace NCloud::NBlockStore {
namespace {
// Local checkpoint, not a wire format. Fixed-width words avoid struct padding;
// the marker also rejects files from a machine with different byte order.
constexpr ui64 Marker = 0x4E42534C41543031ULL;
using TCheckpoint = std::array<ui64, 19>;

ui64 Checksum(const TCheckpoint& words)
{
    ui64 hash = 14695981039346656037ULL;
    for (size_t i = 0; i + 1 < words.size(); ++i) {
        for (ui32 shift = 0; shift < 64; shift += 8) {
            hash ^= (words[i] >> shift) & 0xff;
            hash *= 1099511628211ULL;
        }
    }
    return hash;
}

TCheckpoint Encode(const TLatencyBatch& batch)
{
    TCheckpoint words{Marker, batch.Version, batch.ThresholdVersion,
                      batch.Generation,
                      batch.Sequence,
                      batch.CapturedAt.MicroSeconds(),
                      batch.Read.Good,
                      batch.Read.Bad,
                      batch.Read.Unknown,
                      batch.Read.InvalidClientRequest,
                      batch.Read.ClientCancellation,
                      batch.Read.ClientLimit,
                      batch.Write.Good,
                      batch.Write.Bad,
                      batch.Write.Unknown,
                      batch.Write.InvalidClientRequest,
                      batch.Write.ClientCancellation,
                      batch.Write.ClientLimit};
    words.back() = Checksum(words);
    return words;
}

bool Decode(const TCheckpoint& words, TLatencyBatch& batch)
{
    if (words.back() != Checksum(words) || words[0] != Marker ||
        words[1] > Max<ui32>() || words[2] > Max<ui32>() || !words[3] ||
        !words[4] || !words[5])
    {
        return false;
    }
    batch.Version = words[1];
    batch.ThresholdVersion = words[2];
    batch.Generation = words[3];
    batch.Sequence = words[4];
    batch.CapturedAt = TInstant::MicroSeconds(words[5]);
    batch.Read = {words[6], words[7], words[8], words[9], words[10], words[11]};
    batch.Write = {words[12], words[13], words[14], words[15], words[16],
                   words[17]};
    return true;
}
}   // namespace

void TLatencyBatchTracker::SetCheckpointPath(const TString& path)
{
    std::lock_guard lock(Lock);
    if (!CheckpointPath.empty() || path.empty()) {
        return;
    }
    CheckpointPath = path;
    const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        CheckpointHealthy = errno == ENOENT;
        return;
    }

    struct stat st
    {
    };

    TCheckpoint words{};
    TLatencyBatch batch;
    CheckpointHealthy =
        !::fstat(fd, &st) && S_ISREG(st.st_mode) && st.st_uid == ::geteuid() &&
        st.st_size == sizeof(words) &&
        ::read(fd, words.data(), sizeof(words)) == sizeof(words) &&
        Decode(words, batch);
    ::close(fd);
    if (CheckpointHealthy) {
        Last = batch;
    }
}

bool TLatencyBatchTracker::SaveCheckpoint(const TLatencyBatch& batch)
{
    if (CheckpointPath.empty()) {
        return true;
    }
    const TString temporary = CheckpointPath + ".XXXXXX";
    TVector<char> name(temporary.begin(), temporary.end());
    name.push_back(0);
    const int fd = ::mkstemp(name.data());
    if (fd < 0) {
        return false;
    }
    const auto words = Encode(batch);
    bool ok = ::write(fd, words.data(), sizeof(words)) == sizeof(words) &&
              !::fsync(fd);
    ::close(fd);
    if (ok) {
        ok = !::rename(name.data(), CheckpointPath.c_str());
    }
    ::unlink(name.data());
    if (!ok) {
        return false;
    }
    const auto slash = CheckpointPath.rfind('/');
    const TString directory =
        slash == TString::npos ? "." : CheckpointPath.substr(0, slash);
    const int dir =
        ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir < 0) {
        return false;
    }
    ok = !::fsync(dir);
    ::close(dir);
    return ok;
}
}   // namespace NCloud::NBlockStore
