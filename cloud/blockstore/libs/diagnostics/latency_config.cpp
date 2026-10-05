#include "latency_config.h"

#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/yexception.h>
#include <util/string/cast.h>

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <limits>

namespace NCloud::NBlockStore::NVHostServer {
TString SerializeLatencyConfig(const NProto::TDiagnosticsConfig& config,
                               ui32 mediaKind)
{
    NProto::TDiagnosticsConfig selected;
    selected.SetEnableLatency(config.GetEnableLatency());
    selected.SetLatencyThresholdVersion(config.GetLatencyThresholdVersion());
    for (const auto& row: config.GetLatencyThresholds()) {
        if (row.GetMediaKind() == mediaKind) {
            *selected.AddLatencyThresholds() = row;
        }
    }
    return ToString(mediaKind) + ":" +
           Base64Encode(selected.SerializeAsString());
}

TLatencyConfig ParseLatencyConfig(TStringBuf value)
{
    Y_ENSURE(value.size() < 65536, "latency configuration is too large");
    const auto colon = value.find(':');
    Y_ENSURE(colon != TStringBuf::npos, "invalid latency configuration");
    TLatencyConfig result;
    result.MediaKind = FromString<ui32>(value.SubStr(0, colon));
    Y_ENSURE(
        result.Config.ParseFromString(Base64Decode(value.SubStr(colon + 1))),
        "invalid latency protobuf");
    return result;
}

ui64 NextLatencyGeneration(const TString& socketPath)
{
    const TString path = socketPath + ".latency-generation";
    int fd =
        ::open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0) {
        return 0;
    }

    struct TClose
    {
        int Fd;

        ~TClose()
        {
            ::close(Fd);
        }
    } close{fd};

    struct stat st
    {
    };

    if (::flock(fd, LOCK_EX) || ::fstat(fd, &st) || !S_ISREG(st.st_mode) ||
        st.st_uid != ::geteuid() ||
        (st.st_size != 0 && st.st_size != sizeof(ui64)))
    {
        return 0;
    }
    ui64 generation = 0;
    const auto size = ::pread(fd, &generation, sizeof(generation), 0);
    if ((size != 0 && size != sizeof(generation)) ||
        generation == std::numeric_limits<ui64>::max())
    {
        return 0;
    }
    ++generation;
    if (::pwrite(fd, &generation, sizeof(generation), 0) !=
            sizeof(generation) ||
        ::fsync(fd))
    {
        return 0;
    }
    // Persist the directory entry when this is the first generation.
    const auto slash = path.rfind('/');
    const TString directory =
        slash == TString::npos ? "." : path.substr(0, slash);
    const int dir =
        ::open(directory.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir < 0) {
        return 0;
    }
    const int error = ::fsync(dir);
    ::close(dir);
    return error ? 0 : generation;
}
}   // namespace NCloud::NBlockStore::NVHostServer
