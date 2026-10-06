#include "latency_generation.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <limits>

namespace NCloud::NBlockStore::NVHostServer {

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
