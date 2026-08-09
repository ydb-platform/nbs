#include "device_discard.h"

#include <util/string/builder.h>

#include <cerrno>
#include <cstring>

#if defined(_linux_)
#   include <linux/fs.h>
#   include <sys/ioctl.h>
#   include <sys/stat.h>
#endif

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

NProto::TError DiscardDeviceRange(
    TFileHandle& file,
    ui64 offset,
    ui64 length)
{
#if defined(_linux_)
    struct stat st;
    if (fstat(FHANDLE(file), &st) != 0) {
        const int err = errno;
        return MakeError(
            E_IO,
            TStringBuilder() << "fstat failed: " << err << " " << strerror(err));
    }

    if (!S_ISBLK(st.st_mode)) {
        return MakeError(
            E_ARGUMENT,
            "Discard/Zero is only supported for block devices");
    }

    ui64 range[2] = {offset, length};
    if (ioctl(FHANDLE(file), BLKDISCARD, range) == 0) {
        return {};
    }

    const int err = errno;
    return MakeError(
        E_IO,
        TStringBuilder() << "BLKDISCARD failed: " << err << " "
                         << strerror(err));
#else
    Y_UNUSED(file);
    Y_UNUSED(offset);
    Y_UNUSED(length);
    return MakeError(E_NOT_IMPLEMENTED, "Discard/Zero is not supported");
#endif
}

}   // namespace NCloud::NBlockStore::NTesting
