#include "zero_extent.h"

#include <util/string/builder.h>
#include <util/system/file.h>

#include <limits>

#if defined(__linux__)
// This exists because on sandbox builds we have newer kernels with much older
// UAPI headers If kernel does not actually support this we will get a proper
// error in runtime
#if !defined(FALLOC_FL_ZERO_RANGE)
#define FALLOC_FL_ZERO_RANGE 0x10
#endif
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <sys/types.h>
#endif

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

NProto::TError
ZeroFileExtent(TFileHandle& fileHandle, i64 offset, i64 length) noexcept
{
#if defined(__linux__)
    if (length > std::numeric_limits<off_t>::max() ||
        offset > std::numeric_limits<off_t>::max())
    {
        return MakeError(E_ARGUMENT);
    }

    // On linux we have FALLOC_FL_ZERO_RANGE (or even PUNCH_HOLE, but let's not
    // get carried away)
    int res =
        fallocate(FHANDLE(fileHandle), FALLOC_FL_ZERO_RANGE, offset, length);
    if (res == 0) {
        return {};
    }

    if (errno != EOPNOTSUPP) {
        return MakeError(
            E_IO,
            TStringBuilder() << "FALLOC_FL_ZERO_RANGE failed: " << errno << " "
                             << strerror(errno));
    }

#endif

    // Not supported

    NProto::TError error;
    error.SetCode(E_NOT_IMPLEMENTED);

    return error;
}

}   // namespace NCloud::NBlockStore::NServer
