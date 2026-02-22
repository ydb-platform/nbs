#include "write_sync_flags.h"

#include <fcntl.h>
#include <linux/fs.h>

namespace NCloud {

ui32 GetWriteSyncFlags(ui32 flags)
{
    // O_SYNC includes O_DSYNC bits on Linux, so O_SYNC must take precedence.
    if ((flags & O_SYNC) == O_SYNC) {
        return RWF_SYNC;
    }
    if ((flags & O_DSYNC) == O_DSYNC) {
        return RWF_DSYNC;
    }

    return 0;
}

}   // namespace NCloud
