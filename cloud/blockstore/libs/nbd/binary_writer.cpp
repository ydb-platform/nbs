#include "binary_writer.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

size_t TBinaryWriter::Write(const TSgList& sglist)
{
    size_t len = 0;
    for (const auto& buffer: sglist) {
        Write(buffer.Data(), buffer.Size());
        len += buffer.Size();
    }

    return len;
}

}   // namespace NCloud::NBlockStore::NBD
