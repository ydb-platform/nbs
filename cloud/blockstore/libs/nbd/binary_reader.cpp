#include "binary_reader.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

bool TBinaryReader::Read(const TSgList& dstList, size_t srcLen)
{
    char* dst = nullptr;
    size_t dstLen = 0;

    size_t dstIndex = 0;
    while (srcLen) {
        if (!dstLen) {
            if (dstIndex < dstList.size()) {
                const auto& block = dstList[dstIndex++];
                dst = (char*)block.Data();
                dstLen = block.Size();
            } else {
                // end of destination
                return false;
            }
        }

        size_t toCopy = Min(srcLen, dstLen);

        if (!Read(dst, toCopy)) {
            // end of source
            return false;
        }

        dst += toCopy;
        srcLen -= toCopy;
        dstLen -= toCopy;
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NBD
