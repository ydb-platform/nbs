#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TIO
{
    explicit TIO(const TString& filePath, ui32 blockSize);
    ~TIO();

    [[nodiscard]] TString Read(ui64 blockIndex, ui32 blockCount) const;
    void AlternatingWrite(ui64 blockIndex, const TVector<TStringBuf>& datas);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud::NBlockStore
