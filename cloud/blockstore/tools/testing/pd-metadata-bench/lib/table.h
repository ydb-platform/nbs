#pragma once

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlockMeta
{
    ui64 CommitId = 0;
    ui64 Checksum = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TMetaTable
{
    TMetaTable(const TString& filePath, ui64 blockCount);
    ~TMetaTable();

    NThreading::TFuture<TBlockMeta> Read(ui64 blockIndex) const;
    NThreading::TFuture<void> Write(ui64 blockIndex, TBlockMeta meta);

private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;
};

}   // namespace NCloud::NBlockStore
