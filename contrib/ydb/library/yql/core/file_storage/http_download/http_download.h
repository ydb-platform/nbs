#pragma once

#include <contrib/ydb/library/yql/core/file_storage/defs/downloader.h>

namespace NYql {

class TFileStorageConfig;

NYql::NFS::IDownloaderPtr MakeHttpDownloader(const TFileStorageConfig& config);

} // namespace NYql
