#pragma once

#include <contrib/ydb/library/yql/providers/yt/gateway/lib/downloader.h>
#include <contrib/ydb/library/yql/providers/yt/common/yql_yt_settings.h>

#include <contrib/ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>


#include <contrib/ydb/library/yql/core/yql_type_annotation.h>
#include <contrib/ydb/library/yql/core/yql_udf_resolver.h>
#include <contrib/ydb/library/yql/minikql/mkql_node.h>
#include <contrib/ydb/library/yql/minikql/mkql_node_visitor.h>
#include <contrib/ydb/library/yql/minikql/mkql_program_builder.h>

using namespace NKikimr::NMiniKQL;
using namespace NKikimr;

namespace NYql {

ITableDownloaderFunc MakeYtNativeFileDownloader(
    IYtGateway::TPtr gateway,
    const TString& sessionId,
    const TString& cluster,
    TYtSettings::TConstPtr settings,
    NYT::IClientPtr client,
    TTempFiles::TPtr tmpFiles
);

} // namespace NYql
