#pragma once

#include <contrib/ydb/library/yql/providers/yt/gateway/lib/exec_ctx.h>
#include <contrib/ydb/library/yql/providers/yt/provider/yql_yt_gateway.h>
#include <contrib/ydb/library/yql/providers/yt/lib/access_provider/yt_access_provider.h>
#include <contrib/ydb/library/yql/providers/yt/lib/secret_masker/secret_masker.h>
#include <contrib/ydb/library/yql/providers/yt/lib/tvm_client/tvm_client.h>
#include <contrib/ydb/library/yql/providers/yt/lib/yt_token_resolver/yt_token_resolver.h>

#include <contrib/ydb/library/yql/core/file_storage/file_storage.h>
#include <contrib/ydb/library/yql/minikql/mkql_function_registry.h>
#include <contrib/ydb/library/yql/providers/common/metrics/metrics_registry.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TYtNativeServices: public TYtBaseServices {
    using TPtr = TIntrusivePtr<TYtNativeServices>;
    // allow anonymous access for tests
    bool DisableAnonymousClusterAccess = false;

    IMetricsRegistryPtr Metrics;
    ISecretMasker::TPtr SecretMasker;
    ITvmClient::TPtr TvmClient;
    IYtAccessProvider::TPtr YtAccessProvider;
    IYtTokenResolver::TPtr YtTokenResolver;
};

IYtGateway::TPtr CreateYtNativeGateway(const TYtNativeServices& services);

}
