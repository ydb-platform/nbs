#pragma once

#include "public.h"

#include "ydbscheme.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/iam/iface/client.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <optional>
#include <utility>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

using TTableStat = std::pair<TString, TInstant>;

struct TGetTablesResponse
{
    const NProto::TError Error;
    const TVector<TTableStat> Tables;

    explicit TGetTablesResponse(const NProto::TError& error)
        : Error(error)
    {}

    explicit TGetTablesResponse(TVector<TTableStat> tables)
        : Tables(std::move(tables))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TDescribeTableResponse
{
    const NProto::TError Error;
    const TStatsTableScheme TableScheme;

    explicit TDescribeTableResponse(const NProto::TError& error)
        : Error(error)
    {}

    TDescribeTableResponse(
            TVector<NYdb::TColumn> columns,
            TVector<TString> keyColumns,
            TMaybe<NYdb::NTable::TTtlSettings> ttlSettings )
        : TableScheme(
            std::move(columns),
            std::move(keyColumns),
            std::move(ttlSettings))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct IYdbStorage
    : public IStartable
{
    virtual ~IYdbStorage() = default;

    virtual NThreading::TFuture<NProto::TError> CreateTable(
        const TString& table,
        const NYdb::NTable::TTableDescription& description) = 0;

    virtual NThreading::TFuture<NProto::TError> AlterTable(
        const TString& table,
        const NYdb::NTable::TAlterTableSettings& settings) = 0;

    virtual NThreading::TFuture<NProto::TError> DropTable(const TString& table) = 0;

    virtual NThreading::TFuture<NProto::TError> ExecuteUploadQuery(
        TString tableName,
        NYdb::TValue data) = 0;

    virtual NThreading::TFuture<TDescribeTableResponse> DescribeTable(const TString& table) = 0;

    virtual NThreading::TFuture<TGetTablesResponse> GetHistoryTables() = 0;
};

}   // namespace NCloud::NBlockStore::NYdbStats
