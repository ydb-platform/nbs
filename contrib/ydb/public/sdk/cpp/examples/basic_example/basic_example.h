#pragma once

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/query.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/tx.h>

NYdb::TParams GetTablesDataParams();

bool Run(const NYdb::TDriver& driver);
