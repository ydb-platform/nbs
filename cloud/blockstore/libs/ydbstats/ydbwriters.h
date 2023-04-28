#pragma once

#include "ydbrow.h"

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

class IYdbWriter
{
public:
    virtual ~IYdbWriter() = default;

    virtual bool IsValid() const
    {
        return false;
    }

    virtual void Declare(TStringStream& out) const
    {
        Y_UNUSED(out);
    }

    virtual void Replace(TStringStream& out) const
    {
        Y_UNUSED(out);
    }

    virtual void PushData(NYdb::TParamsBuilder& out) const
    {
        Y_UNUSED(out);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYdbReplaceWriter
    : public IYdbWriter
{
public:
    TYdbReplaceWriter(TStringBuf tableName, TStringBuf itemName);

    bool IsValid() const;
    void Replace(TStringStream& out) const;

private:
    const TStringBuf TableName;
    const TStringBuf ItemName;
};

////////////////////////////////////////////////////////////////////////////////

class TYdbRowWriter
    : public TYdbReplaceWriter
{
public:
    TYdbRowWriter(const TVector<TYdbRow>& data,
                  TStringBuf tableName);

    TYdbRowWriter(TVector<TYdbRow>&& data,
                  TStringBuf tableName) = delete;

    bool IsValid() const;
    void Declare(TStringStream& out) const final;
    void PushData(NYdb::TParamsBuilder& out) const final;

    static constexpr TStringBuf ItemName = "$items";

private:
    const TVector<TYdbRow>& Data;
};

////////////////////////////////////////////////////////////////////////////////

class TYdbBlobLoadMetricWriter
    : public TYdbReplaceWriter
{
public:
    TYdbBlobLoadMetricWriter(const TVector<TYdbBlobLoadMetricRow>& data,
                             TStringBuf tableName);

    TYdbBlobLoadMetricWriter(TVector<TYdbBlobLoadMetricRow>&& data,
                             TStringBuf tableName) = delete;

    bool IsValid() const;
    void Declare(TStringStream& out) const final;
    void PushData(NYdb::TParamsBuilder& out) const final;

    static constexpr TStringBuf ItemName = "$metrics_items";

private:
    const TVector<TYdbBlobLoadMetricRow>& Data;
};

} // namespace NCloud::NBlockStore::NYdbStats
