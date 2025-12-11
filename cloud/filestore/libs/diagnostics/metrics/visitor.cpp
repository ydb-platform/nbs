#include "visitor.h"

#include <array>

namespace NCloud::NFileStore::NMetrics {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRegistryVisitor: public IRegistryVisitor
{
private:
    IOutputStream& Out;

public:
    explicit TRegistryVisitor(IOutputStream& out)
        : Out(out)
    {}

    // IRegistryVisitor
    void OnStreamBegin() override
    {
        Out << "metrics:[";
    }

    void OnStreamEnd() override
    {
        Out << ']';
    }

    void OnMetricBegin(
        TInstant time,
        EAggregationType aggrType,
        EMetricType metrType) override
    {
        Out << '{';
        DumpTime(time);
        Out << "aggregation_type:'" << aggrType << "'," << "metric_type:'"
            << metrType << "',";
    }

    void OnMetricEnd() override
    {
        Out << "},";
    }

    void OnLabelsBegin() override
    {
        Out << "labels:[";
    }

    void OnLabelsEnd() override
    {
        Out << "],";
    }

    void OnLabel(TStringBuf name, TStringBuf value) override
    {
        Out << "{name:'" << name << "',value:'" << value << "'},";
    }

    void OnValue(i64 value) override
    {
        Out << "value:'" << value << "',";
    }

private:
    void DumpTime(TInstant time) const
    {
        std::array<char, 64> buf;
        const size_t len = FormatDate8601(buf.data(), buf.size(), time.TimeT());
        Out << "time:'";
        Out.Write(buf.data(), len);
        Out << "',";
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRegistryVisitorPtr CreateRegistryVisitor(IOutputStream& out)
{
    return std::make_shared<TRegistryVisitor>(out);
}

}   // namespace NCloud::NFileStore::NMetrics
