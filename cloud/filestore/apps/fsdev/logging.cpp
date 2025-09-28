#include "logging.h"

#include "spdk/log.h"

#include <library/cpp/logger/system.h>

namespace NCloud::NFileStore::NFsdev {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSpdkLogBackend final
    : public TLogBackend
{
public:
    TSpdkLogBackend()
    {}

    void WriteData(const TLogRecord& rec) override
    {
        SPDK_PRINTF("%s", rec.Data);
    }

    void ReopenLog() override
    {
        // nothing to do
    }

    void ReopenLogNoFlush() override
    {
        // nothing to do
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TLogBackend> CreateSpdkLogBackend()
{
    return std::make_shared<TSpdkLogBackend>();
}

}   // namespace NCloud::NFileStore::NFsdev
