#include "logging.h"

#include <cloud/vm/api/blockstore-plugin.h>

#include <library/cpp/logger/system.h>

namespace NCloud::NBlockStore::NPlugin {

namespace {

////////////////////////////////////////////////////////////////////////////////

class THostLogBackend final: public TLogBackend
{
private:
    BlockPluginHost* const Host;

public:
    THostLogBackend(BlockPluginHost* host)
        : Host(host)
    {}

    void WriteData(const TLogRecord& rec) override
    {
        // TODO: now we assume null-terminated string in TLogRecord,
        // but it is much better to fix the ABI
        Host->log_message(Host, rec.Data);
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

std::shared_ptr<TLogBackend> CreateHostLogBackend(BlockPluginHost* host)
{
    return std::make_shared<THostLogBackend>(host);
}

std::shared_ptr<TLogBackend> CreateSysLogBackend(const TString& ident)
{
    return std::make_shared<TSysLogBackend>(
        ident ? ident.data() : "NBS_CLIENT",
        TSysLogBackend::TSYSLOG_LOCAL1,
        0);
}

}   // namespace NCloud::NBlockStore::NPlugin
