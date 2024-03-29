#pragma once

#include <contrib/ydb/core/ymq/base/events_writer_iface.h>
#include <contrib/ydb/core/protos/config.pb.h>

class TSqsEventsWriterFactory : public NKikimr::NSQS::IEventsWriterFactory {
public:
    NKikimr::NSQS::IEventsWriterWrapper::TPtr CreateEventsWriter(const NKikimrConfig::TSqsConfig& config, const NMonitoring::TDynamicCounterPtr& counters) const override;
};
