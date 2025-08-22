#pragma once

#include "public.h"

#include "node.h"

#include <cloud/storage/core/libs/daemon/config_initializer.h>

#include <contrib/ydb/core/protos/config.pb.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TConfigInitializerYdbBase
    : public virtual TConfigInitializerBase
{
    TOptionsYdbBasePtr Options;

    NKikimrConfig::TAppConfigPtr KikimrConfig;

    TConfigInitializerYdbBase(TOptionsYdbBasePtr options) noexcept
        : Options(std::move(options))
    {}

    virtual ~TConfigInitializerYdbBase() = default;

    void InitKikimrConfig();

    void SetupMonitoringConfig(NKikimrConfig::TMonitoringConfig& monConfig) const;
    void SetupLogLevel(NKikimrConfig::TLogConfig& logConfig) const;

    void ApplyCMSConfigs(NKikimrConfig::TAppConfig cmsConfig);

    ui32 GetLogDefaultLevel() const override;
    ui32 GetMonitoringPort() const override;
    TString GetMonitoringAddress() const override;
    ui32 GetMonitoringThreads() const override;
    TString GetLogBackendFileName() const override;

    virtual void ApplyCustomCMSConfigs(const NKikimrConfig::TAppConfig& config) = 0;

protected:
    NKikimrConfig::TLogConfig GetLogConfig() const;
    NKikimrConfig::TMonitoringConfig GetMonitoringConfig() const;
    TString GetFullSchemeShardDir() const;

    void ApplyActorSystemConfig(const TString& text);
    void ApplyInterconnectConfig(const TString& text);
    void ApplyAuthConfig(const TString& text);
    void ApplyLogConfig(const TString& text);
    void ApplyMonitoringConfig(const TString& text);
};

}   // namespace NCloud::NServer
