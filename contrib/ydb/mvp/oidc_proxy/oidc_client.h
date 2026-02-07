#pragma once

#include <contrib/ydb/mvp/core/core_ydb.h>
#include "openid_connect.h"

void InitOIDC(NActors::TActorSystem& actorSystem, const NActors::TActorId& httpProxyId, const TOpenIdConnectSettings& settings);
