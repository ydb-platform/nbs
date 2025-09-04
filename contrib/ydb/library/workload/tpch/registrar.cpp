#include "tpch.h"
#include <contrib/ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TTpchWorkloadParams> TpchRegistrar("tpch");

}