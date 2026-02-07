#include "tpcds.h"
#include <contrib/ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TTpcdsWorkloadParams> TpcdsRegistrar("tpcds");

}