#include "export.h"
#include <contrib/ydb/core/driver_lib/run/main.h>
#include <contrib/ydb/core/security/ticket_parser.h>
#include <contrib/ydb/core/ymq/actor/auth_multi_factory.h>
#include <contrib/ydb/core/ymq/base/events_writer.h>
#include <contrib/ydb/library/folder_service/folder_service.h>
#include <contrib/ydb/library/pdisk_io/aio.h>
#include <contrib/ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>


int main(int argc, char **argv) {
    SetupTerminateHandler();

    auto factories = std::make_shared<NKikimr::TModuleFactories>();
    factories->DataShardExportFactory = std::make_shared<TDataShardExportFactory>();
    factories->CreateTicketParser = NKikimr::CreateTicketParser;
    factories->FolderServiceFactory = NKikimr::NFolderService::CreateFolderServiceActor;
    factories->IoContextFactory = std::make_shared<NKikimr::NPDisk::TIoContextFactoryOSS>();
    factories->DataStreamsAuthFactory = std::make_shared<NKikimr::NHttpProxy::TIamAuthFactory>();
    factories->AdditionalComputationNodeFactories = { NYql::GetPgFactory() };
    factories->SqsAuthFactory = std::make_shared<NKikimr::NSQS::TMultiAuthFactory>();
    factories->SqsEventsWriterFactory = std::make_shared<TSqsEventsWriterFactory>();

    return ParameterizedMain(argc, argv, std::move(factories));
}
