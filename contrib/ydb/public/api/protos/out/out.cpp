#include <contrib/ydb/public/api/protos/ydb_cms.pb.h>
#include <contrib/ydb/public/api/protos/ydb_monitoring.pb.h>
#include <contrib/ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, Ydb::Cms::GetDatabaseStatusResult::State, stream, value) {
    stream << Ydb::Cms::GetDatabaseStatusResult::State_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::StatusIds::StatusCode, stream, value) {
    stream << Ydb::StatusIds::StatusCode_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Monitoring::SelfCheck::Result, stream, value) {
    stream << Ydb::Monitoring::SelfCheck_Result_Name(value);
}

Y_DECLARE_OUT_SPEC(, Ydb::Monitoring::StatusFlag::Status, stream, value) {
    stream << Ydb::Monitoring::StatusFlag_Status_Name(value);
}
