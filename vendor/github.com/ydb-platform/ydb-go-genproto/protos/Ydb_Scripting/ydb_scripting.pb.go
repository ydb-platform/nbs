// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v4.25.1
// source: protos/ydb_scripting.proto

package Ydb_Scripting

import (
	Ydb "github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	Ydb_Issue "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	Ydb_Operations "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Operations"
	Ydb_Table "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	Ydb_TableStats "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_TableStats"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ExplainYqlRequest_Mode int32

const (
	ExplainYqlRequest_MODE_UNSPECIFIED ExplainYqlRequest_Mode = 0
	// PARSE = 1;
	ExplainYqlRequest_VALIDATE ExplainYqlRequest_Mode = 2
	ExplainYqlRequest_PLAN     ExplainYqlRequest_Mode = 3
)

// Enum value maps for ExplainYqlRequest_Mode.
var (
	ExplainYqlRequest_Mode_name = map[int32]string{
		0: "MODE_UNSPECIFIED",
		2: "VALIDATE",
		3: "PLAN",
	}
	ExplainYqlRequest_Mode_value = map[string]int32{
		"MODE_UNSPECIFIED": 0,
		"VALIDATE":         2,
		"PLAN":             3,
	}
)

func (x ExplainYqlRequest_Mode) Enum() *ExplainYqlRequest_Mode {
	p := new(ExplainYqlRequest_Mode)
	*p = x
	return p
}

func (x ExplainYqlRequest_Mode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (ExplainYqlRequest_Mode) Descriptor() protoreflect.EnumDescriptor {
	return file_protos_ydb_scripting_proto_enumTypes[0].Descriptor()
}

func (ExplainYqlRequest_Mode) Type() protoreflect.EnumType {
	return &file_protos_ydb_scripting_proto_enumTypes[0]
}

func (x ExplainYqlRequest_Mode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use ExplainYqlRequest_Mode.Descriptor instead.
func (ExplainYqlRequest_Mode) EnumDescriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{5, 0}
}

type ExecuteYqlRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	OperationParams *Ydb_Operations.OperationParams     `protobuf:"bytes,1,opt,name=operation_params,json=operationParams,proto3" json:"operation_params,omitempty"`
	Script          string                              `protobuf:"bytes,2,opt,name=script,proto3" json:"script,omitempty"`
	Parameters      map[string]*Ydb.TypedValue          `protobuf:"bytes,3,rep,name=parameters,proto3" json:"parameters,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	CollectStats    Ydb_Table.QueryStatsCollection_Mode `protobuf:"varint,4,opt,name=collect_stats,json=collectStats,proto3,enum=Ydb.Table.QueryStatsCollection_Mode" json:"collect_stats,omitempty"`
}

func (x *ExecuteYqlRequest) Reset() {
	*x = ExecuteYqlRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecuteYqlRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecuteYqlRequest) ProtoMessage() {}

func (x *ExecuteYqlRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecuteYqlRequest.ProtoReflect.Descriptor instead.
func (*ExecuteYqlRequest) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{0}
}

func (x *ExecuteYqlRequest) GetOperationParams() *Ydb_Operations.OperationParams {
	if x != nil {
		return x.OperationParams
	}
	return nil
}

func (x *ExecuteYqlRequest) GetScript() string {
	if x != nil {
		return x.Script
	}
	return ""
}

func (x *ExecuteYqlRequest) GetParameters() map[string]*Ydb.TypedValue {
	if x != nil {
		return x.Parameters
	}
	return nil
}

func (x *ExecuteYqlRequest) GetCollectStats() Ydb_Table.QueryStatsCollection_Mode {
	if x != nil {
		return x.CollectStats
	}
	return Ydb_Table.QueryStatsCollection_Mode(0)
}

type ExecuteYqlResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Operation *Ydb_Operations.Operation `protobuf:"bytes,1,opt,name=operation,proto3" json:"operation,omitempty"`
}

func (x *ExecuteYqlResponse) Reset() {
	*x = ExecuteYqlResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecuteYqlResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecuteYqlResponse) ProtoMessage() {}

func (x *ExecuteYqlResponse) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecuteYqlResponse.ProtoReflect.Descriptor instead.
func (*ExecuteYqlResponse) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{1}
}

func (x *ExecuteYqlResponse) GetOperation() *Ydb_Operations.Operation {
	if x != nil {
		return x.Operation
	}
	return nil
}

type ExecuteYqlResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ResultSets []*Ydb.ResultSet           `protobuf:"bytes,1,rep,name=result_sets,json=resultSets,proto3" json:"result_sets,omitempty"`
	QueryStats *Ydb_TableStats.QueryStats `protobuf:"bytes,2,opt,name=query_stats,json=queryStats,proto3" json:"query_stats,omitempty"`
}

func (x *ExecuteYqlResult) Reset() {
	*x = ExecuteYqlResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecuteYqlResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecuteYqlResult) ProtoMessage() {}

func (x *ExecuteYqlResult) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecuteYqlResult.ProtoReflect.Descriptor instead.
func (*ExecuteYqlResult) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{2}
}

func (x *ExecuteYqlResult) GetResultSets() []*Ydb.ResultSet {
	if x != nil {
		return x.ResultSets
	}
	return nil
}

func (x *ExecuteYqlResult) GetQueryStats() *Ydb_TableStats.QueryStats {
	if x != nil {
		return x.QueryStats
	}
	return nil
}

// Response for StreamExecuteYql is a stream of ExecuteYqlPartialResponse messages.
// These responses can contain ExecuteYqlPartialResult messages with
// results (or result parts) for data or scan queries in the script.
// YqlScript can have multiple results (result sets).
// Each result set has an index (starting at 0).
type ExecuteYqlPartialResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Status Ydb.StatusIds_StatusCode  `protobuf:"varint,1,opt,name=status,proto3,enum=Ydb.StatusIds_StatusCode" json:"status,omitempty"`
	Issues []*Ydb_Issue.IssueMessage `protobuf:"bytes,2,rep,name=issues,proto3" json:"issues,omitempty"`
	Result *ExecuteYqlPartialResult  `protobuf:"bytes,3,opt,name=result,proto3" json:"result,omitempty"`
}

func (x *ExecuteYqlPartialResponse) Reset() {
	*x = ExecuteYqlPartialResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecuteYqlPartialResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecuteYqlPartialResponse) ProtoMessage() {}

func (x *ExecuteYqlPartialResponse) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecuteYqlPartialResponse.ProtoReflect.Descriptor instead.
func (*ExecuteYqlPartialResponse) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{3}
}

func (x *ExecuteYqlPartialResponse) GetStatus() Ydb.StatusIds_StatusCode {
	if x != nil {
		return x.Status
	}
	return Ydb.StatusIds_StatusCode(0)
}

func (x *ExecuteYqlPartialResponse) GetIssues() []*Ydb_Issue.IssueMessage {
	if x != nil {
		return x.Issues
	}
	return nil
}

func (x *ExecuteYqlPartialResponse) GetResult() *ExecuteYqlPartialResult {
	if x != nil {
		return x.Result
	}
	return nil
}

// Contains result set (or a result set part) for one data or scan query in the script.
// One result set can be split into several responses with same result_index.
// Only the final response can contain query stats.
type ExecuteYqlPartialResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Index of current result
	ResultSetIndex uint32 `protobuf:"varint,1,opt,name=result_set_index,json=resultSetIndex,proto3" json:"result_set_index,omitempty"`
	// Result set (or a result set part) for one data or scan query
	ResultSet  *Ydb.ResultSet             `protobuf:"bytes,2,opt,name=result_set,json=resultSet,proto3" json:"result_set,omitempty"`
	QueryStats *Ydb_TableStats.QueryStats `protobuf:"bytes,3,opt,name=query_stats,json=queryStats,proto3" json:"query_stats,omitempty"`
}

func (x *ExecuteYqlPartialResult) Reset() {
	*x = ExecuteYqlPartialResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExecuteYqlPartialResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExecuteYqlPartialResult) ProtoMessage() {}

func (x *ExecuteYqlPartialResult) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExecuteYqlPartialResult.ProtoReflect.Descriptor instead.
func (*ExecuteYqlPartialResult) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{4}
}

func (x *ExecuteYqlPartialResult) GetResultSetIndex() uint32 {
	if x != nil {
		return x.ResultSetIndex
	}
	return 0
}

func (x *ExecuteYqlPartialResult) GetResultSet() *Ydb.ResultSet {
	if x != nil {
		return x.ResultSet
	}
	return nil
}

func (x *ExecuteYqlPartialResult) GetQueryStats() *Ydb_TableStats.QueryStats {
	if x != nil {
		return x.QueryStats
	}
	return nil
}

type ExplainYqlRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	OperationParams *Ydb_Operations.OperationParams `protobuf:"bytes,1,opt,name=operation_params,json=operationParams,proto3" json:"operation_params,omitempty"`
	Script          string                          `protobuf:"bytes,2,opt,name=script,proto3" json:"script,omitempty"`
	Mode            ExplainYqlRequest_Mode          `protobuf:"varint,3,opt,name=mode,proto3,enum=Ydb.Scripting.ExplainYqlRequest_Mode" json:"mode,omitempty"`
}

func (x *ExplainYqlRequest) Reset() {
	*x = ExplainYqlRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExplainYqlRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExplainYqlRequest) ProtoMessage() {}

func (x *ExplainYqlRequest) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExplainYqlRequest.ProtoReflect.Descriptor instead.
func (*ExplainYqlRequest) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{5}
}

func (x *ExplainYqlRequest) GetOperationParams() *Ydb_Operations.OperationParams {
	if x != nil {
		return x.OperationParams
	}
	return nil
}

func (x *ExplainYqlRequest) GetScript() string {
	if x != nil {
		return x.Script
	}
	return ""
}

func (x *ExplainYqlRequest) GetMode() ExplainYqlRequest_Mode {
	if x != nil {
		return x.Mode
	}
	return ExplainYqlRequest_MODE_UNSPECIFIED
}

type ExplainYqlResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Operation *Ydb_Operations.Operation `protobuf:"bytes,1,opt,name=operation,proto3" json:"operation,omitempty"`
}

func (x *ExplainYqlResponse) Reset() {
	*x = ExplainYqlResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExplainYqlResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExplainYqlResponse) ProtoMessage() {}

func (x *ExplainYqlResponse) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExplainYqlResponse.ProtoReflect.Descriptor instead.
func (*ExplainYqlResponse) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{6}
}

func (x *ExplainYqlResponse) GetOperation() *Ydb_Operations.Operation {
	if x != nil {
		return x.Operation
	}
	return nil
}

type ExplainYqlResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ParametersTypes map[string]*Ydb.Type `protobuf:"bytes,1,rep,name=parameters_types,json=parametersTypes,proto3" json:"parameters_types,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Plan            string               `protobuf:"bytes,2,opt,name=plan,proto3" json:"plan,omitempty"`
}

func (x *ExplainYqlResult) Reset() {
	*x = ExplainYqlResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_protos_ydb_scripting_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ExplainYqlResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ExplainYqlResult) ProtoMessage() {}

func (x *ExplainYqlResult) ProtoReflect() protoreflect.Message {
	mi := &file_protos_ydb_scripting_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ExplainYqlResult.ProtoReflect.Descriptor instead.
func (*ExplainYqlResult) Descriptor() ([]byte, []int) {
	return file_protos_ydb_scripting_proto_rawDescGZIP(), []int{7}
}

func (x *ExplainYqlResult) GetParametersTypes() map[string]*Ydb.Type {
	if x != nil {
		return x.ParametersTypes
	}
	return nil
}

func (x *ExplainYqlResult) GetPlan() string {
	if x != nil {
		return x.Plan
	}
	return ""
}

var File_protos_ydb_scripting_proto protoreflect.FileDescriptor

var file_protos_ydb_scripting_proto_rawDesc = []byte{
	0x0a, 0x1a, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x79, 0x64, 0x62, 0x5f, 0x73, 0x63, 0x72,
	0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0d, 0x59, 0x64,
	0x62, 0x2e, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x1a, 0x1a, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x73, 0x2f, 0x79, 0x64, 0x62, 0x5f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x16, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f,
	0x79, 0x64, 0x62, 0x5f, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x16, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x79, 0x64, 0x62, 0x5f, 0x74, 0x61, 0x62, 0x6c,
	0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1c, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f,
	0x79, 0x64, 0x62, 0x5f, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x79, 0x64,
	0x62, 0x5f, 0x69, 0x73, 0x73, 0x75, 0x65, 0x5f, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1d, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f, 0x79, 0x64,
	0x62, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x5f, 0x63, 0x6f, 0x64, 0x65, 0x73, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x22, 0xe4, 0x02, 0x0a, 0x11, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65,
	0x59, 0x71, 0x6c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4a, 0x0a, 0x10, 0x6f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1f, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50,
	0x61, 0x72, 0x61, 0x6d, 0x73, 0x52, 0x0f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x12, 0x50,
	0x0a, 0x0a, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20, 0x03,
	0x28, 0x0b, 0x32, 0x30, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69,
	0x6e, 0x67, 0x2e, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x2e, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x52, 0x0a, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73,
	0x12, 0x49, 0x0a, 0x0d, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x5f, 0x73, 0x74, 0x61, 0x74,
	0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x24, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x54, 0x61,
	0x62, 0x6c, 0x65, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x53, 0x74, 0x61, 0x74, 0x73, 0x43, 0x6f,
	0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x4d, 0x6f, 0x64, 0x65, 0x52, 0x0c, 0x63,
	0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x53, 0x74, 0x61, 0x74, 0x73, 0x1a, 0x4e, 0x0a, 0x0f, 0x50,
	0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10,
	0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79,
	0x12, 0x25, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x0f, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x54, 0x79, 0x70, 0x65, 0x64, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01, 0x22, 0x4d, 0x0a, 0x12, 0x45,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
	0x65, 0x12, 0x37, 0x0a, 0x09, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52,
	0x09, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x80, 0x01, 0x0a, 0x10, 0x45,
	0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12,
	0x2f, 0x0a, 0x0b, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x5f, 0x73, 0x65, 0x74, 0x73, 0x18, 0x01,
	0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x53, 0x65, 0x74, 0x52, 0x0a, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x53, 0x65, 0x74, 0x73,
	0x12, 0x3b, 0x0a, 0x0b, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x73, 0x74, 0x61, 0x74, 0x73, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x54, 0x61, 0x62, 0x6c,
	0x65, 0x53, 0x74, 0x61, 0x74, 0x73, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79, 0x53, 0x74, 0x61, 0x74,
	0x73, 0x52, 0x0a, 0x71, 0x75, 0x65, 0x72, 0x79, 0x53, 0x74, 0x61, 0x74, 0x73, 0x22, 0xbf, 0x01,
	0x0a, 0x19, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x50, 0x61, 0x72, 0x74,
	0x69, 0x61, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x31, 0x0a, 0x06, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x19, 0x2e, 0x59, 0x64,
	0x62, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x49, 0x64, 0x73, 0x2e, 0x53, 0x74, 0x61, 0x74,
	0x75, 0x73, 0x43, 0x6f, 0x64, 0x65, 0x52, 0x06, 0x73, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x2f,
	0x0a, 0x06, 0x69, 0x73, 0x73, 0x75, 0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x17,
	0x2e, 0x59, 0x64, 0x62, 0x2e, 0x49, 0x73, 0x73, 0x75, 0x65, 0x2e, 0x49, 0x73, 0x73, 0x75, 0x65,
	0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x52, 0x06, 0x69, 0x73, 0x73, 0x75, 0x65, 0x73, 0x12,
	0x3e, 0x0a, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x26, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x2e,
	0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x50, 0x61, 0x72, 0x74, 0x69, 0x61,
	0x6c, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x22,
	0xaf, 0x01, 0x0a, 0x17, 0x45, 0x78, 0x65, 0x63, 0x75, 0x74, 0x65, 0x59, 0x71, 0x6c, 0x50, 0x61,
	0x72, 0x74, 0x69, 0x61, 0x6c, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x28, 0x0a, 0x10, 0x72,
	0x65, 0x73, 0x75, 0x6c, 0x74, 0x5f, 0x73, 0x65, 0x74, 0x5f, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0e, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x53, 0x65, 0x74,
	0x49, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x2d, 0x0a, 0x0a, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x5f,
	0x73, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x59, 0x64, 0x62, 0x2e,
	0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x53, 0x65, 0x74, 0x52, 0x09, 0x72, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x53, 0x65, 0x74, 0x12, 0x3b, 0x0a, 0x0b, 0x71, 0x75, 0x65, 0x72, 0x79, 0x5f, 0x73, 0x74,
	0x61, 0x74, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x59, 0x64, 0x62, 0x2e,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x53, 0x74, 0x61, 0x74, 0x73, 0x2e, 0x51, 0x75, 0x65, 0x72, 0x79,
	0x53, 0x74, 0x61, 0x74, 0x73, 0x52, 0x0a, 0x71, 0x75, 0x65, 0x72, 0x79, 0x53, 0x74, 0x61, 0x74,
	0x73, 0x22, 0xe8, 0x01, 0x0a, 0x11, 0x45, 0x78, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x59, 0x71, 0x6c,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x4a, 0x0a, 0x10, 0x6f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x1f, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x73, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x61, 0x72, 0x61,
	0x6d, 0x73, 0x52, 0x0f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x61, 0x72,
	0x61, 0x6d, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x06, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x12, 0x39, 0x0a, 0x04, 0x6d,
	0x6f, 0x64, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x25, 0x2e, 0x59, 0x64, 0x62, 0x2e,
	0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x45, 0x78, 0x70, 0x6c, 0x61, 0x69,
	0x6e, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x4d, 0x6f, 0x64, 0x65,
	0x52, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x22, 0x34, 0x0a, 0x04, 0x4d, 0x6f, 0x64, 0x65, 0x12, 0x14,
	0x0a, 0x10, 0x4d, 0x4f, 0x44, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49,
	0x45, 0x44, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x56, 0x41, 0x4c, 0x49, 0x44, 0x41, 0x54, 0x45,
	0x10, 0x02, 0x12, 0x08, 0x0a, 0x04, 0x50, 0x4c, 0x41, 0x4e, 0x10, 0x03, 0x22, 0x4d, 0x0a, 0x12,
	0x45, 0x78, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x37, 0x0a, 0x09, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18,
	0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x59, 0x64, 0x62, 0x2e, 0x4f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x52, 0x09, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xd6, 0x01, 0x0a, 0x10,
	0x45, 0x78, 0x70, 0x6c, 0x61, 0x69, 0x6e, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74,
	0x12, 0x5f, 0x0a, 0x10, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x5f, 0x74,
	0x79, 0x70, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x34, 0x2e, 0x59, 0x64, 0x62,
	0x2e, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x2e, 0x45, 0x78, 0x70, 0x6c, 0x61,
	0x69, 0x6e, 0x59, 0x71, 0x6c, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x2e, 0x50, 0x61, 0x72, 0x61,
	0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x54, 0x79, 0x70, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79,
	0x52, 0x0f, 0x70, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74, 0x65, 0x72, 0x73, 0x54, 0x79, 0x70, 0x65,
	0x73, 0x12, 0x12, 0x0a, 0x04, 0x70, 0x6c, 0x61, 0x6e, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x04, 0x70, 0x6c, 0x61, 0x6e, 0x1a, 0x4d, 0x0a, 0x14, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x65, 0x74,
	0x65, 0x72, 0x73, 0x54, 0x79, 0x70, 0x65, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12,
	0x1f, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x09,
	0x2e, 0x59, 0x64, 0x62, 0x2e, 0x54, 0x79, 0x70, 0x65, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65,
	0x3a, 0x02, 0x38, 0x01, 0x42, 0x6c, 0x0a, 0x18, 0x74, 0x65, 0x63, 0x68, 0x2e, 0x79, 0x64, 0x62,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67,
	0x42, 0x0f, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0x50, 0x72, 0x6f, 0x74, 0x6f,
	0x73, 0x5a, 0x3c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x79, 0x64,
	0x62, 0x2d, 0x70, 0x6c, 0x61, 0x74, 0x66, 0x6f, 0x72, 0x6d, 0x2f, 0x79, 0x64, 0x62, 0x2d, 0x67,
	0x6f, 0x2d, 0x67, 0x65, 0x6e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x73, 0x2f, 0x59, 0x64, 0x62, 0x5f, 0x53, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6e, 0x67, 0xf8,
	0x01, 0x01, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_protos_ydb_scripting_proto_rawDescOnce sync.Once
	file_protos_ydb_scripting_proto_rawDescData = file_protos_ydb_scripting_proto_rawDesc
)

func file_protos_ydb_scripting_proto_rawDescGZIP() []byte {
	file_protos_ydb_scripting_proto_rawDescOnce.Do(func() {
		file_protos_ydb_scripting_proto_rawDescData = protoimpl.X.CompressGZIP(file_protos_ydb_scripting_proto_rawDescData)
	})
	return file_protos_ydb_scripting_proto_rawDescData
}

var file_protos_ydb_scripting_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_protos_ydb_scripting_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_protos_ydb_scripting_proto_goTypes = []interface{}{
	(ExplainYqlRequest_Mode)(0),              // 0: Ydb.Scripting.ExplainYqlRequest.Mode
	(*ExecuteYqlRequest)(nil),                // 1: Ydb.Scripting.ExecuteYqlRequest
	(*ExecuteYqlResponse)(nil),               // 2: Ydb.Scripting.ExecuteYqlResponse
	(*ExecuteYqlResult)(nil),                 // 3: Ydb.Scripting.ExecuteYqlResult
	(*ExecuteYqlPartialResponse)(nil),        // 4: Ydb.Scripting.ExecuteYqlPartialResponse
	(*ExecuteYqlPartialResult)(nil),          // 5: Ydb.Scripting.ExecuteYqlPartialResult
	(*ExplainYqlRequest)(nil),                // 6: Ydb.Scripting.ExplainYqlRequest
	(*ExplainYqlResponse)(nil),               // 7: Ydb.Scripting.ExplainYqlResponse
	(*ExplainYqlResult)(nil),                 // 8: Ydb.Scripting.ExplainYqlResult
	nil,                                      // 9: Ydb.Scripting.ExecuteYqlRequest.ParametersEntry
	nil,                                      // 10: Ydb.Scripting.ExplainYqlResult.ParametersTypesEntry
	(*Ydb_Operations.OperationParams)(nil),   // 11: Ydb.Operations.OperationParams
	(Ydb_Table.QueryStatsCollection_Mode)(0), // 12: Ydb.Table.QueryStatsCollection.Mode
	(*Ydb_Operations.Operation)(nil),         // 13: Ydb.Operations.Operation
	(*Ydb.ResultSet)(nil),                    // 14: Ydb.ResultSet
	(*Ydb_TableStats.QueryStats)(nil),        // 15: Ydb.TableStats.QueryStats
	(Ydb.StatusIds_StatusCode)(0),            // 16: Ydb.StatusIds.StatusCode
	(*Ydb_Issue.IssueMessage)(nil),           // 17: Ydb.Issue.IssueMessage
	(*Ydb.TypedValue)(nil),                   // 18: Ydb.TypedValue
	(*Ydb.Type)(nil),                         // 19: Ydb.Type
}
var file_protos_ydb_scripting_proto_depIdxs = []int32{
	11, // 0: Ydb.Scripting.ExecuteYqlRequest.operation_params:type_name -> Ydb.Operations.OperationParams
	9,  // 1: Ydb.Scripting.ExecuteYqlRequest.parameters:type_name -> Ydb.Scripting.ExecuteYqlRequest.ParametersEntry
	12, // 2: Ydb.Scripting.ExecuteYqlRequest.collect_stats:type_name -> Ydb.Table.QueryStatsCollection.Mode
	13, // 3: Ydb.Scripting.ExecuteYqlResponse.operation:type_name -> Ydb.Operations.Operation
	14, // 4: Ydb.Scripting.ExecuteYqlResult.result_sets:type_name -> Ydb.ResultSet
	15, // 5: Ydb.Scripting.ExecuteYqlResult.query_stats:type_name -> Ydb.TableStats.QueryStats
	16, // 6: Ydb.Scripting.ExecuteYqlPartialResponse.status:type_name -> Ydb.StatusIds.StatusCode
	17, // 7: Ydb.Scripting.ExecuteYqlPartialResponse.issues:type_name -> Ydb.Issue.IssueMessage
	5,  // 8: Ydb.Scripting.ExecuteYqlPartialResponse.result:type_name -> Ydb.Scripting.ExecuteYqlPartialResult
	14, // 9: Ydb.Scripting.ExecuteYqlPartialResult.result_set:type_name -> Ydb.ResultSet
	15, // 10: Ydb.Scripting.ExecuteYqlPartialResult.query_stats:type_name -> Ydb.TableStats.QueryStats
	11, // 11: Ydb.Scripting.ExplainYqlRequest.operation_params:type_name -> Ydb.Operations.OperationParams
	0,  // 12: Ydb.Scripting.ExplainYqlRequest.mode:type_name -> Ydb.Scripting.ExplainYqlRequest.Mode
	13, // 13: Ydb.Scripting.ExplainYqlResponse.operation:type_name -> Ydb.Operations.Operation
	10, // 14: Ydb.Scripting.ExplainYqlResult.parameters_types:type_name -> Ydb.Scripting.ExplainYqlResult.ParametersTypesEntry
	18, // 15: Ydb.Scripting.ExecuteYqlRequest.ParametersEntry.value:type_name -> Ydb.TypedValue
	19, // 16: Ydb.Scripting.ExplainYqlResult.ParametersTypesEntry.value:type_name -> Ydb.Type
	17, // [17:17] is the sub-list for method output_type
	17, // [17:17] is the sub-list for method input_type
	17, // [17:17] is the sub-list for extension type_name
	17, // [17:17] is the sub-list for extension extendee
	0,  // [0:17] is the sub-list for field type_name
}

func init() { file_protos_ydb_scripting_proto_init() }
func file_protos_ydb_scripting_proto_init() {
	if File_protos_ydb_scripting_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_protos_ydb_scripting_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecuteYqlRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecuteYqlResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecuteYqlResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecuteYqlPartialResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExecuteYqlPartialResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExplainYqlRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExplainYqlResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_protos_ydb_scripting_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ExplainYqlResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_protos_ydb_scripting_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_protos_ydb_scripting_proto_goTypes,
		DependencyIndexes: file_protos_ydb_scripting_proto_depIdxs,
		EnumInfos:         file_protos_ydb_scripting_proto_enumTypes,
		MessageInfos:      file_protos_ydb_scripting_proto_msgTypes,
	}.Build()
	File_protos_ydb_scripting_proto = out.File
	file_protos_ydb_scripting_proto_rawDesc = nil
	file_protos_ydb_scripting_proto_goTypes = nil
	file_protos_ydb_scripting_proto_depIdxs = nil
}