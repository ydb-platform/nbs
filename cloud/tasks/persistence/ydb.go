package persistence

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	ydb_credentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_options "github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_indexed "github.com/ydb-platform/ydb-go-sdk/v3/table/result/indexed"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
	grpc_codes "google.golang.org/grpc/codes"
)

////////////////////////////////////////////////////////////////////////////////

type Type = ydb_types.Type

const (
	TypeBool      = ydb_types.TypeBool
	TypeInt32     = ydb_types.TypeInt32
	TypeUint32    = ydb_types.TypeUint32
	TypeInt64     = ydb_types.TypeInt64
	TypeUint64    = ydb_types.TypeUint64
	TypeDouble    = ydb_types.TypeDouble
	TypeTimestamp = ydb_types.TypeTimestamp
	TypeInterval  = ydb_types.TypeInterval
	TypeString    = ydb_types.TypeString
	TypeBytes     = ydb_types.TypeBytes
	TypeUTF8      = ydb_types.TypeText
)

func Optional(t ydb_types.Type) ydb_types.Type { return ydb_types.Optional(t) }

func List(t Type) ydb_types.Type { return ydb_types.List(t) }

////////////////////////////////////////////////////////////////////////////////

type Value = ydb_types.Value

type RawValue = ydb_types.RawValue

func OptionalValue(v Value) Value { return ydb_types.OptionalValue(v) }

func TupleValue(values ...Value) Value {
	return ydb_types.TupleValue(values...)
}

func ZeroValue(t Type) Value { return ydb_types.ZeroValue(t) }

func BoolValue(v bool) Value { return ydb_types.BoolValue(v) }

func Int32Value(v int32) Value { return ydb_types.Int32Value(v) }

func Uint32Value(v uint32) Value { return ydb_types.Uint32Value(v) }

func Int64Value(v int64) Value { return ydb_types.Int64Value(v) }

func Uint64Value(v uint64) Value { return ydb_types.Uint64Value(v) }

func FloatValue(v float32) Value { return ydb_types.FloatValue(v) }

func DoubleValue(v float64) Value { return ydb_types.DoubleValue(v) }

func BytesValue(v []byte) Value { return ydb_types.BytesValue(v) }

func StringValue(v []byte) Value { return ydb_types.StringValue(v) }

func UTF8Value(v string) Value { return ydb_types.UTF8Value(v) }

func ListValue(items ...Value) Value { return ydb_types.ListValue(items...) }

func StructFieldValue(name string, v Value) ydb_types.StructValueOption {
	return ydb_types.StructFieldValue(name, v)
}

func StructValue(opts ...ydb_types.StructValueOption) Value {
	return ydb_types.StructValue(opts...)
}

////////////////////////////////////////////////////////////////////////////////

func DatetimeValueFromTime(t time.Time) Value {
	return ydb_types.DatetimeValueFromTime(t)
}

func TimestampValue(t time.Time) Value {
	if t.IsZero() {
		return ydb_types.ZeroValue(ydb_types.TypeTimestamp)
	}

	return ydb_types.TimestampValueFromTime(t)
}

func IntervalValue(t time.Duration) Value {
	return ydb_types.IntervalValueFromDuration(t)
}

////////////////////////////////////////////////////////////////////////////////

type Result struct {
	ctx context.Context
	tx  *Transaction
	res ydb_result.BaseResult
}

func (r *Result) NextResultSet(ctx context.Context, columns ...string) bool {
	return r.res.NextResultSet(ctx, columns...)
}

func (r *Result) NextRow() bool { return r.res.NextRow() }

func (r *Result) ScanWithDefaults(values ...ydb_indexed.Required) error {
	err := r.res.ScanWithDefaults(values...)
	return r.handleError(err)
}

func (r *Result) Scan(values ...ydb_indexed.RequiredOrOptional) error {
	err := r.res.Scan(values...)
	return r.handleError(err)
}

func (r *Result) ScanNamed(namedValues ...ydb_named.Value) error {
	err := r.res.ScanNamed(namedValues...)
	return r.handleError(err)
}

func (r *Result) Err() error { return r.res.Err() }

func (r *Result) Close() error { return r.res.Close() }

////////////////////////////////////////////////////////////////////////////////

func (r *Result) handleError(err error) error {
	if err != nil {
		if r.tx != nil {
			commitErr := r.tx.Commit(r.ctx)
			if commitErr != nil {
				return commitErr
			}
		}

		return errors.NewNonRetriableErrorf("failed to parse YDB result: %w", err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func OptionalWithDefault(
	columnName string,
	destinationValueReference interface{},
) ydb_named.Value {

	return ydb_named.OptionalWithDefault(columnName, destinationValueReference)
}

func ValueParam(name string, v Value) ydb_table.ParameterOption {
	return ydb_table.ValueParam(name, v)
}

////////////////////////////////////////////////////////////////////////////////

func ReadOrdered() ydb_options.ReadTableOption {
	return ydb_options.ReadOrdered()
}

func ReadColumn(name string) ydb_options.ReadTableOption {
	return ydb_options.ReadColumn(name)
}

func ReadLessOrEqual(v Value) ydb_options.ReadTableOption {
	return ydb_options.ReadLessOrEqual(v)
}

func ReadGreaterOrEqual(v Value) ydb_options.ReadTableOption {
	return ydb_options.ReadGreaterOrEqual(v)
}

////////////////////////////////////////////////////////////////////////////////

func IsTransportError(err error, codes ...grpc_codes.Code) bool {
	return ydb.IsTransportError(err, codes...)
}

////////////////////////////////////////////////////////////////////////////////

type CreateTableDescription struct {
	Columns                []ydb_options.Column
	PrimaryKey             []string
	SecondaryKeys          []string
	ColumnFamilies         []ydb_options.ColumnFamily
	UniformPartitions      uint64
	ExternalBlobsMediaKind string
}

type CreateTableOption func(*CreateTableDescription)

func WithColumn(name string, typ Type) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.Columns = append(d.Columns, ydb_options.Column{
			Name: name,
			Type: typ,
		})
	}
}

func WithColumnAndFamily(name string, typ Type, family string) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.Columns = append(d.Columns, ydb_options.Column{
			Name:   name,
			Type:   typ,
			Family: family,
		})
	}
}

func WithPrimaryKeyColumn(columns ...string) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.PrimaryKey = append(d.PrimaryKey, columns...)
	}
}

func WithSecondaryKeyColumn(columns ...string) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.SecondaryKeys = append(d.SecondaryKeys, columns...)
	}
}

func WithColumnFamilies(cf ...ydb_options.ColumnFamily) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.ColumnFamilies = append(d.ColumnFamilies, cf...)
	}
}

func WithUniformPartitions(n uint64) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.UniformPartitions = n
	}
}

func WithExternalBlobs(mediaKind string) CreateTableOption {
	return func(d *CreateTableDescription) {
		d.ExternalBlobsMediaKind = mediaKind
	}
}

func NewCreateTableDescription(opts ...CreateTableOption) CreateTableDescription {
	var result CreateTableDescription
	for _, opt := range opts {
		opt(&result)
	}
	return result
}

////////////////////////////////////////////////////////////////////////////////

type Transaction struct {
	tx          ydb_table.Transaction
	callTimeout time.Duration
	metrics     *ydbMetrics
}

func (t *Transaction) Execute(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (res Result, err error) {

	ctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()

	defer t.metrics.StatCall(ctx, "transaction/Execute", query)(&err)

	var ydbRes ydb_result.Result
	ydbRes, err = t.tx.Execute(
		ctx,
		query,
		ydb_table.NewQueryParameters(params...),
	)
	if err != nil {
		logging.Warn(
			ctx,
			"query failed: query %v: %v",
			query,
			err,
		)

		// TODO: some errors should not be retriable.
		return Result{}, errors.NewRetriableErrorf(
			"failed: query %v: %w",
			query,
			err,
		)
	}

	return Result{ctx: ctx, tx: t, res: ydbRes}, nil
}

func (t *Transaction) Commit(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()

	defer t.metrics.StatCall(ctx, "transaction/Commit", "")(&err)

	_, err = t.tx.CommitTx(ctx)
	if err != nil {
		// TODO: some errors should not be retriable.
		return errors.NewRetriableErrorf(
			"failed to commit transaction: %w",
			err,
		)
	}

	return nil
}

func (t *Transaction) Rollback(ctx context.Context) (err error) {
	ctx, cancel := context.WithTimeout(ctx, t.callTimeout)
	defer cancel()

	return t.tx.Rollback(ctx)
}

////////////////////////////////////////////////////////////////////////////////

type Session struct {
	session     ydb_table.Session
	callTimeout time.Duration
	metrics     *ydbMetrics
}

func (s *Session) BeginRWTransaction(
	ctx context.Context,
) (tx *Transaction, err error) {

	ctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()

	defer s.metrics.StatCall(ctx, "session/BeginTransaction", "")(&err)

	settings := ydb_table.TxSettings(ydb_table.WithSerializableReadWrite())

	var ydbTx ydb_table.Transaction
	ydbTx, err = s.session.BeginTransaction(ctx, settings)
	if err != nil {
		logging.Warn(ctx, "failed to begin transaction: %v", err)

		// TODO: some errors should not be retriable.
		return nil, errors.NewRetriableErrorf(
			"failed to begin transaction: %w",
			err,
		)
	}

	return &Transaction{
		tx:          ydbTx,
		callTimeout: s.callTimeout,
		metrics:     s.metrics,
	}, nil
}

func (s *Session) ExecuteRO(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (Result, error) {

	tx := ydb_table.TxControl(
		ydb_table.BeginTx(
			ydb_table.WithOnlineReadOnly(),
		),
		ydb_table.CommitTx(),
	)
	_, res, err := s.execute(ctx, tx, query, params...)
	return res, err
}

func (s *Session) StreamExecuteRO(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (Result, error) {

	res, err := s.session.StreamExecuteScanQuery(
		ctx,
		query,
		ydb_table.NewQueryParameters(params...),
	)
	if err != nil {
		// TODO: some errors should not be retriable.
		return Result{}, errors.NewRetriableErrorf(
			"StreamExecuteScanQuery failed, query %v: %w",
			query,
			err,
		)
	}

	return Result{res: res}, nil
}

func (s *Session) ExecuteRW(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (Result, error) {

	tx := ydb_table.TxControl(
		ydb_table.BeginTx(
			ydb_table.WithSerializableReadWrite(),
		),
		ydb_table.CommitTx(),
	)
	_, res, err := s.execute(ctx, tx, query, params...)
	return res, err
}

func (s *Session) StreamReadTable(
	ctx context.Context,
	path string,
	opts ...ydb_options.ReadTableOption,
) (Result, error) {

	res, err := s.session.StreamReadTable(ctx, path, opts...)
	if err != nil {
		// TODO: some errors should not be retriable.
		return Result{}, errors.NewRetriableErrorf(
			"StreamReadTable failed, path %v: %w",
			path,
			err,
		)
	}

	return Result{res: res}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *Session) execute(
	ctx context.Context,
	control *ydb_table.TransactionControl,
	query string,
	params ...ydb_table.ParameterOption,
) (tx ydb_table.Transaction, res Result, err error) {

	ctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()

	defer s.metrics.StatCall(ctx, "session/Execute", query)(&err)

	var ydbRes ydb_result.Result
	tx, ydbRes, err = s.session.Execute(
		ctx,
		control,
		query,
		ydb_table.NewQueryParameters(params...),
	)
	if err != nil {
		logging.Warn(
			ctx,
			"session execution failed: query %v: %v",
			query,
			err,
		)

		// TODO: some errors should not be retriable.
		return nil, Result{}, errors.NewRetriableErrorf(
			"session execution failed: query %v: %w",
			query,
			err,
		)
	}

	return tx, Result{res: ydbRes}, nil
}

func (s *Session) CreateOrAlterTable(
	ctx context.Context,
	fullPath string,
	description CreateTableDescription,
	dropUnusedColumns bool,
) (err error) {

	ctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()

	defer s.metrics.StatCall(
		ctx,
		"session/CreateOrAlterTable",
		fmt.Sprintf("At path: %q", fullPath),
	)(&err)

	return createOrAlterTable(
		ctx,
		s.session,
		fullPath,
		description,
		dropUnusedColumns,
	)
}

func (s *Session) DropTable(
	ctx context.Context,
	fullPath string,
) (err error) {

	ctx, cancel := context.WithTimeout(ctx, s.callTimeout)
	defer cancel()

	defer s.metrics.StatCall(
		ctx,
		"session/DropTable",
		fmt.Sprintf("At path: %q", fullPath),
	)(&err)

	return dropTable(ctx, s.session, fullPath)
}

////////////////////////////////////////////////////////////////////////////////

type YDBClient struct {
	db          *ydb.Driver
	metrics     *ydbMetrics
	callTimeout time.Duration
	database    string
	rootPath    string
}

func (c *YDBClient) Close(ctx context.Context) error {
	return c.db.Close(ctx)
}

func (c *YDBClient) AbsolutePath(elem ...string) string {
	return path.Join(c.database, c.rootPath, path.Join(elem...))
}

func (c *YDBClient) CreateOrAlterTable(
	ctx context.Context,
	folder string,
	name string,
	description CreateTableDescription,
	dropUnusedColumns bool,
) error {

	folderFullPath := c.AbsolutePath(folder)
	fullPath := path.Join(folderFullPath, name)

	return c.Execute(
		ctx,
		func(ctx context.Context, s *Session) (err error) {
			err = c.makeDirs(ctx, folderFullPath)
			if err != nil {
				return err
			}

			return s.CreateOrAlterTable(ctx,
				fullPath,
				description,
				dropUnusedColumns,
			)
		},
	)
}

func (c *YDBClient) DropTable(
	ctx context.Context,
	folder string,
	name string,
) error {

	folderFullPath := c.AbsolutePath(folder)
	fullPath := path.Join(folderFullPath, name)

	return c.Execute(
		ctx,
		func(ctx context.Context, s *Session) error {
			return s.DropTable(ctx, fullPath)
		},
	)
}

func (c *YDBClient) Execute(
	ctx context.Context,
	op func(context.Context, *Session) error,
) error {

	adapter := func(ctx context.Context, session ydb_table.Session) error {
		return op(ctx, &Session{
			session:     session,
			callTimeout: c.callTimeout,
			metrics:     c.metrics,
		})
	}

	err := c.db.Table().Do(ctx, adapter, ydb_table.WithIdempotent())
	if err != nil {
		// TODO: some errors should not be retriable.
		return errors.NewRetriableError(err)
	}

	return nil
}

func (c *YDBClient) ExecuteRO(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (Result, error) {

	var res Result

	err := c.Execute(ctx, func(ctx context.Context, session *Session) error {
		var err error
		res, err = session.ExecuteRO(ctx, query, params...)
		return err
	})

	return res, err
}

func (c *YDBClient) ExecuteRW(
	ctx context.Context,
	query string,
	params ...ydb_table.ParameterOption,
) (Result, error) {

	var res Result

	err := c.Execute(ctx, func(ctx context.Context, session *Session) error {
		var err error
		res, err = session.ExecuteRW(ctx, query, params...)
		return err
	})

	return res, err
}

////////////////////////////////////////////////////////////////////////////////

func (c *YDBClient) makeDirs(ctx context.Context, absolutePath string) (err error) {
	defer c.metrics.StatCall(
		ctx,
		"client/makeDirs",
		fmt.Sprintf("At path: %q", absolutePath),
	)(&err)

	if !strings.HasPrefix(absolutePath, c.database) {
		return errors.NewNonRetriableErrorf(
			"'%v' is expected to be rooted at '%v'",
			absolutePath,
			c.database,
		)
	}

	relativePath := strings.TrimPrefix(absolutePath, c.database)
	parts := strings.Split(relativePath, "/")

	dirPath := c.database
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}

		dirPath = path.Join(dirPath, part)
		err = c.db.Scheme().MakeDirectory(ctx, dirPath)
		if err != nil {
			return errors.NewNonRetriableErrorf(
				"cannot make directory %v: %w",
				dirPath,
				err,
			)
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func getDSN(config *persistence_config.PersistenceConfig) string {
	var protocol string
	if config.GetSecure() {
		protocol = "grpcs"
	} else {
		protocol = "grpc"
	}

	return protocol + "://" + config.GetEndpoint() + "/" + config.GetDatabase()
}

func NewYDBClient(
	ctx context.Context,
	config *persistence_config.PersistenceConfig,
	registry metrics.Registry,
	opts ...option,
) (*YDBClient, error) {

	h := &optionsHolder{
		details: trace.DriverRepeaterEvents |
			trace.TablePoolEvents |
			trace.RetryEvents |
			trace.DiscoveryEvents |
			trace.SchemeEvents,
	}
	for _, o := range opts {
		o(h)
	}

	l := &logger{
		logger: logging.GetLogger(ctx),
	}

	if l.logger == nil {
		return nil, errors.NewNonRetriableErrorf("failed to initialize logger")
	}

	options := []ydb.Option{
		ydb.WithPanicCallback(func(e interface{}) {
			fmt.Fprintf(os.Stdout, "panic from ydb driver: %v\n%s", e, debug.Stack())
			os.Exit(1)
		}),
		ydb.WithLogger(l, h.details),
	}

	if !config.GetDisableAuthentication() {
		options = append(options, ydb.WithCredentials(h.creds))
	}

	if h.registry != nil {
		options = append(
			options,
			WithTraces(h.registry, WithTraceDetails(h.details)),
		)
	}

	if config.GetSecure() && len(config.GetRootCertsFile()) != 0 {
		options = append(
			options,
			ydb.WithCertificatesFromFile(config.GetRootCertsFile()),
		)
	}

	connectionTimeout, err := time.ParseDuration(config.GetConnectionTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse ConnectionTimeout: %w",
			err,
		)
	}

	options = append(
		options,
		ydb.WithDialTimeout(connectionTimeout),
	)

	driver, err := ydb.Open(
		ctx,
		getDSN(config),
		options...,
	)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf("failed to dial: %w", err)
	}

	callTimeout, err := time.ParseDuration(config.GetCallTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse QueryTimeout: %w",
			err,
		)
	}

	return &YDBClient{
		db:          driver,
		metrics:     newYDBMetrics(registry, callTimeout),
		callTimeout: callTimeout,
		database:    config.GetDatabase(),
		rootPath:    config.GetRootPath(),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type optionsHolder struct {
	registry metrics.Registry
	creds    ydb_credentials.Credentials
	details  trace.Details
}

type option func(h *optionsHolder)

func WithRegistry(registry metrics.Registry) option {
	return func(h *optionsHolder) {
		h.registry = registry
	}
}

func WithCredentials(creds ydb_credentials.Credentials) option {
	return func(h *optionsHolder) {
		h.creds = creds
	}
}

func WithDetails(details trace.Details) option {
	return func(h *optionsHolder) {
		h.details = details
	}
}

////////////////////////////////////////////////////////////////////////////////

func getIndexName(columnName string) string {
	return columnName + "_index"
}

func createTable(
	ctx context.Context,
	session ydb_table.Session,
	fullPath string,
	description CreateTableDescription,
) error {

	options := make([]ydb_options.CreateTableOption, 0, len(description.Columns)+1)
	for _, column := range description.Columns {
		options = append(options, ydb_options.WithColumn(column.Name, column.Type))
	}

	options = append(options, ydb_options.WithPrimaryKeyColumn(description.PrimaryKey...))

	for _, key := range description.SecondaryKeys {
		options = append(
			options,
			ydb_options.WithIndex(
				getIndexName(key),
				ydb_options.WithIndexType(ydb_options.GlobalIndex()),
				ydb_options.WithIndexColumns(key),
			),
		)
	}

	options = append(options, ydb_options.WithColumnFamilies(description.ColumnFamilies...))

	if description.UniformPartitions > 0 {
		options = append(options, ydb_options.WithProfile(
			ydb_options.WithPartitioningPolicy(
				ydb_options.WithPartitioningPolicyUniformPartitions(description.UniformPartitions),
			),
		))
	}

	if len(description.ExternalBlobsMediaKind) > 0 {
		options = append(options, ydb_options.WithStorageSettings(
			ydb_options.StorageSettings{
				External:           ydb_options.StoragePool{Media: description.ExternalBlobsMediaKind},
				StoreExternalBlobs: ydb_options.FeatureEnabled,
			},
		))
		options = append(options, ydb_options.WithProfile(
			ydb_options.WithPartitioningPolicy(
				ydb_options.WithPartitioningPolicyMode(ydb_options.PartitioningDisabled),
			),
		))
	}

	return session.CreateTable(ctx, fullPath, options...)
}

func primaryKeysMatch(
	currentKey []string,
	key []string,
) bool {

	sort.Strings(currentKey)
	sort.Strings(key)
	if len(key) != len(currentKey) {
		return false
	}

	for i := range currentKey {
		if key[i] != currentKey[i] {
			return false
		}
	}

	return true
}

func splitColumns(
	lhs []ydb_options.Column,
	rhs []ydb_options.Column,
) (lhsOnly []ydb_options.Column, rhsOnly []ydb_options.Column, err error) {

	for _, column := range rhs {
		i := sort.Search(len(lhs), func(i int) bool {
			return lhs[i].Name >= column.Name
		})
		if i >= len(lhs) || lhs[i].Name != column.Name {
			rhsOnly = append(rhsOnly, column)
		} else if !ydb_types.Equal(lhs[i].Type, column.Type) {
			err = errors.NewNonRetriableErrorf(
				"column %v type mismatch old=%v, new=%v",
				column.Name,
				lhs[i].Type,
				column.Type,
			)
		}
	}

	for _, column := range lhs {
		i := sort.Search(len(rhs), func(i int) bool {
			return rhs[i].Name >= column.Name
		})
		if i >= len(rhs) || rhs[i].Name != column.Name {
			lhsOnly = append(lhsOnly, column)
		}
	}

	return
}

func prepareAlterColumnOptions(
	currentDescription ydb_options.Description,
	description CreateTableDescription,
	dropUnusedColumns bool,
) ([]ydb_options.AlterTableOption, error) {

	currentPrimaryKey := currentDescription.PrimaryKey
	primaryKey := description.PrimaryKey
	if !primaryKeysMatch(currentPrimaryKey, primaryKey) {
		return nil, errors.NewNonRetriableErrorf(
			"cannot change primary key. Current=%v, Requested=%v",
			currentPrimaryKey,
			primaryKey,
		)
	}

	currentColumns := currentDescription.Columns
	columns := description.Columns
	sort.Slice(currentColumns, func(i, j int) bool {
		return currentColumns[i].Name < currentColumns[j].Name
	})
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].Name < columns[j].Name
	})

	unusedColumns, addedColumns, err := splitColumns(currentColumns, columns)
	if err != nil {
		return nil, err
	}

	alterTableOptions := make([]ydb_options.AlterTableOption, 0)

	for _, column := range addedColumns {
		alterTableOptions = append(
			alterTableOptions,
			ydb_options.WithAddColumn(column.Name, column.Type),
		)
	}

	if dropUnusedColumns {
		for _, column := range unusedColumns {
			alterTableOptions = append(
				alterTableOptions,
				ydb_options.WithDropColumn(column.Name),
			)
		}
	}

	return alterTableOptions, nil
}

func getUnusedAndAddedIndexes(
	currentDescription ydb_options.Description,
	description CreateTableDescription,
) ([]string, []string, error) {

	lhsSet := common.NewStringSet()
	for _, index := range currentDescription.Indexes {
		if len(index.IndexColumns) != 1 {
			return nil, nil, errors.NewNonRetriableErrorf(
				"indexes with more than one column are not supported",
			)
		}

		lhsSet.Add(index.IndexColumns[0])
	}

	rhsSet := common.NewStringSet(description.SecondaryKeys...)

	lhsOnly := lhsSet.Subtract(rhsSet)
	rhsOnly := rhsSet.Subtract(lhsSet)

	return lhsOnly.List(), rhsOnly.List(), nil
}

func alterTable(
	ctx context.Context,
	session ydb_table.Session,
	fullPath string,
	currentDescription ydb_options.Description,
	description CreateTableDescription,
	dropUnusedColumns bool,
) error {

	// Indexes must be created and deleted one-by-one
	// https://ydb.tech/docs/en/concepts/secondary_indexes#index-add

	unusedIndexes, addedIndexes, err := getUnusedAndAddedIndexes(
		currentDescription, description,
	)
	if err != nil {
		return err
	}

	for _, key := range unusedIndexes {
		err = session.AlterTable(
			ctx,
			fullPath,
			ydb_options.WithDropIndex(getIndexName(key)),
		)
		if err != nil {
			return err
		}
	}

	alterColumnsOptions, err := prepareAlterColumnOptions(
		currentDescription, description, dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	if len(alterColumnsOptions) != 0 {
		err = session.AlterTable(ctx, fullPath, alterColumnsOptions...)
		if err != nil {
			return err
		}
	}

	for _, key := range addedIndexes {
		option := ydb_options.WithAddIndex(
			getIndexName(key),
			ydb_options.WithIndexType(ydb_options.GlobalIndex()),
			ydb_options.WithIndexColumns(key),
		)
		err = session.AlterTable(ctx, fullPath, option)
		if err != nil {
			return err
		}
	}

	return nil
}

func createOrAlterTable(
	ctx context.Context,
	session ydb_table.Session,
	fullPath string,
	description CreateTableDescription,
	dropUnusedColumns bool,
) error {

	currentDescription, err := session.DescribeTable(ctx, fullPath)

	switch {
	case err == nil:
		// Table exists, altering.
		err = alterTable(
			ctx,
			session,
			fullPath,
			currentDescription,
			description,
			dropUnusedColumns,
		)
		if err != nil {
			return err
		}

	case ydb.IsOperationErrorSchemeError(err):
		// Table not found, creating new.
		err = createTable(ctx, session, fullPath, description)
		if err != nil {
			return err
		}

	default:
		return err
	}

	return nil
}

func dropTable(
	ctx context.Context,
	session ydb_table.Session,
	fullPath string,
) error {

	_, err := session.DescribeTable(ctx, fullPath)

	switch {
	case err == nil:
		// Table exists, dropping.
		err = session.DropTable(ctx, fullPath)
		if err != nil {
			return fmt.Errorf("failed to drop table %v: %w", fullPath, err)
		}

	case ydb.IsOperationErrorSchemeError(err):
		// Table not found, nothing to do.
	default:
		return fmt.Errorf("failed to describe table %vi before dropping: %w", fullPath, err)
	}

	return nil
}
