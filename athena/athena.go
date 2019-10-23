// Package athena is a shim over the AWS SDK athena API to submit
// a query and receive results.
//
// The only purpose this exists is because the AWS SDK Go is overly complicated,
// unidiomatic, and headache inducing to do very basic tasks. Also crap documentation,
// few to no examples, and doesn't provide "basic" or "simple" clients.
//
// All the possible query options/toggles are not in scope, e.g. pagination.
// You should probably only use this for quick once off jobs, e.g. previewing data.
// If you require further functionality, it is advisable to use the AWS SDK API instead.
//
// Since Athena queries are asynchronous in nature with no signalling mechanism
// to let you know when a query is complete, you will need to poll. This library
// makes this convenient by avoiding some of the boilerplate.
//
// While we can manage the lifecycle of the query and allow polling its status,
// we make it the responsibility of the caller to do the polling.
//
// See the 'cli' directory which contains an example program using the library,
// and demonstrates how to poll for query completion and fetch results.
package athena

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

const nilSession = constError("session is nil")
const emptyDatabase = constError("database must not be an empty string")
const emptyQuery = constError("query must not be an empty string")
const nilQueryExecution = constError("QueryExecution is nil")
const nilQueryExecutionStatus = constError("Status is nil")
const nilQueryExecutionStatusState = constError("State is nil")
const nilQueryExecutionResultConfiguration = constError("ResultConfiguration is nil")
const nilQueryExecutionResultConfigurationOutputLocation = constError("OutputLocation is nil")

// Client is used to query AWS Athena.
type Client struct {
	// api is either an athena.Athena or a mock implementation for testing.
	api athenaiface.AthenaAPI
}

// NewClient creates and returns a new Athena client.
//
// Caller must provide a valid AWS session.
//
// This can be used for concurrent requests as the underlying Athena
// supports concurrent queries.
func NewClient(session *session.Session) (Client, error) {
	if session == nil {
		return Client{}, nilSession
	}

	return Client{athena.New(session)}, nil
}

// Query allows checking for query status and completion
// and fetching results.
type Query struct {
	id string
	Client
}

// Row is a just a slice of string data; to reconcile the types
// you will need to refer to the corresponding column data.
type Row []string

// Result contains row data and associated column information.
type Result struct {
	Columns []Column `json:"columns"`
	Rows    []Row    `json:"rows"`
}

// Column specifies the various properties of the column.
type Column struct {
	// The name of the column.
	Name string `json:"name"`

	// Indicates whether values in the column are case-sensitive.
	CaseSensitive bool `json:"case_sensitive"`

	// Indicates whether CaseSensitive is provided.
	CaseSensitiveExists bool `json:"case_sensitive_exists"`

	// The catalog to which the query results belong.
	CatalogName string `json:"catalog_name"`

	// Indicates whether CatalogName is provided.
	CatalogNameExists bool `json:"catalog_name_exists"`

	// A column label.
	Label string `json:"label"`

	// Indicates whether Label is provided.
	LabelExists bool `json:"label_exists"`

	// Indicates the column's nullable status.
	// Takes on the following values: NOT_NULL | NULLABLE | UNKNOWN
	Nullable string `json:"nullable"`

	// Indicates whether Nullable is provided.
	NullableExists bool `json:"nullable_exists"`

	// Precision specifies the total number of digits, up to 38. For performance
	// reasons, we recommend restricting up to 18 digits.
	Precision int `json:"precision"`

	// Indicates whether Precision is provided.
	PrecisionExists bool `json:"precision_exists"`

	// Scale is for DECIMAL data types, and specifies the number of digits in the
	// fractional part of the value. Defaults to 0.
	Scale int `json:"scale"`

	// Indicates whether Scale is provided.
	ScaleExists bool `json:"scale_exists"`

	// The schema name (database name) to which the query results belong.
	SchemaName string `json:"schema_name"`

	// Indicates whether SchemaName is provided.
	SchemaNameExists bool `json:"schema_name_exists"`

	// The table name for the query results.
	TableName string `json:"table_name"`

	// Indicates whether SchemaName is provided.
	TableNameExists bool `json:"table_name_exists"`
}

// makeQuery is a helper function to wrap AWS crap
func makeQuery(database, query, output string) *athena.StartQueryExecutionInput {
	var sqeInput athena.StartQueryExecutionInput
	var qec athena.QueryExecutionContext
	var rc athena.ResultConfiguration
	rc.SetOutputLocation(output)
	qec.SetDatabase(database)
	sqeInput.SetQueryString(query).SetQueryExecutionContext(&qec).SetResultConfiguration(&rc)

	return &sqeInput
}

// DoQuery starts a query on a database in Athena. Output is an S3 URL
// specifying bucket and optionally a (folder) key where Athena can
// store CSV results.
//
// A Query is returned which can be used to check the status and retrieve
// results of the query.
//
// An error is returned if the query couldn't be performed.
func (c Client) DoQuery(database, query, output string) (Query, error) {
	if database == "" {
		return Query{}, emptyDatabase
	}

	if query == "" {
		return Query{}, emptyQuery
	}

	err := validS3URL(output)
	if err != nil {
		return Query{}, err
	}

	in := makeQuery(database, query, output)
	out, err := c.api.StartQueryExecution(in)

	if err != nil {
		return Query{}, err
	}

	return Query{id: *out.QueryExecutionId, Client: c}, nil
}

// Result fetches and returns column and row data from athena.
//
// It is advisable the caller calls Ready() until it returns
// the job has completed (i.e. polling).
//
// Note: Athena pads an initial row containing column names;
// the caller can remove it as required.
func (q Query) Result() (Result, error) {
	r := Result{}

	in := &athena.GetQueryResultsInput{QueryExecutionId: &q.id}
	out, err := q.api.GetQueryResults(in)

	if err != nil {
		return Result{}, err
	}

	r.Columns = columns(out.ResultSet.ResultSetMetadata.ColumnInfo)
	r.Rows = rows(out.ResultSet.Rows)

	return r, nil
}

func columns(columnInfo []*athena.ColumnInfo) []Column {
	columns := make([]Column, 0, len(columnInfo))

	for _, ci := range columnInfo {
		c := Column{Name: *ci.Name}

		if ci.CaseSensitive != nil {
			c.CaseSensitive = *ci.CaseSensitive
			c.CaseSensitiveExists = true
		}

		if ci.CatalogName != nil {
			c.CatalogName = *ci.CatalogName
			c.CatalogNameExists = true
		}

		if ci.Label != nil {
			c.Label = *ci.Label
			c.LabelExists = true
		}

		if ci.Nullable != nil {
			c.Nullable = *ci.Nullable
			c.NullableExists = true
		}

		if ci.Precision != nil {
			c.Precision = int(*ci.Precision)
			c.PrecisionExists = true
		}

		if ci.Scale != nil {
			c.Scale = int(*ci.Scale)
			c.ScaleExists = true
		}
		if ci.SchemaName != nil {
			c.SchemaName = *ci.SchemaName
			c.SchemaNameExists = true
		}

		if ci.TableName != nil {
			c.TableName = *ci.TableName
			c.TableNameExists = true
		}

		columns = append(columns, c)
	}

	return columns
}

func rows(rowData []*athena.Row) []Row {
	rows := make([]Row, 0, len(rowData))
	for _, r := range rowData {
		row := make(Row, 0, len(r.Data))
		for _, d := range r.Data {
			var s string
			if d.VarCharValue != nil {
				s = *d.VarCharValue
			}
			row = append(row, s)
		}

		rows = append(rows, row)
	}

	return rows
}

// QueryStatus returns the state of the current query
type QueryStatus struct {
	State          string
	OutputLocation string
}

// Done returns true if the query has completed successfully.
func (qs QueryStatus) Done() bool {
	return qs.State == "SUCCEEDED"
}

// Status returns true if and only if the query has finished and there is no error.
// The location specifies (as an S3 URL) where Athena wrote the results of the
// query.
func (q Query) Status() (QueryStatus, error) {
	in := &athena.GetQueryExecutionInput{QueryExecutionId: &q.id}
	qe, err := q.api.GetQueryExecution(in)

	if err != nil {
		return QueryStatus{}, err
	}

	// all of this stuff appears to be optional, so probably best to armour everything with checks
	{
		if qe.QueryExecution == nil {
			return QueryStatus{}, nilQueryExecution
		}

		if qe.QueryExecution.Status == nil {
			return QueryStatus{}, nilQueryExecutionStatus
		}

		if qe.QueryExecution.Status.State == nil {
			return QueryStatus{}, nilQueryExecutionStatusState
		}

		if qe.QueryExecution.ResultConfiguration == nil {
			return QueryStatus{}, nilQueryExecutionResultConfiguration
		}

		if qe.QueryExecution.ResultConfiguration.OutputLocation == nil {
			return QueryStatus{}, nilQueryExecutionResultConfigurationOutputLocation
		}
	}

	status := QueryStatus{
		State:          *qe.QueryExecution.Status.State,
		OutputLocation: *qe.QueryExecution.ResultConfiguration.OutputLocation,
	}

	return status, nil
}

// ID is the associated Athena query job execution ID
func (q Query) ID() string {
	return q.id
}
