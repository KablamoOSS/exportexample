package athena_test

import (
	aa "github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

type startQueryExecution struct {
	id  string
	err error
}

type getQueryExecution struct {
	state       string
	outLocation string
	err         error
}

type getQueryResults struct {
	columns []*aa.ColumnInfo
	rows    []*aa.Row

	err error
}

type mockClient struct {
	startQueryExecution
	getQueryExecution
	getQueryResults

	athenaiface.AthenaAPI
}

func (mc mockClient) StartQueryExecution(in *aa.StartQueryExecutionInput) (*aa.StartQueryExecutionOutput, error) {
	id := mc.startQueryExecution.id
	out := (&aa.StartQueryExecutionOutput{}).SetQueryExecutionId(id)
	return out, mc.startQueryExecution.err
}

func (mc mockClient) GetQueryExecution(in *aa.GetQueryExecutionInput) (*aa.GetQueryExecutionOutput, error) {
	s := (&aa.QueryExecutionStatus{}).SetState(mc.getQueryExecution.state)
	rc := (&aa.ResultConfiguration{}).SetOutputLocation(mc.getQueryExecution.outLocation)
	qe := (&aa.QueryExecution{}).SetStatus(s).SetResultConfiguration(rc)
	out := (&aa.GetQueryExecutionOutput{}).SetQueryExecution(qe)
	return out, mc.getQueryExecution.err
}

func (mc mockClient) GetQueryResults(in *aa.GetQueryResultsInput) (*aa.GetQueryResultsOutput, error) {

	// athena inserts a leading row containing column names
	headerRow := aa.Row{Data: make([]*aa.Datum, len(mc.getQueryResults.columns))}
	for i, c := range mc.getQueryResults.columns {
		datum := aa.Datum{VarCharValue: c.Name}
		headerRow.Data[i] = &datum
	}
	rows := append([]*aa.Row{&headerRow}, mc.getQueryResults.rows...)
	rsm := (&aa.ResultSetMetadata{}).SetColumnInfo(mc.getQueryResults.columns)
	rs := (&aa.ResultSet{}).SetRows(rows).SetResultSetMetadata(rsm)
	out := (&aa.GetQueryResultsOutput{}).SetResultSet(rs)
	return out, mc.getQueryResults.err
}
