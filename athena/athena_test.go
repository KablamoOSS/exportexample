package athena_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KablamoOSS/exportexample/athena"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	aa "github.com/aws/aws-sdk-go/service/athena"
)

func TestNewClient(t *testing.T) {
	t.Run("happy path", func(tt *testing.T) {
		sess, err := session.NewSession()

		if err != nil {
			t.Errorf("err == %v (want nil)", err)
		}

		_, err = athena.NewClient(sess)

		if err != nil {
			t.Errorf("err == %v (want nil)", err)
		}

	})

	t.Run("nil session", func(tt *testing.T) {
		_, err := athena.NewClient(nil)

		if err != athena.ErrNilSession {
			t.Errorf("err == %v (want %v)", err, athena.ErrNilSession)
		}
	})
}

func TestDoQuery(t *testing.T) {
	inputHandling := []struct {
		id       string
		database string
		query    string
		output   string
		expected athena.Query
		err      error
	}{
		{
			id:       "invalid input: empty database",
			database: "",
			query:    "query",
			output:   "s3://output",
			expected: athena.Query{},
			err:      athena.ErrEmptyDatabase,
		},
		{
			id:       "invalid input: empty query",
			database: "database",
			query:    "",
			output:   "s3://output",
			expected: athena.Query{},
			err:      athena.ErrEmptyQuery,
		},
		{
			id:       "invalid input: output doesn't start with s3://",
			database: "database",
			query:    "query",
			output:   "http://output",
			expected: athena.Query{},
			err:      athena.ErrS3BadPrefix,
		},
		{
			id:       "invalid input: output doesn't specify bucket",
			database: "database",
			query:    "query",
			output:   "s3://",
			expected: athena.Query{},
			err:      athena.ErrS3NoBucket,
		},
	}

	for _, tc := range inputHandling {
		t.Run(tc.id, func(tt *testing.T) {
			c := athena.NewCustomClient(mockClient{})

			q, err := c.DoQuery(tc.database, tc.query, tc.output)

			if q != tc.expected {
				tt.Errorf("Query == %v (want %v)", q, tc.expected)
			}

			if err != tc.err {
				tt.Errorf("err == %v (want %v)", err, tc.err)
			}
		})
	}

	t.Run("unhappy path", func(tt *testing.T) {
		expectedQuery := athena.Query{}
		expectedErr := errors.New("StartQueryExecution error")

		cfg := startQueryExecution{err: expectedErr}
		mc := mockClient{startQueryExecution: cfg}

		c := athena.NewCustomClient(mc)

		q, err := c.DoQuery("database", "query", "s3://output")

		if !reflect.DeepEqual(q, expectedQuery) {
			tt.Errorf("Query == %v (want %v)", q, expectedQuery)
		}

		if err != expectedErr {
			tt.Errorf("err == %v (want %v)", err, expectedErr)
		}
	})

	t.Run("happy path", func(tt *testing.T) {
		const id = "jobid"

		cfg := startQueryExecution{id: id}
		mc := mockClient{startQueryExecution: cfg}

		c := athena.NewCustomClient(mc)

		expectedQuery := c.CreateQuery(id)

		q, err := c.DoQuery("database", "query", "s3://output")

		if !reflect.DeepEqual(q, expectedQuery) {
			tt.Errorf("Query == %v (want %v)", q, expectedQuery)
		}

		if err != nil {
			tt.Errorf("err == %v (want nil)", err)
		}
	})
}

func TestNRows(t *testing.T) {
	cases := []struct {
		id       string
		table    string
		limit    int
		expected string
		err      error
	}{
		{
			id:       "happy path",
			table:    "table",
			limit:    10,
			expected: "SELECT * FROM table LIMIT 10",
			err:      nil,
		},
		{
			id:       "invalid input: non-negative limit",
			table:    "table",
			limit:    -1,
			expected: "",
			err:      athena.ErrInvalidLimit,
		},
		{
			id:       "invalid input: empty table",
			table:    "",
			limit:    0,
			expected: "",
			err:      athena.ErrEmptyTable,
		},
	}

	for _, tc := range cases {
		t.Run(tc.id, func(tt *testing.T) {
			q, err := athena.NRows(tc.table, tc.limit)

			if q != tc.expected {
				tt.Errorf("Query == %v (want %v)", q, tc.expected)
			}

			if err != tc.err {
				tt.Errorf("err == %v (want %v)", err, tc.err)
			}
		})
	}
}

func TestQueryResult(t *testing.T) {
	var errFailure = errors.New("GetQueryResults failure")

	row := func(v ...string) *aa.Row {
		r := aa.Row{Data: make([]*aa.Datum, len(v))}
		for i := range v {
			datum := aa.Datum{VarCharValue: &v[i]}
			r.Data[i] = &datum
		}

		return &r
	}

	colinfo := func(ci aa.ColumnInfo) *aa.ColumnInfo {
		return &ci
	}

	cases := []struct {
		id          string
		cfg         getQueryResults
		expected    athena.Result
		expectedErr error
	}{
		{
			id: "happy path",
			cfg: getQueryResults{
				columns: []*aa.ColumnInfo{
					colinfo(aa.ColumnInfo{Name: aws.String("first")}),
					colinfo(aa.ColumnInfo{Name: aws.String("second")}),
					colinfo(aa.ColumnInfo{Name: aws.String("third")}),
				},
				rows: []*aa.Row{
					row("some", "data", "here"),
				},
			},
			expected: athena.Result{
				Columns: []athena.Column{
					{Name: "first"},
					{Name: "second"},
					{Name: "third"},
				},
				Rows: []athena.Row{
					athena.Row([]string{"first", "second", "third"}),
					athena.Row([]string{"some", "data", "here"}),
				},
			},
			expectedErr: nil,
		},
		{
			id: "row with nil data",
			cfg: getQueryResults{
				columns: []*aa.ColumnInfo{
					colinfo(aa.ColumnInfo{Name: aws.String("first")}),
					colinfo(aa.ColumnInfo{Name: aws.String("second")}),
					colinfo(aa.ColumnInfo{Name: aws.String("third")}),
				},
				rows: []*aa.Row{
					{
						Data: []*aa.Datum{
							{VarCharValue: aws.String("some")},
							{VarCharValue: nil},
							{VarCharValue: aws.String("here")},
						},
					},
				},
			},
			expected: athena.Result{
				Columns: []athena.Column{
					{Name: "first"},
					{Name: "second"},
					{Name: "third"},
				},
				Rows: []athena.Row{
					athena.Row([]string{"first", "second", "third"}),
					athena.Row([]string{"some", "", "here"}),
				},
			},
			expectedErr: nil,
		},
		{
			id: "unhappy path",
			cfg: getQueryResults{
				columns: []*aa.ColumnInfo{
					colinfo(aa.ColumnInfo{Name: aws.String("first")}),
					colinfo(aa.ColumnInfo{Name: aws.String("second")}),
					colinfo(aa.ColumnInfo{Name: aws.String("third")}),
				},
				rows: []*aa.Row{
					row("some", "data", "here"),
				},
				err: errFailure,
			},
			expected:    athena.Result{},
			expectedErr: errFailure,
		},
		{
			id: "christmas tree column",
			cfg: getQueryResults{
				columns: []*aa.ColumnInfo{
					{
						Name:          aws.String("christmas"),
						CaseSensitive: aws.Bool(true),
						CatalogName:   aws.String("catalog"),
						Label:         aws.String("label"),
						Nullable:      aws.String("nullable"),
						Precision:     aws.Int64(1),
						Scale:         aws.Int64(2),
						SchemaName:    aws.String("schema"),
						TableName:     aws.String("table"),
					},
				},
			},
			expected: athena.Result{
				Columns: []athena.Column{
					{
						Name:                "christmas",
						CaseSensitive:       true,
						CaseSensitiveExists: true,
						CatalogName:         "catalog",
						CatalogNameExists:   true,
						Label:               "label",
						LabelExists:         true,
						Nullable:            "nullable",
						NullableExists:      true,
						Precision:           1,
						PrecisionExists:     true,
						Scale:               2,
						ScaleExists:         true,
						SchemaName:          "schema",
						SchemaNameExists:    true,
						TableName:           "table",
						TableNameExists:     true,
					},
				},
				Rows: []athena.Row{
					athena.Row([]string{"christmas"}),
				},
			},
			expectedErr: nil,
		},
	}

	equalRow := func(a, b athena.Row) bool {
		if len(a) != len(b) {
			return false
		}

		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}

		return true
	}

	for _, tc := range cases {
		t.Run(tc.id, func(tt *testing.T) {
			c := athena.NewCustomClient(mockClient{getQueryResults: tc.cfg})

			q := c.CreateQuery("")

			actual, err := q.Result()

			if len(actual.Rows) == len(tc.expected.Rows) {
				for i := range actual.Rows {
					if !equalRow(actual.Rows[i], tc.expected.Rows[i]) {
						tt.Errorf("Result.Rows[%d] == %v (want %v)", i, actual.Rows[i], tc.expected.Rows[i])
					}
				}
			} else {
				tt.Errorf("len(Result.Rows) == %d (want %d)", len(actual.Rows), len(tc.expected.Rows))
			}

			if len(actual.Columns) == len(tc.expected.Columns) {
				for i := range actual.Columns {
					if actual.Columns[i] != tc.expected.Columns[i] {
						tt.Errorf("Result.Columns[%d] == %v (want %v)", i, actual.Columns[i], tc.expected.Columns[i])
					}
				}
			} else {
				tt.Errorf("len(Result.Columns) == %d (want %d)", len(actual.Columns), len(tc.expected.Columns))
			}

			if err != tc.expectedErr {
				tt.Errorf("err == %v (want %v)", err, tc.expectedErr)
			}
		})
	}
}

func TestQueryReady(t *testing.T) {
	var errFailure = errors.New("GetQueryExecution failure")

	cases := []struct {
		id          string
		cfg         getQueryExecution
		expected    athena.QueryStatus
		expectedErr error
	}{
		{
			id:          "unhappy path",
			cfg:         getQueryExecution{"doesntmatter", "ignore", errFailure},
			expected:    athena.QueryStatus{},
			expectedErr: errFailure,
		},
		{
			id:          "query still in progress",
			cfg:         getQueryExecution{"INPROGRESS", "dummyloc", nil},
			expected:    athena.QueryStatus{"INPROGRESS", "dummyloc"},
			expectedErr: nil,
		},
		{
			id:       "query succeeds",
			cfg:      getQueryExecution{"SUCCEEDED", "s3://finished/file.csv", nil},
			expected: athena.QueryStatus{"SUCCEEDED", "s3://finished/file.csv"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.id, func(tt *testing.T) {
			c := athena.NewCustomClient(mockClient{getQueryExecution: tc.cfg})
			q := c.CreateQuery("")

			actual, err := q.Status()

			if actual != tc.expected {
				tt.Errorf("Status() == %v (want %v)", actual, tc.expected)
			}

			if err != tc.expectedErr {
				tt.Errorf("err == %v, (want %v)", err, tc.expectedErr)
			}
		})
	}
}

func TestQueryStatusDone(t *testing.T) {
	cases := []struct {
		id       string
		status   athena.QueryStatus
		expected bool
	}{
		{
			id:       "done",
			status:   athena.QueryStatus{State: "SUCCEEDED"},
			expected: true,
		},
		{
			id:       "not done",
			status:   athena.QueryStatus{State: "NOT DONE"},
			expected: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.id, func(tt *testing.T) {
			actual := tc.status.Done()

			if actual != tc.expected {
				tt.Errorf("Done() == %t (want %t)", actual, tc.expected)
			}
		})
	}
}

func TestQueryID(t *testing.T) {
	const id = "jobid"
	c := athena.NewCustomClient(mockClient{})
	q := c.CreateQuery(id)

	got := q.ID()
	if got != id {
		t.Errorf("q.ID() == %v (want %v)", got, id)
	}
}
