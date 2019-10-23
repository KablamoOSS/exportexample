package athena

import (
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
)

// Exports helpful constants and functions for use in testing.

const ErrNilSession = nilSession
const ErrEmptyDatabase = emptyDatabase
const ErrEmptyTable = emptyTable
const ErrEmptyQuery = emptyQuery
const ErrInvalidLimit = invalidLimit
const ErrS3BadPrefix = s3BadPrefix
const ErrS3NoBucket = s3NoBucket

// NewCustomClient creates and returns a custom Athena client.
//
// A mock implementation of the Athena Interface can be provided for testing.
func NewCustomClient(api athenaiface.AthenaAPI) Client {
	return Client{api}
}

func (c Client) CreateQuery(id string) Query {
	return Query{id, c}
}

func CreateConstError(msg string) error {
	return constError(msg)
}
