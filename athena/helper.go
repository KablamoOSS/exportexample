package athena

import "strings"

// Errors for invalid S3 URLs
const (
	s3BadPrefix = constError("URL must begin with s3://")
	s3NoBucket  = constError("bucket not specified")
)

// validS3URL checks if the url is a valid S3 URL,
// returns error if invalid.
func validS3URL(s3url string) error {
	const s3Prefix = "s3://"

	if !strings.HasPrefix(strings.ToLower(s3url), s3Prefix) {
		return s3BadPrefix
	}

	if strings.ToLower(s3url) == s3Prefix {
		return s3NoBucket
	}

	return nil
}

// constError provides a way to specify constant errors: see https://dave.cheney.net/2016/04/07/constant-errors
//
// To be used effectively, you create a const package variable of type constError.
//
//   e.g. const nilValue = constError("nil value")
//
type constError string

// Error returns the string representation of constError (satisfying the error interface)
func (s constError) Error() string { return string(s) }
