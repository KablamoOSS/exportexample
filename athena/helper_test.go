package athena_test

import (
	"testing"

	"github.com/KablamoOSS/exportexample/athena"
)

// Wanky test for full coverage.
func TestConstError(t *testing.T) {
	msg := "test"

	err := athena.CreateConstError(msg)

	if err.Error() != msg {
		t.Errorf("err.Error() == %v (want %v)", err.Error(), msg)
	}
}
