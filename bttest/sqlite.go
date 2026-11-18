package bttest

import (
	"errors"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// isConstraintErr reports whether err is a SQLITE_CONSTRAINT error.
// modernc.org/sqlite reports extended result codes (e.g.
// SQLITE_CONSTRAINT_PRIMARYKEY), so compare only the primary code in the low byte.
func isConstraintErr(err error) bool {
	var e *sqlite.Error
	return errors.As(err, &e) && e.Code()&0xff == sqlite3.SQLITE_CONSTRAINT
}
