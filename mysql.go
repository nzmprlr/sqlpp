package sqlpp

import (
	"strings"
)

var (
	mysqlErrPrefixPrepareNotSupported = "Error 1295:"
)

func isMysqlPrepareNotSupported(err error) bool {
	return strings.HasPrefix(err.Error(), mysqlErrPrefixPrepareNotSupported)
}
