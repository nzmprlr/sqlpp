package sqlpp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_isMysqlPrepareNotSupported(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{
			nil,
			false,
		},
		{
			errors.New(""),
			false,
		},
		{
			errors.New("Error 1295: This command is not supported in the prepared statement protocol yet"),
			true,
		},
	}

	t.Parallel()
	for _, c := range cases {
		t.Run(fmt.Sprintf("%s", c.err), func(t *testing.T) {
			assert.Equal(t, c.want, isMysqlPrepareNotSupported(c.err))
		})
	}
}
