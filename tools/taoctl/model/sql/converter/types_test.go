package converter

import (
	"testing"

	"github.com/sllt/tao/tools/taoctl/pkg/ddl/parser"
	"github.com/stretchr/testify/assert"
)

func TestConvertDataType(t *testing.T) {
	v, err := ConvertDataType(parser.TinyInt, false, false, true)
	assert.Nil(t, err)
	assert.Equal(t, "int64", v)

	v, err = ConvertDataType(parser.TinyInt, false, true, true)
	assert.Nil(t, err)
	assert.Equal(t, "uint64", v)

	v, err = ConvertDataType(parser.TinyInt, true, false, true)
	assert.Nil(t, err)
	assert.Equal(t, "sql.NullInt64", v)

	v, err = ConvertDataType(parser.Timestamp, false, false, true)
	assert.Nil(t, err)
	assert.Equal(t, "time.Time", v)

	v, err = ConvertDataType(parser.Timestamp, true, false, true)
	assert.Nil(t, err)
	assert.Equal(t, "sql.NullTime", v)
}
