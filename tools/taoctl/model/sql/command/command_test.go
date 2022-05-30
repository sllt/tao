package command

import (
	_ "embed"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"manlu.org/tao/tools/taoctl/config"
	"manlu.org/tao/tools/taoctl/model/sql/gen"
	"manlu.org/tao/tools/taoctl/util/pathx"
)

var (
	//go:embed testdata/user.sql
	sql string
	cfg = &config.Config{
		NamingFormat: "gotao",
	}
)

func TestFromDDl(t *testing.T) {
	err := gen.Clean()
	assert.Nil(t, err)

	err = fromDDL("./user.sql", pathx.MustTempDir(), cfg, true, false, "go_tao")
	assert.Equal(t, errNotMatched, err)

	// case dir is not exists
	unknownDir := filepath.Join(pathx.MustTempDir(), "test", "user.sql")
	err = fromDDL(unknownDir, pathx.MustTempDir(), cfg, true, false, "go_tao")
	assert.True(t, func() bool {
		switch err.(type) {
		case *os.PathError:
			return true
		default:
			return false
		}
	}())

	// case empty src
	err = fromDDL("", pathx.MustTempDir(), cfg, true, false, "go_tao")
	if err != nil {
		assert.Equal(t, "expected path or path globbing patterns, but nothing found", err.Error())
	}

	tempDir := filepath.Join(pathx.MustTempDir(), "test")
	err = pathx.MkdirIfNotExist(tempDir)
	if err != nil {
		return
	}

	user1Sql := filepath.Join(tempDir, "user1.sql")
	user2Sql := filepath.Join(tempDir, "user2.sql")

	err = ioutil.WriteFile(user1Sql, []byte(sql), os.ModePerm)
	if err != nil {
		return
	}

	err = ioutil.WriteFile(user2Sql, []byte(sql), os.ModePerm)
	if err != nil {
		return
	}

	_, err = os.Stat(user1Sql)
	assert.Nil(t, err)

	_, err = os.Stat(user2Sql)
	assert.Nil(t, err)

	filename := filepath.Join(tempDir, "usermodel.go")
	fromDDL := func(db string) {
		err = fromDDL(filepath.Join(tempDir, "user*.sql"), tempDir, cfg, true, false, db)
		assert.Nil(t, err)

		_, err = os.Stat(filename)
		assert.Nil(t, err)
	}

	fromDDL("go_tao")
	_ = os.Remove(filename)
	fromDDL("go-zero")
	_ = os.Remove(filename)
	fromDDL("1gotao")
}

func Test_parseTableList(t *testing.T) {
	testData := []string{"foo", "b*", "bar", "back_up", "foo,bar,b*"}
	patterns := parseTableList(testData)
	actual := patterns.list()
	expected := []string{"foo", "b*", "bar", "back_up"}
	sort.Slice(actual, func(i, j int) bool {
		return actual[i] > actual[j]
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i] > expected[j]
	})
	assert.Equal(t, strings.Join(expected, ","), strings.Join(actual, ","))

	matchTestData := map[string]bool{
		"foo":     true,
		"bar":     true,
		"back_up": true,
		"bit":     true,
		"ab":      false,
		"b":       true,
	}
	for v, expected := range matchTestData {
		actual := patterns.Match(v)
		assert.Equal(t, expected, actual)
	}
}
