package ctx

import (
	"go/build"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"manlu.org/tao/core/stringx"
	"manlu.org/tao/tools/taoctl/rpc/execx"
	"manlu.org/tao/tools/taoctl/util"
)

func TestIsGoMod(t *testing.T) {
	// create mod project
	dft := build.Default
	gp := dft.GOPATH
	if len(gp) == 0 {
		return
	}
	projectName := stringx.Rand()
	dir := filepath.Join(gp, "src", projectName)
	err := util.MkdirIfNotExist(dir)
	if err != nil {
		return
	}

	_, err = execx.Run("go mod init "+projectName, dir)
	assert.Nil(t, err)
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	isGoMod, err := IsGoMod(dir)
	assert.Nil(t, err)
	assert.Equal(t, true, isGoMod)
}

func TestIsGoModNot(t *testing.T) {
	dft := build.Default
	gp := dft.GOPATH
	if len(gp) == 0 {
		return
	}
	projectName := stringx.Rand()
	dir := filepath.Join(gp, "src", projectName)
	err := util.MkdirIfNotExist(dir)
	if err != nil {
		return
	}

	defer func() {
		_ = os.RemoveAll(dir)
	}()

	isGoMod, err := IsGoMod(dir)
	assert.Nil(t, err)
	assert.Equal(t, false, isGoMod)
}

func TestIsGoModWorkDirIsNil(t *testing.T) {
	_, err := IsGoMod("")
	assert.Equal(t, err.Error(), func() string {
		return "the work directory is not found"
	}())
}