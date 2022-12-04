package new

import (
	_ "embed"
	"errors"
	"html/template"
	"os"
	"path/filepath"
	"strings"

	"github.com/sllt/tao/tools/taoctl/api/gogen"
	conf "github.com/sllt/tao/tools/taoctl/config"
	"github.com/sllt/tao/tools/taoctl/util"
	"github.com/sllt/tao/tools/taoctl/util/pathx"
)

//go:embed api.tpl
var apiTemplate string

var (
	// VarStringHome describes the taoctl home.
	VarStringHome string
	// VarStringRemote describes the remote git repository.
	VarStringRemote string
	// VarStringBranch describes the git branch.
	VarStringBranch string
	// VarStringStyle describes the style of output files.
	VarStringStyle string
)

// CreateServiceCommand fast create service
func CreateServiceCommand(args []string) error {
	dirName := args[0]
	if len(VarStringStyle) == 0 {
		VarStringStyle = conf.DefaultFormat
	}
	if strings.Contains(dirName, "-") {
		return errors.New("api new command service name not support strikethrough, because this will used by function name")
	}

	abs, err := filepath.Abs(dirName)
	if err != nil {
		return err
	}

	err = pathx.MkdirIfNotExist(abs)
	if err != nil {
		return err
	}

	dirName = filepath.Base(filepath.Clean(abs))
	filename := dirName + ".api"
	apiFilePath := filepath.Join(abs, filename)
	fp, err := os.Create(apiFilePath)
	if err != nil {
		return err
	}

	defer fp.Close()

	if len(VarStringRemote) > 0 {
		repo, _ := util.CloneIntoGitHome(VarStringRemote, VarStringBranch)
		if len(repo) > 0 {
			VarStringHome = repo
		}
	}

	if len(VarStringHome) > 0 {
		pathx.RegisterTaoctlHome(VarStringHome)
	}

	text, err := pathx.LoadTemplate(category, apiTemplateFile, apiTemplate)
	if err != nil {
		return err
	}

	t := template.Must(template.New("template").Parse(text))
	if err := t.Execute(fp, map[string]string{
		"name":    dirName,
		"handler": strings.Title(dirName),
	}); err != nil {
		return err
	}

	err = gogen.DoGenProject(apiFilePath, abs, VarStringStyle)
	return err
}
