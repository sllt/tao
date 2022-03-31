package tsgen

import (
	"errors"
	"fmt"

	"github.com/logrusorgru/aurora"
	"github.com/urfave/cli"
	"manlu.org/tao/core/logx"
	"manlu.org/tao/tools/taoctl/api/parser"
	"manlu.org/tao/tools/taoctl/util/pathx"
)

// TsCommand provides the entry to generate typescript codes
func TsCommand(c *cli.Context) error {
	apiFile := c.String("api")
	dir := c.String("dir")
	webAPI := c.String("webapi")
	caller := c.String("caller")
	unwrapAPI := c.Bool("unwrap")
	if len(apiFile) == 0 {
		return errors.New("missing -api")
	}

	if len(dir) == 0 {
		return errors.New("missing -dir")
	}

	api, err := parser.Parse(apiFile)
	if err != nil {
		fmt.Println(aurora.Red("Failed"))
		return err
	}

	api.Service = api.Service.JoinPrefix()
	logx.Must(pathx.MkdirIfNotExist(dir))
	logx.Must(genHandler(dir, webAPI, caller, api, unwrapAPI))
	logx.Must(genComponents(dir, api))

	fmt.Println(aurora.Green("Done."))
	return nil
}
