package env

import (
	"fmt"
	"strings"
	"time"

	"github.com/sllt/tao/tools/taoctl/pkg/env"
	"github.com/sllt/tao/tools/taoctl/pkg/protoc"
	"github.com/sllt/tao/tools/taoctl/pkg/protocgengo"
	"github.com/sllt/tao/tools/taoctl/pkg/protocgengogrpc"
	"github.com/sllt/tao/tools/taoctl/util/console"
	"github.com/spf13/cobra"
)

type bin struct {
	name   string
	exists bool
	get    func(cacheDir string) (string, error)
}

var bins = []bin{
	{
		name:   "protoc",
		exists: protoc.Exists(),
		get:    protoc.Install,
	},
	{
		name:   "protoc-gen-go",
		exists: protocgengo.Exists(),
		get:    protocgengo.Install,
	},
	{
		name:   "protoc-gen-go-grpc",
		exists: protocgengogrpc.Exists(),
		get:    protocgengogrpc.Install,
	},
}

func check(_ *cobra.Command, _ []string) error {
	return Prepare(boolVarInstall, boolVarForce, boolVarVerbose)
}

func Prepare(install, force, verbose bool) error {
	log := console.NewColorConsole(verbose)
	pending := true
	log.Info("[taoctl-env]: preparing to check env")
	defer func() {
		if p := recover(); p != nil {
			log.Error("%+v", p)
			return
		}
		if pending {
			log.Success("\n[taoctl-env]: congratulations! your taoctl environment is ready!")
		} else {
			log.Error(`
[taoctl-env]: check env finish, some dependencies is not found in PATH, you can execute
command 'taoctl env check --install' to install it, for details, please execute command 
'taoctl env check --help'`)
		}
	}()
	for _, e := range bins {
		time.Sleep(200 * time.Millisecond)
		log.Info("")
		log.Info("[taoctl-env]: looking up %q", e.name)
		if e.exists {
			log.Success("[taoctl-env]: %q is installed", e.name)
			continue
		}
		log.Warning("[taoctl-env]: %q is not found in PATH", e.name)
		if install {
			install := func() {
				log.Info("[taoctl-env]: preparing to install %q", e.name)
				path, err := e.get(env.Get(env.TaoctlCache))
				if err != nil {
					log.Error("[taoctl-env]: an error interrupted the installation: %+v", err)
					pending = false
				} else {
					log.Success("[taoctl-env]: %q is already installed in %q", e.name, path)
				}
			}
			if force {
				install()
				continue
			}
			console.Info("[taoctl-env]: do you want to install %q [y: YES, n: No]", e.name)
			for {
				var in string
				fmt.Scanln(&in)
				var brk bool
				switch {
				case strings.EqualFold(in, "y"):
					install()
					brk = true
				case strings.EqualFold(in, "n"):
					pending = false
					console.Info("[taoctl-env]: %q installation is ignored", e.name)
					brk = true
				default:
					console.Error("[taoctl-env]: invalid input, input 'y' for yes, 'n' for no")
				}
				if brk {
					break
				}
			}
		} else {
			pending = false
		}
	}
	return nil
}
