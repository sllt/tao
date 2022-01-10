package generator

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/emicklei/proto"
	"manlu.org/tao/core/collection"
	conf "manlu.org/tao/tools/taoctl/config"
	"manlu.org/tao/tools/taoctl/rpc/parser"
	"manlu.org/tao/tools/taoctl/util"
	"manlu.org/tao/tools/taoctl/util/format"
	"manlu.org/tao/tools/taoctl/util/pathx"
	"manlu.org/tao/tools/taoctl/util/stringx"
)

const (
	callTemplateText = `{{.head}}

package {{.filePackage}}

import (
	"context"

	{{.package}}

	"manlu.org/tao/zrpc"
	"google.golang.org/grpc"
)

type (
	{{.alias}}

	{{.serviceName}} interface {
		{{.interface}}
	}

	default{{.serviceName}} struct {
		cli zrpc.Client
	}
)

func New{{.serviceName}}(cli zrpc.Client) {{.serviceName}} {
	return &default{{.serviceName}}{
		cli: cli,
	}
}

{{.functions}}
`

	callInterfaceFunctionTemplate = `{{if .hasComment}}{{.comment}}
{{end}}{{.method}}(ctx context.Context{{if .hasReq}}, in *{{.pbRequest}}{{end}}, opts ...grpc.CallOption) ({{if .notStream}}*{{.pbResponse}}, {{else}}{{.streamBody}},{{end}} error)`

	callFunctionTemplate = `
{{if .hasComment}}{{.comment}}{{end}}
func (m *default{{.serviceName}}) {{.method}}(ctx context.Context{{if .hasReq}}, in *{{.pbRequest}}{{end}}, opts ...grpc.CallOption) ({{if .notStream}}*{{.pbResponse}}, {{else}}{{.streamBody}},{{end}} error) {
	client := {{.package}}.New{{.rpcServiceName}}Client(m.cli.Conn())
	return client.{{.method}}(ctx{{if .hasReq}}, in{{end}}, opts...)
}
`
)

// GenCall generates the rpc client code, which is the entry point for the rpc service call.
// It is a layer of encapsulation for the rpc client and shields the details in the pb.
func (g *DefaultGenerator) GenCall(ctx DirContext, proto parser.Proto, cfg *conf.Config) error {
	dir := ctx.GetCall()
	service := proto.Service
	head := util.GetHead(proto.Name)

	callFilename, err := format.FileNamingFormat(cfg.NamingFormat, service.Name)
	if err != nil {
		return err
	}

	filename := filepath.Join(dir.Filename, fmt.Sprintf("%s.go", callFilename))
	functions, err := g.genFunction(proto.PbPackage, service)
	if err != nil {
		return err
	}

	iFunctions, err := g.getInterfaceFuncs(proto.PbPackage, service)
	if err != nil {
		return err
	}

	text, err := pathx.LoadTemplate(category, callTemplateFile, callTemplateText)
	if err != nil {
		return err
	}

	alias := collection.NewSet()
	for _, item := range proto.Message {
		msgName := getMessageName(*item.Message)
		alias.AddStr(fmt.Sprintf("%s = %s", parser.CamelCase(msgName), fmt.Sprintf("%s.%s", proto.PbPackage, parser.CamelCase(msgName))))
	}

	aliasKeys := alias.KeysStr()
	sort.Strings(aliasKeys)
	err = util.With("shared").GoFmt(true).Parse(text).SaveTo(map[string]interface{}{
		"name":        callFilename,
		"alias":       strings.Join(aliasKeys, pathx.NL),
		"head":        head,
		"filePackage": dir.Base,
		"package":     fmt.Sprintf(`"%s"`, ctx.GetPb().Package),
		"serviceName": stringx.From(service.Name).ToCamel(),
		"functions":   strings.Join(functions, pathx.NL),
		"interface":   strings.Join(iFunctions, pathx.NL),
	}, filename, true)
	return err
}

func getMessageName(msg proto.Message) string {
	list := []string{msg.Name}

	for {
		parent := msg.Parent
		if parent == nil {
			break
		}

		parentMsg, ok := parent.(*proto.Message)
		if !ok {
			break
		}

		tmp := []string{parentMsg.Name}
		list = append(tmp, list...)
		msg = *parentMsg
	}

	return strings.Join(list, "_")
}

func (g *DefaultGenerator) genFunction(goPackage string, service parser.Service) ([]string, error) {
	functions := make([]string, 0)

	for _, rpc := range service.RPC {
		text, err := pathx.LoadTemplate(category, callFunctionTemplateFile, callFunctionTemplate)
		if err != nil {
			return nil, err
		}

		comment := parser.GetComment(rpc.Doc())
		streamServer := fmt.Sprintf("%s.%s_%s%s", goPackage, parser.CamelCase(service.Name), parser.CamelCase(rpc.Name), "Client")
		buffer, err := util.With("sharedFn").Parse(text).Execute(map[string]interface{}{
			"serviceName":    stringx.From(service.Name).ToCamel(),
			"rpcServiceName": parser.CamelCase(service.Name),
			"method":         parser.CamelCase(rpc.Name),
			"package":        goPackage,
			"pbRequest":      parser.CamelCase(rpc.RequestType),
			"pbResponse":     parser.CamelCase(rpc.ReturnsType),
			"hasComment":     len(comment) > 0,
			"comment":        comment,
			"hasReq":         !rpc.StreamsRequest,
			"notStream":      !rpc.StreamsRequest && !rpc.StreamsReturns,
			"streamBody":     streamServer,
		})
		if err != nil {
			return nil, err
		}

		functions = append(functions, buffer.String())
	}

	return functions, nil
}

func (g *DefaultGenerator) getInterfaceFuncs(goPackage string, service parser.Service) ([]string, error) {
	functions := make([]string, 0)

	for _, rpc := range service.RPC {
		text, err := pathx.LoadTemplate(category, callInterfaceFunctionTemplateFile, callInterfaceFunctionTemplate)
		if err != nil {
			return nil, err
		}

		comment := parser.GetComment(rpc.Doc())
		streamServer := fmt.Sprintf("%s.%s_%s%s", goPackage, parser.CamelCase(service.Name), parser.CamelCase(rpc.Name), "Client")
		buffer, err := util.With("interfaceFn").Parse(text).Execute(
			map[string]interface{}{
				"hasComment": len(comment) > 0,
				"comment":    comment,
				"method":     parser.CamelCase(rpc.Name),
				"hasReq":     !rpc.StreamsRequest,
				"pbRequest":  parser.CamelCase(rpc.RequestType),
				"notStream":  !rpc.StreamsRequest && !rpc.StreamsReturns,
				"pbResponse": parser.CamelCase(rpc.ReturnsType),
				"streamBody": streamServer,
			})
		if err != nil {
			return nil, err
		}

		functions = append(functions, buffer.String())
	}

	return functions, nil
}
