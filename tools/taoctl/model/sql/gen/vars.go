package gen

import (
	"strings"

	"manlu.org/tao/tools/taoctl/model/sql/template"
	"manlu.org/tao/tools/taoctl/util"
	"manlu.org/tao/tools/taoctl/util/stringx"
)

func genVars(table Table, withCache bool) (string, error) {
	keys := make([]string, 0)
	keys = append(keys, table.PrimaryCacheKey.VarExpression)
	for _, v := range table.UniqueCacheKey {
		keys = append(keys, v.VarExpression)
	}

	camel := table.Name.ToCamel()
	text, err := util.LoadTemplate(category, varTemplateFile, template.Vars)
	if err != nil {
		return "", err
	}

	output, err := util.With("var").Parse(text).
		GoFmt(true).Execute(map[string]interface{}{
		"lowerStartCamelObject": stringx.From(camel).Untitle(),
		"upperStartCamelObject": camel,
		"cacheKeys":             strings.Join(keys, "\n"),
		"autoIncrement":         table.PrimaryKey.AutoIncrement,
		"originalPrimaryKey":    wrapWithRawString(table.PrimaryKey.Name.Source()),
		"withCache":             withCache,
	})
	if err != nil {
		return "", err
	}

	return output.String(), nil
}