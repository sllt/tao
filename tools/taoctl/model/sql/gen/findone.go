package gen

import (
	"github.com/sllt/tao/tools/taoctl/model/sql/template"
	"github.com/sllt/tao/tools/taoctl/util"
	"github.com/sllt/tao/tools/taoctl/util/pathx"
	"github.com/sllt/tao/tools/taoctl/util/stringx"
)

func genFindOne(table Table, withCache, postgreSql bool) (string, string, error) {
	camel := table.Name.ToCamel()
	text, err := pathx.LoadTemplate(category, findOneTemplateFile, template.FindOne)
	if err != nil {
		return "", "", err
	}

	output, err := util.With("findOne").
		Parse(text).
		Execute(map[string]interface{}{
			"withCache":                 withCache,
			"upperStartCamelObject":     camel,
			"lowerStartCamelObject":     stringx.From(camel).Untitle(),
			"originalPrimaryKey":        wrapWithRawString(table.PrimaryKey.Name.Source(), postgreSql),
			"lowerStartCamelPrimaryKey": util.EscapeGolangKeyword(stringx.From(table.PrimaryKey.Name.ToCamel()).Untitle()),
			"dataType":                  table.PrimaryKey.DataType,
			"cacheKey":                  table.PrimaryCacheKey.KeyExpression,
			"cacheKeyVariable":          table.PrimaryCacheKey.KeyLeft,
			"postgreSql":                postgreSql,
			"data":                      table,
		})
	if err != nil {
		return "", "", err
	}

	text, err = pathx.LoadTemplate(category, findOneMethodTemplateFile, template.FindOneMethod)
	if err != nil {
		return "", "", err
	}

	findOneMethod, err := util.With("findOneMethod").
		Parse(text).
		Execute(map[string]interface{}{
			"upperStartCamelObject":     camel,
			"lowerStartCamelPrimaryKey": util.EscapeGolangKeyword(stringx.From(table.PrimaryKey.Name.ToCamel()).Untitle()),
			"dataType":                  table.PrimaryKey.DataType,
			"data":                      table,
		})
	if err != nil {
		return "", "", err
	}

	return output.String(), findOneMethod.String(), nil
}
