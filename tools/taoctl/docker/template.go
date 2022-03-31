package docker

import (
	"github.com/urfave/cli"
	"manlu.org/tao/tools/taoctl/util/pathx"
)

const (
	category           = "docker"
	dockerTemplateFile = "docker.tpl"
	dockerTemplate     = `FROM golang:{{.Version}}alpine AS builder

LABEL stage=gobuilder

ENV CGO_ENABLED 0
{{if .Chinese}}ENV GOPROXY https://goproxy.cn,direct
{{end}}{{if .HasTimezone}}
RUN apk update --no-cache && apk add --no-cache tzdata
{{end}}
WORKDIR /build

ADD go.mod .
ADD go.sum .
RUN go mod download
COPY . .
{{if .Argument}}COPY {{.GoRelPath}}/etc /app/etc
{{end}}RUN go build -ldflags="-s -w" -o /app/{{.ExeFile}} {{.GoRelPath}}/{{.GoFile}}


FROM {{.BaseImage}}

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
{{if .HasTimezone}}COPY --from=builder /usr/share/zoneinfo/{{.Timezone}} /usr/share/zoneinfo/{{.Timezone}}
ENV TZ {{.Timezone}}
{{end}}
WORKDIR /app
COPY --from=builder /app/{{.ExeFile}} /app/{{.ExeFile}}{{if .Argument}}
COPY --from=builder /app/etc /app/etc{{end}}
{{if .HasPort}}
EXPOSE {{.Port}}
{{end}}
CMD ["./{{.ExeFile}}"{{.Argument}}]
`
)

// Clean deletes all templates files
func Clean() error {
	return pathx.Clean(category)
}

// GenTemplates creates docker template files
func GenTemplates(_ *cli.Context) error {
	return initTemplate()
}

// Category returns the const string of docker category
func Category() string {
	return category
}

// RevertTemplate recovers the deleted template files
func RevertTemplate(name string) error {
	return pathx.CreateTemplate(category, name, dockerTemplate)
}

// Update deletes and creates new template files
func Update() error {
	err := Clean()
	if err != nil {
		return err
	}

	return initTemplate()
}

func initTemplate() error {
	return pathx.InitTemplates(category, map[string]string{
		dockerTemplateFile: dockerTemplate,
	})
}
