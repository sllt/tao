Quick Start
==============

Download the module:

```console
go get -u manlu.org/tao/contrib/router/gin
```

For example:

```go
package main

import (
	"net/http"
	"strings"

	"manlu.org/tao/core/logx"
	"manlu.org/tao/core/service"
	"manlu.org/tao/rest"
	"manlu.org/tao/rest/httpx"
	"manlu.org/tao/contrib/router/gin"
	stdgin "github.com/gin-gonic/gin"
)

type CommonPathID struct {
	ID   int    `path:"id"`
	Name string `path:"name"`
}

func (c *CommonPathID) String() string {
	var builder strings.Builder
	builder.WriteString("CommonPathID(")
	builder.WriteString(fmt.Sprintf("ID=%v", c.ID))
	builder.WriteString(fmt.Sprintf(", Name=%s", c.Name))
	builder.WriteByte(')')
	return builder.String()
}

func init() {
	stdgin.SetMode(stdgin.ReleaseMode)
}

func main() {
	r := gin.NewRouter()
	engine := rest.MustNewServer(rest.RestConf{
		ServiceConf: service.ServiceConf{
			Log: logx.LogConf{
				Mode: "console",
			},
		},
		Port:     3345,
		Timeout:  20000,
		MaxConns: 500,
	}, rest.WithRouter(r))

	// NotFound defines a handler to respond whenever a route could
	// not be found.
	r.SetNotFoundHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		w.Write([]byte("nothing here"))
	}))
	// MethodNotAllowed defines a handler to respond whenever a method is
	// not allowed.
	r.SetNotAllowedHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(405)
		w.Write([]byte("405"))
	}))
	defer engine.Stop()

	engine.AddRoute(rest.Route{
		Method: http.MethodGet,
		Path:   "/api/:name/:id",  // GET /api/joh/123
		Handler: func(w http.ResponseWriter, r *http.Request) {
			var commonPath CommonPath
			err := httpx.Parse(r, &commonPath)
			if err != nil {
				return
			}
			w.Write([]byte(commonPath.String()))
		},
	})
	engine.Start()
}
```
