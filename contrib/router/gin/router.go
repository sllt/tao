package gin

import (
	"github.com/gin-gonic/gin"
	"manlu.org/tao/rest/httpx"
	"manlu.org/tao/rest/pathvar"
	"net/http"
	"strings"
)

type ginRouter struct {
	g *gin.Engine
}

// NewRouter returns a gin.Router.
func NewRouter(opts ...Option) httpx.Router {
	g := gin.New()
	cfg := config{
		redirectTrailingSlash: true,
		redirectFixedPath:     false,
	}
	cfg.options(opts...)

	g.RedirectTrailingSlash = cfg.redirectTrailingSlash
	g.RedirectFixedPath = cfg.redirectFixedPath
	return &ginRouter{g: g}
}

func (pr *ginRouter) Handle(method, reqPath string, handler http.Handler) error {
	if !validMethod(method) {
		return ErrInvalidMethod
	}

	if len(reqPath) == 0 || reqPath[0] != '/' {
		return ErrInvalidPath
	}

	pr.g.Handle(strings.ToUpper(method), reqPath, func(ctx *gin.Context) {
		params := make(map[string]string)
		for i := 0; i < len(ctx.Params); i++ {
			params[ctx.Params[i].Key] = ctx.Params[i].Value
		}
		if len(params) > 0 {
			ctx.Request = pathvar.WithVars(ctx.Request, params)
		}
		handler.ServeHTTP(ctx.Writer, ctx.Request)
	})
	return nil
}

func (pr *ginRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pr.g.ServeHTTP(w, r)
}

func (pr *ginRouter) SetNotFoundHandler(handler http.Handler) {
	pr.g.NoRoute(gin.WrapH(handler))
}

func (pr *ginRouter) SetNotAllowedHandler(handler http.Handler) {
	pr.g.NoMethod(gin.WrapH(handler))
}

func validMethod(method string) bool {
	return method == http.MethodDelete || method == http.MethodGet ||
		method == http.MethodHead || method == http.MethodOptions ||
		method == http.MethodPatch || method == http.MethodPost ||
		method == http.MethodPut
}
