package handler

import (
	"net/http"
	"sync"

	"manlu.org/tao/core/load"
	"manlu.org/tao/core/logx"
	"manlu.org/tao/core/stat"
	"manlu.org/tao/rest/httpx"
	"manlu.org/tao/rest/internal/response"
)

const serviceType = "api"

var (
	sheddingStat *load.SheddingStat
	lock         sync.Mutex
)

// SheddingHandler returns a middleware that does load shedding.
func SheddingHandler(shedder load.Shedder, metrics *stat.Metrics) func(http.Handler) http.Handler {
	if shedder == nil {
		return func(next http.Handler) http.Handler {
			return next
		}
	}

	ensureSheddingStat()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sheddingStat.IncrementTotal()
			promise, err := shedder.Allow()
			if err != nil {
				metrics.AddDrop()
				sheddingStat.IncrementDrop()
				logx.Errorf("[http] dropped, %s - %s - %s",
					r.RequestURI, httpx.GetRemoteAddr(r), r.UserAgent())
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}

			cw := &response.WithCodeResponseWriter{Writer: w}
			defer func() {
				if cw.Code == http.StatusServiceUnavailable {
					promise.Fail()
				} else {
					sheddingStat.IncrementPass()
					promise.Pass()
				}
			}()
			next.ServeHTTP(cw, r)
		})
	}
}

func ensureSheddingStat() {
	lock.Lock()
	if sheddingStat == nil {
		sheddingStat = load.NewSheddingStat(serviceType)
	}
	lock.Unlock()
}
