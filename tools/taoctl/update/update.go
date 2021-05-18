package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"path"

	"manlu.org/tao/core/conf"
	"manlu.org/tao/core/hash"
	"manlu.org/tao/core/logx"
	"manlu.org/tao/tools/taoctl/update/config"
	"manlu.org/tao/tools/taoctl/util"
)

const (
	contentMd5Header = "Content-Md5"
	filename         = "taoctl"
)

var configFile = flag.String("f", "etc/update-api.json", "the config file")

func forChksumHandler(file string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !util.FileExists(file) {
			logx.Errorf("file %q not exist", file)
			http.Error(w, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}

		content, err := ioutil.ReadFile(file)
		if err != nil {
			logx.Error(err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		chksum := hash.Md5Hex(content)
		if chksum == r.Header.Get(contentMd5Header) {
			w.WriteHeader(http.StatusNotModified)
			return
		}

		w.Header().Set(contentMd5Header, chksum)
		next.ServeHTTP(w, r)
	})
}

func main() {
	flag.Parse()

	var c config.Config
	conf.MustLoad(*configFile, &c)

	fs := http.FileServer(http.Dir(c.FileDir))
	http.Handle(c.FilePath, http.StripPrefix(c.FilePath, forChksumHandler(path.Join(c.FileDir, filename), fs)))
	logx.Must(http.ListenAndServe(c.ListenOn, nil))
}
