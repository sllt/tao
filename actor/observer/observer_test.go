package observer

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/sllt/tao/actor"
)

type sampleActor struct{ actor.Actor }

func (*sampleActor) Receive(any) error { return nil }
func (*sampleActor) Inspect(context.Context) (any, error) {
	return map[string]any{"value": 42, "text": "<script>alert(1)</script>"}, nil
}
func fixture(t *testing.T) (*Observer, *actor.Node, actor.PID) {
	t.Helper()
	node, err := actor.NewNode(actor.NodeOptions{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Stop(context.Background()); err != nil {
			t.Error(err)
		}
	})
	o, err := New(node)
	if err != nil {
		t.Fatal(err)
	}
	pid, err := node.Spawn(func() actor.Behavior { return &sampleActor{} }, actor.WithName("sample"))
	if err != nil {
		t.Fatal(err)
	}
	return o, node, pid
}
func TestObserverAPIAndEmbeddedAssets(t *testing.T) {
	o, _, pid := fixture(t)
	for _, path := range []string{"/", "/style.css", "/app.js", "/api/nodes", "/api/snapshot", "/api/actor?pid=" + url.QueryEscape(pid.String()), "/api/inspect?pid=" + url.QueryEscape(pid.String())} {
		recorder := httptest.NewRecorder()
		o.ServeHTTP(recorder, httptest.NewRequest("GET", path, nil))
		if recorder.Code != 200 || recorder.Body.Len() == 0 {
			t.Fatalf("%s: %d %s", path, recorder.Code, recorder.Body.String())
		}
		if strings.HasPrefix(path, "/api/") {
			var result any
			if err := json.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
		}
		if recorder.Header().Get("Content-Security-Policy") == "" {
			t.Fatal("missing embedded UI policy")
		}
	}
}
func TestObserverBoundaryErrors(t *testing.T) {
	o, _, _ := fixture(t)
	for _, test := range []struct {
		method, path string
		status       int
	}{{"POST", "/api/snapshot", 405}, {"GET", "/api/snapshot?limit=1001", 400}, {"GET", "/api/snapshot?after=bad", 400}, {"GET", "/api/snapshot?node=missing", 404}, {"GET", "/api/inspect?pid=bad", 400}} {
		recorder := httptest.NewRecorder()
		o.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, nil))
		if recorder.Code != test.status {
			t.Fatalf("%s: %d", test.path, recorder.Code)
		}
	}
}
func TestObserverListenAndShutdown(t *testing.T) {
	o, _, _ := fixture(t)
	server, err := o.Listen("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Get("http://" + server.Addr().String() + "/api/nodes")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.Status)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-server.Done()
	if err := server.Err(); err != nil {
		t.Fatal(err)
	}
}
