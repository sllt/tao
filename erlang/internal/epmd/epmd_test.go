package epmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func startIsolatedEPMD(t *testing.T) (addr string, kill func()) {
	t.Helper()
	epmdBin, err := exec.LookPath("epmd")
	if err != nil {
		t.Skip("epmd not found in PATH")
	}
	// Pick a free loopback port without touching default port 4369.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	cmd := exec.Command(epmdBin, "-address", "127.0.0.1", "-port", strconv.Itoa(port))
	if err := cmd.Start(); err != nil {
		t.Fatalf("start epmd: %v", err)
	}
	addr = fmt.Sprintf("127.0.0.1:%d", port)

	deadline := time.Now().Add(3 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("isolated epmd did not start listening on %s", addr)
		}
		time.Sleep(15 * time.Millisecond)
	}

	var killed bool
	killFn := func() {
		if killed {
			return
		}
		killed = true
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	t.Cleanup(killFn)
	return addr, killFn
}

func TestSplitNodeName(t *testing.T) {
	alive, host, err := SplitNodeName("tao_bridge@127.0.0.1")
	if err != nil || alive != "tao_bridge" || host != "127.0.0.1" {
		t.Fatalf("unexpected split: %q %q %v", alive, host, err)
	}
	for _, bad := range []string{"", "@host", "alive@", "a@b@c", "bad space@host", "非ascii@host"} {
		if _, _, err := SplitNodeName(bad); !errors.Is(err, ErrInvalidNodeName) {
			t.Fatalf("expected ErrInvalidNodeName for %q, got %v", bad, err)
		}
	}
}

func TestRealEPMDRegisterLookupDuplicateAndLoss(t *testing.T) {
	addr, killEPMD := startIsolatedEPMD(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Missing node returns ErrNodeNotFound (2-byte [119, 1] response)
	if _, err := LookupPort(ctx, addr, "missing_node"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("expected ErrNodeNotFound, got %v", err)
	}

	reg, err := Register(ctx, addr, "tao_test", 19100, true, nil)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	defer reg.Close()
	if reg.Creation() == 0 {
		t.Fatalf("expected non-zero 32-bit creation from EPMD v6, got 0")
	}

	// Duplicate registration rejected directly without renaming
	if _, err := Register(ctx, addr, "tao_test", 19101, true, nil); !errors.Is(err, ErrDuplicateName) {
		t.Fatalf("expected ErrDuplicateName, got %v", err)
	}

	info, err := LookupPort(ctx, addr, "tao_test")
	if err != nil {
		t.Fatalf("LookupPort: %v", err)
	}
	if info.Port != 19100 || info.NodeType != NodeTypeHidden || info.AliveName != "tao_test" {
		t.Fatalf("unexpected PortInfo: %+v", info)
	}

	_, listing, err := Names(ctx, addr)
	if err != nil || !strings.Contains(listing, "name tao_test at port 19100") {
		t.Fatalf("Names listing mismatch: %q err=%v", listing, err)
	}

	// Killing EPMD closes registration socket and triggers reg.Lost()
	killEPMD()
	select {
	case <-reg.Lost():
	case <-time.After(2 * time.Second):
		t.Fatalf("expected reg.Lost() to close when EPMD died")
	}
}
