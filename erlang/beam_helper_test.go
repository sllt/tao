package erlang_test

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	beamBuildOnce sync.Once
	beamOutDir    string
	sslEbinDir    string
	beamBuildErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if beamOutDir != "" {
		_ = os.RemoveAll(beamOutDir)
	}
	os.Exit(code)
}

func ensureBEAMFixture(t *testing.T) (ebinDir string, sslDir string) {
	t.Helper()
	if _, err := exec.LookPath("erl"); err != nil {
		t.Skip("erl not found in PATH")
	}
	if _, err := exec.LookPath("erlc"); err != nil {
		t.Skip("erlc not found in PATH")
	}
	if _, err := exec.LookPath("epmd"); err != nil {
		t.Skip("epmd not found in PATH")
	}

	beamBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "tao-erlang-fixture-*")
		if err != nil {
			beamBuildErr = err
			return
		}
		src := filepath.Join("testdata", "beam", "tao_test_server.erl")
		cmd := exec.Command("erlc", "-Wall", "-o", dir, src)
		cmd.Env = append(os.Environ(), "ERL_CRASH_DUMP=/dev/null")
		if out, err := cmd.CombinedOutput(); err != nil {
			beamBuildErr = fmt.Errorf("erlc failed: %w: %s", err, string(out))
			return
		}

		qCmd := exec.Command("erl", "-noinput", "-eval", `io:format("~s~n", [filename:dirname(code:which(inet_tls_dist))]), halt(0).`)
		qCmd.Env = append(os.Environ(), "ERL_CRASH_DUMP=/dev/null")
		out, err := qCmd.Output()
		if err != nil {
			beamBuildErr = fmt.Errorf("locate inet_tls_dist: %w", err)
			return
		}
		beamOutDir = dir
		sslEbinDir = strings.TrimSpace(string(out))
	})
	if beamBuildErr != nil {
		t.Fatalf("build BEAM fixture: %v", beamBuildErr)
	}
	return beamOutDir, sslEbinDir
}

type isolatedEPMD struct {
	addr string
	port int
	cmd  *exec.Cmd
}

func startIsolatedEPMD(t *testing.T) *isolatedEPMD {
	t.Helper()
	if _, err := exec.LookPath("epmd"); err != nil {
		t.Skip("epmd not found in PATH")
	}

	for attempt := 0; attempt < 16; attempt++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve epmd port: %v", err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()

		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		cmd := exec.Command("epmd", "-address", "127.0.0.1", "-port", strconv.Itoa(port))
		cmd.Env = append(os.Environ(), "ERL_CRASH_DUMP=/dev/null")
		if err := cmd.Start(); err != nil {
			t.Fatalf("start epmd: %v", err)
		}

		ready := false
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				break
			}
			c, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
			if err == nil {
				_ = c.Close()
				ready = true
				break
			}
			time.Sleep(15 * time.Millisecond)
		}
		if ready {
			ep := &isolatedEPMD{addr: addr, port: port, cmd: cmd}
			t.Cleanup(func() { ep.Stop() })
			return ep
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	t.Fatal("failed to start isolated epmd after 16 attempts")
	return nil
}

func (e *isolatedEPMD) Stop() {
	if e == nil || e.cmd == nil || e.cmd.Process == nil {
		return
	}
	_ = e.cmd.Process.Kill()
	_, _ = e.cmd.Process.Wait()
}

type distTLSBundle struct {
	GoTLS         *tls.Config
	WrongCATLS    *tls.Config
	ErlSSLOptFile string
}

func generateDistTLSBundle(t *testing.T) *distTLSBundle {
	t.Helper()
	dir := t.TempDir()

	caKey, caCert, caPEM := mustMakeCA(t, "Tao Dist Test CA")
	_, _, wrongCAPEM := mustMakeCA(t, "Wrong Dist CA")

	nodeCertPEM, nodeKeyPEM := mustMakeLeaf(t, caKey, caCert, "127.0.0.1", []string{"localhost"}, []net.IP{net.ParseIP("127.0.0.1")})

	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "node.pem")
	keyPath := filepath.Join(dir, "node-key.pem")
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(certPath, nodeCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, nodeKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}

	optContent := fmt.Sprintf(
		"[{server, [{certfile, %q}, {keyfile, %q}, {cacertfile, %q}, {verify, verify_peer}, {fail_if_no_peer_cert, true}]},\n"+
			" {client, [{certfile, %q}, {keyfile, %q}, {cacertfile, %q}, {verify, verify_peer}]}].\n",
		certPath, keyPath, caPath,
		certPath, keyPath, caPath,
	)
	optPath := filepath.Join(dir, "ssl_dist.conf")
	if err := os.WriteFile(optPath, []byte(optContent), 0o600); err != nil {
		t.Fatal(err)
	}

	keyPair, err := tls.X509KeyPair(nodeCertPEM, nodeKeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	rootPool := x509.NewCertPool()
	rootPool.AppendCertsFromPEM(caPEM)

	wrongPool := x509.NewCertPool()
	wrongPool.AppendCertsFromPEM(wrongCAPEM)

	goTLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      rootPool,
		ClientCAs:    rootPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   "127.0.0.1",
	}
	wrongCATLS := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{keyPair},
		RootCAs:      wrongPool,
		ClientCAs:    wrongPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ServerName:   "127.0.0.1",
	}

	return &distTLSBundle{
		GoTLS:         goTLS,
		WrongCATLS:    wrongCATLS,
		ErlSSLOptFile: optPath,
	}
}

func mustMakeCA(t *testing.T, cn string) (*ecdsa.PrivateKey, *x509.Certificate, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return priv, cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func mustMakeLeaf(t *testing.T, caKey *ecdsa.PrivateKey, caCert *x509.Certificate, cn string, dns []string, ips []net.IP) ([]byte, []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     dns,
		IPAddresses:  ips,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &priv.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

type beamServer struct {
	fullName string
	cmd      *exec.Cmd
	done     chan struct{}
}

type beamServerOptions struct {
	AliveName      string
	Cookie         string
	EPMDPort       int
	TLSOptFile     string
	NetTickTimeSec int
}

func startBEAMServer(t *testing.T, opts beamServerOptions) *beamServer {
	t.Helper()
	ebinDir, sslDir := ensureBEAMFixture(t)

	fullName := opts.AliveName + "@127.0.0.1"
	args := []string{
		"-noinput",
		"-name", fullName,
		"-setcookie", opts.Cookie,
		"-epmd_port", strconv.Itoa(opts.EPMDPort),
		"-pa", ebinDir,
	}
	if opts.NetTickTimeSec > 0 {
		args = append(args, "-kernel", "net_ticktime", strconv.Itoa(opts.NetTickTimeSec))
	}
	if opts.TLSOptFile != "" {
		args = append(args,
			"-pa", sslDir,
			"-proto_dist", "inet_tls",
			"-ssl_dist_optfile", opts.TLSOptFile,
		)
	}
	args = append(args, "-eval",
		`{ok, _} = tao_test_server:start_link(), io:format("TAO_BEAM_READY~n"), receive stop_vm -> halt(0) end.`,
	)

	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "erl", args...)
	cmd.Env = append(os.Environ(),
		"ERL_CRASH_DUMP=/dev/null",
		"ERL_EPMD_PORT="+strconv.Itoa(opts.EPMDPort),
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var stderrBuf strings.Builder
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start erl (%s): %v", fullName, err)
	}

	readyCh := make(chan error, 1)
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) == "TAO_BEAM_READY" {
				select {
				case readyCh <- nil:
				default:
				}
			}
		}
		err := cmd.Wait()
		select {
		case readyCh <- fmt.Errorf("erl exited before ready (%v): %s", err, stderrBuf.String()):
		default:
		}
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cancel()
			t.Fatal(err)
		}
	case <-time.After(15 * time.Second):
		cancel()
		t.Fatalf("timed out waiting for BEAM server %s ready; stderr=%s", fullName, stderrBuf.String())
	}

	srv := &beamServer{
		fullName: fullName,
		cmd:      cmd,
		done:     doneCh,
	}
	t.Cleanup(func() {
		cancel()
		srv.Stop()
	})
	return srv
}

func (b *beamServer) Stop() {
	if b == nil || b.cmd == nil || b.cmd.Process == nil {
		return
	}
	_ = b.cmd.Process.Kill()
	<-b.done
}
