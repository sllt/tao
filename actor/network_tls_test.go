package actor

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"testing"
	"time"
)

func TestActorNetworkMutualTLSUsesExpectedPeerIdentity(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), IsCA: true, BasicConstraintsValid: true, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	data, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(data)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	makeNode := func(name string, serial int64) *Node {
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		der, err := x509.CreateCertificate(rand.Reader, cert, root, &private.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		tlsConfig := &tls.Config{RootCAs: roots, ClientCAs: roots, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: private}}}
		n, err := StartNode(name+"@127.0.0.1:0", NodeOptions{Network: NetworkOptions{TLS: tlsConfig}, Register: []MessageType{{"command", 1, remoteCommand{}}, {"count", 1, int64(0)}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := n.Stop(ctx); err != nil {
				t.Error(err)
			}
		})
		return n
	}
	a, b := makeNode("a", 2), makeNode("b", 3)
	connectNodes(t, a, b)
	pid := spawn(t, b, factory(func(actor *testBehavior, msg any) error {
		if _, ok := msg.(remoteCommand); ok {
			return actor.Reply(int64(42))
		}
		return nil
	}))
	if value, err := a.Call[int64](t.Context(), pid, remoteCommand{}); value != 42 || err != nil {
		t.Fatalf("mTLS call: %d %v", value, err)
	}
}
