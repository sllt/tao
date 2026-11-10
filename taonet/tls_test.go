package taonet

import (
	"bytes"
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

func TestTLSOwnedChannelInputRelease(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	accepted := make(chan *Channel, 1)
	m := NewMux()
	if err := m.HandleChannel("owned", func(ctx context.Context, c *Channel) error {
		accepted <- c
		<-c.Done()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	a, b := localConfig("client", nil), localConfig("server", m)
	a.Transport = TransportConfig{TLS: clientTLS, ExpectedPeer: "server"}
	b.Transport = TransportConfig{TLS: serverTLS, ExpectedPeer: "client"}
	client, _ := pair(t, a, b)
	channel, err := client.OpenChannel(t.Context(), "owned", nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	peer := <-accepted
	inputs := make([][]byte, 32)
	receipts := make([]*SendReceipt, len(inputs))
	for i := range inputs {
		inputs[i] = bytes.Repeat([]byte{byte(i + 1)}, 1024)
		receipts[i], err = channel.SendOwnedWithReceipt(t.Context(), Message{Body: inputs[i]})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i, receipt := range receipts {
		waitSignal(t, receipt.Released())
		clear(inputs[i])
		delivery := receive(t, peer)
		if !bytes.Equal(delivery.Message().Body, bytes.Repeat([]byte{byte(i + 1)}, 1024)) {
			t.Fatalf("input %d reused before TLS finished reading it", i)
		}
		if err := delivery.Accept(); err != nil {
			t.Fatal(err)
		}
		delivery.Release()
		if err := receipt.Wait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	closeChannels(t, channel, peer)
}

func tlsConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	root, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(root)
	leaf := func(name string, serial int64) tls.Certificate {
		private, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		cert := &x509.Certificate{SerialNumber: big.NewInt(serial), DNSNames: []string{name}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		encoded, err := x509.CreateCertificate(rand.Reader, cert, root, &private.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		return tls.Certificate{Certificate: [][]byte{encoded}, PrivateKey: private}
	}
	return &tls.Config{RootCAs: roots, ServerName: "server", Certificates: []tls.Certificate{leaf("client", 2)}}, &tls.Config{ClientCAs: roots, Certificates: []tls.Certificate{leaf("server", 3)}}
}

func TestTLSIdentityAndNamespace(t *testing.T) {
	clientTLS, serverTLS := tlsConfigs(t)
	for _, scenario := range []string{"success", "wrong-identity", "wrong-namespace", "wrong-expected-peer", "missing-client-certificate"} {
		t.Run(scenario, func(t *testing.T) {
			m := NewMux()
			handle(t, m, "echo", func(_ context.Context, r *Request) (Message, error) { return r.Message, nil })
			a, b := localConfig("client", nil), localConfig("server", m)
			a.Transport = TransportConfig{TLS: clientTLS.Clone(), ExpectedPeer: "server"}
			b.Transport = TransportConfig{TLS: serverTLS.Clone(), ExpectedPeer: "client"}
			switch scenario {
			case "wrong-identity":
				a.Local.ID = "imposter"
				b.Transport.ExpectedPeer = ""
			case "wrong-namespace":
				b.Local.Namespace = "other"
			case "wrong-expected-peer":
				a.Transport.ExpectedPeer = "other"
			case "missing-client-certificate":
				a.Transport.TLS.Certificates = nil
			}
			client, _, ce, se := connectPair(t, a, b)
			if scenario != "success" {
				if ce == nil && se == nil {
					t.Fatal("invalid identity accepted")
				}
				return
			}
			if ce != nil || se != nil {
				t.Fatalf("TLS handshake: %v / %v", ce, se)
			}
			if client.Info().Transport != "tcp/tls" {
				t.Fatal(client.Info())
			}
			response, err := client.Request(t.Context(), "echo", Message{Body: []byte("secure")})
			if err != nil || string(response.Body) != "secure" {
				t.Fatalf("TLS request: %v", err)
			}
		})
	}
}
