//go:build secured

package aerospike

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v8"
	"go-micro.dev/v6/store"
)

// Secured lane tests run against the EE TLS fixture (make secured-up).
// They require AEROSPIKE_SECURED=1 or skip with an explicit reason; an
// explicitly requested secured run (SECURED_REQUIRED=1) fails instead.
func securedEnv(t *testing.T) (addr, certDir string) {
	t.Helper()
	addr = os.Getenv("AEROSPIKE_SECURED_ADDR")
	if addr == "" {
		addr = "127.0.0.1:4533"
	}
	certDir = os.Getenv("AEROSPIKE_TLS_CERT_DIR")
	if certDir == "" {
		certDir = "docker-certs"
	}
	if _, err := os.Stat(filepath.Join(certDir, "ca.crt")); err != nil {
		msg := fmt.Sprintf("secured fixture prerequisites missing (certs in %s): run `make secured-up`", certDir)
		if os.Getenv("SECURED_REQUIRED") == "1" {
			t.Fatal(msg)
		}
		t.Skip("skipping secured test: " + msg)
	}
	return addr, certDir
}

func mustCA(t *testing.T, certDir, name string) *x509.CertPool {
	t.Helper()
	pem, err := os.ReadFile(filepath.Join(certDir, name))
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		t.Fatalf("bad CA file %s", name)
	}
	return pool
}

func mustClientCert(t *testing.T, certDir, name string) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(certDir, name+".crt"),
		filepath.Join(certDir, name+".key"))
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

func securedStore(t *testing.T, addr, certDir string, opts ...store.Option) store.Store {
	t.Helper()
	s := NewStore(append([]store.Option{
		store.Nodes(addr), store.Database("test"),
	}, opts...)...)
	if err := s.Init(); err != nil {
		t.Fatalf("secured init: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func trustAll(t *testing.T, certDir string) *tls.Config {
	return &tls.Config{
		RootCAs:      mustCA(t, certDir, "ca.crt"),
		ServerName:   "aerospike-tls",
		Certificates: []tls.Certificate{mustClientCert(t, certDir, "client")},
		MinVersion:   tls.VersionTLS12,
	}
}

// TestSecuredRoundTrip: trusted TLS + valid credentials, real server CRUD.
func TestSecuredRoundTrip(t *testing.T) {
	addr, certDir := securedEnv(t)
	s := securedStore(t, addr, certDir,
		WithTLS(trustAll(t, certDir)), WithAuth("admin", "admin"))
	rec := &store.Record{Key: "tls-rec", Value: []byte("secret")}
	if err := s.Write(rec); err != nil {
		t.Fatal(err)
	}
	r, err := s.Read("tls-rec")
	if err != nil || string(r[0].Value) != "secret" {
		t.Fatalf("secured read: %v", err)
	}
	if err := s.Delete("tls-rec"); err != nil {
		t.Fatal(err)
	}
}

// TestSecuredUntrustedCA: a CA that did not sign the server cert is rejected.
func TestSecuredUntrustedCA(t *testing.T) {
	addr, certDir := securedEnv(t)
	bad := &tls.Config{
		RootCAs:      mustCA(t, certDir, "other-ca.crt"),
		ServerName:   "aerospike-tls",
		Certificates: []tls.Certificate{mustClientCert(t, certDir, "client")},
	}
	s := NewStore(store.Nodes(addr), store.Database("test"),
		WithTLS(bad), WithAuth("admin", "admin"))
	if err := s.Init(); err == nil {
		s.Close()
		t.Fatal("Init succeeded with untrusted CA")
	}
}

// TestSecuredWrongServerName: connecting under a name absent from the
// server certificate's SAN must be rejected. The client verifies against
// the dialed host, so we dial "localhost" (SAN has aerospike-tls + IP only).
func TestSecuredWrongServerName(t *testing.T) {
	addr, certDir := securedEnv(t)
	host := "localhost" + addr[strings.LastIndex(addr, ":"):]
	bad := &tls.Config{
		RootCAs:      mustCA(t, certDir, "ca.crt"),
		Certificates: []tls.Certificate{mustClientCert(t, certDir, "client")},
	}
	s := NewStore(store.Nodes(host), store.Database("test"), WithTLS(bad))
	if err := s.Init(); err == nil {
		s.Close()
		t.Fatal("Init succeeded with wrong server name")
	}
}

// TestSecuredExpiredCert: a server presenting an expired certificate is
// rejected. Exercises the client's verification against a local TLS endpoint
// so the fixture does not need an expired server cert.
func TestSecuredExpiredCert(t *testing.T) {
	securedEnv(t)
	// Build an expired CA + server cert in-process.
	caKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "expired-ca"},
		NotBefore:             now.Add(-48 * time.Hour),
		NotAfter:              now.Add(-24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	srvKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	srvTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    now.Add(-48 * time.Hour),
		NotAfter:     now.Add(-24 * time.Hour), // expired
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, srvTmpl, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	srvCert := tls.Certificate{
		Certificate: [][]byte{srvDER, caDER},
		PrivateKey:  srvKey,
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{srvCert}})
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	s := NewStore(store.Nodes(ln.Addr().String()), store.Database("test"),
		WithTLS(&tls.Config{RootCAs: pool}))
	err = s.Init()
	if err == nil {
		s.Close()
		t.Fatal("Init succeeded against an expired server certificate")
	}
}

// TestNoPlaintextFallback instruments a listener and proves the store only
// sends TLS handshake bytes — never plaintext — when TLS is configured.
func TestNoPlaintextFallback(t *testing.T) {
	securedEnv(t)
	var mu sync.Mutex
	var firstBytes []byte
	sawPlaintext := false
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 5)
				c.SetReadDeadline(time.Now().Add(2 * time.Second))
				if _, err := c.Read(buf); err == nil {
					mu.Lock()
					firstBytes = append(firstBytes, buf[0])
					// A TLS ClientHello starts with record type 0x16.
					if buf[0] != 0x16 {
						sawPlaintext = true
					}
					mu.Unlock()
				}
			}(c)
		}
	}()
	// TLS against a non-TLS endpoint must fail without a plaintext retry.
	s := NewStore(store.Nodes(ln.Addr().String()), store.Database("test"),
		WithTLS(&tls.Config{InsecureSkipVerify: true}))
	_ = s.Init() // expected to fail
	time.Sleep(500 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if sawPlaintext {
		t.Fatal("store attempted a plaintext fallback after TLS failure")
	}
	if len(firstBytes) == 0 {
		t.Log("warning: no connection attempt observed")
	}
}

// TestSecuredCredentials: on this fixture (tls-authenticate-client any)
// authentication is by client-certificate CN. Valid = provisioned CN;
// invalid = a CA-signed cert whose CN is not a provisioned user. Errors must
// never carry supplied secrets.
func TestSecuredCredentials(t *testing.T) {
	addr, certDir := securedEnv(t)

	// Invalid: valid chain, unprovisioned CN — authentication fails.
	bad := &tls.Config{
		RootCAs:      mustCA(t, certDir, "ca.crt"),
		ServerName:   "aerospike-tls",
		Certificates: []tls.Certificate{mustClientCert(t, certDir, "other-user")},
	}
	s := NewStore(store.Nodes(addr), store.Database("test"),
		WithTLS(bad), WithAuth("not-a-provisioned-user", "wrong-pw-123"))
	err := s.Init()
	if err == nil {
		s.Close()
		t.Fatal("Init succeeded with an unprovisioned certificate identity")
	}
	if strings.Contains(err.Error(), "wrong-pw-123") {
		t.Fatalf("secret leaked in error: %v", err)
	}

	// Valid: provisioned PKI user (client cert CN) authenticates and writes.
	pol := aero.NewClientPolicy()
	pol.AuthMode = aero.AuthModePKI
	s = securedStore(t, addr, certDir,
		WithClientPolicy(pol), WithTLS(trustAll(t, certDir)))
	if err := s.Write(&store.Record{Key: "lim", Value: []byte("v")}); err != nil {
		t.Fatalf("valid credential write: %v", err)
	}
}

// TestSecuredIndexPrivilege: an underprivileged fixture user (read-write)
// fails Init — on EE even the namespace existence check needs sys-admin, so
// the failure is the first privilege boundary hit. Errors must not leak
// credentials. (Cross-references the original change's secured-privilege
// spike: index creation requires sindex-admin on top of sys-admin.)
func TestSecuredIndexPrivilege(t *testing.T) {
	addr, certDir := securedEnv(t)
	s := NewStore(store.Nodes(addr), store.Database("test2"),
		WithDataSet(fmt.Sprintf("noix%d", time.Now().UnixNano())),
		WithTLS(trustAll(t, certDir)), WithAuth("limited", "limited"),
		WithCreateIndex(true))
	err := s.Init()
	if err == nil {
		s.Close()
		t.Fatal("Init succeeded: unprivileged user created or used an index")
	}
	if strings.Contains(err.Error(), "limited") && strings.Contains(strings.ToLower(err.Error()), "password") {
		t.Fatalf("credentials leaked in error: %v", err)
	}
}

// TestSecuredMTLS: client-certificate acceptance and rejection
// (tls-authenticate-client any).
func TestSecuredMTLS(t *testing.T) {
	addr, certDir := securedEnv(t)
	base := func() *tls.Config {
		return &tls.Config{
			RootCAs:    mustCA(t, certDir, "ca.crt"),
			ServerName: "aerospike-tls",
			MinVersion: tls.VersionTLS12,
		}
	}
	// Trusted client cert + password auth: handshake and login succeed.
	s := securedStore(t, addr, certDir,
		WithTLS(&tls.Config{
			RootCAs:      mustCA(t, certDir, "ca.crt"),
			ServerName:   "aerospike-tls",
			Certificates: []tls.Certificate{mustClientCert(t, certDir, "client")},
		}),
		WithAuth("admin", "admin"))
	if err := s.Write(&store.Record{Key: "mtls", Value: []byte("v")}); err != nil {
		t.Fatalf("mTLS write: %v", err)
	}
	// Untrusted client cert: handshake must fail.
	bad := base()
	bad.Certificates = []tls.Certificate{mustClientCert(t, certDir, "untrusted-client")}
	s2 := NewStore(store.Nodes(addr), store.Database("test"), WithTLS(bad))
	if err := s2.Init(); err == nil {
		s2.Close()
		t.Fatal("Init succeeded with untrusted client certificate")
	}
	// No client cert: the server requires one.
	s3 := NewStore(store.Nodes(addr), store.Database("test"), WithTLS(base()))
	if err := s3.Init(); err == nil {
		s3.Close()
		t.Fatal("Init succeeded without a client certificate")
	}
}

// TestWithTLSNilDisablesSecured asserts the approved WithTLS(nil) semantics:
// it explicitly disables TLS even when the base policy has TlsConfig, so
// connecting to the TLS-only port must fail.
//
// REPORTED DEFECT: the nil check in Init skips the assignment, so the base
// policy's TlsConfig survives and the connection succeeds (plaintext would
// fail on the TLS-only port). Preserved failing until a production fix lands.
func TestWithTLSNilDisablesSecured(t *testing.T) {
	addr, certDir := securedEnv(t)
	pol := aero.NewClientPolicy()
	pol.User = "admin"
	pol.Password = "admin"
	pol.TlsConfig = trustAll(t, certDir)
	s := NewStore(store.Nodes(addr), store.Database("test"),
		WithClientPolicy(pol), WithTLS(nil))
	err := s.Init()
	if err == nil {
		s.Close()
		t.Fatal("WithTLS(nil) should disable TLS; connection to TLS-only port succeeded")
	}
	var ae aero.Error
	if errors.As(err, &ae) {
		_ = ae
	}
}
