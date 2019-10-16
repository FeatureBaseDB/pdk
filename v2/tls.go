package pdk

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/pilosa/pilosa/logger"
	"github.com/pkg/errors"
)

// TLSConfig contains TLS configuration
type TLSConfig struct {
	// CertificatePath contains the path to the certificate (.crt or .pem file)
	CertificatePath string `json:"certificate" help:"Path to certificate file."`
	// CertificateKeyPath contains the path to the certificate key (.key file)
	CertificateKeyPath string `json:"key" help:"Path to certificate key file."`
	// CACertPath is the path to a CA certificate (.crt or .pem file)
	CACertPath string `json:"ca-certificate" help:"Path to CA certificate file."`
	// SkipVerify disables verification of server certificates.
	SkipVerify bool `json:"skip-verify" help:"Disables verification of server certificates."`
	// EnableClientVerification enables verification of client TLS certificates (Mutual TLS)
	EnableClientVerification bool `json:"enable-client-verification" help:"Enable verification of client certificates."`
}

type keypairReloader struct {
	certMu   sync.RWMutex
	cert     *tls.Certificate
	certPath string
	keyPath  string
}

func NewKeypairReloader(certPath, keyPath string, log logger.Logger) (*keypairReloader, error) {
	result := &keypairReloader{
		certPath: certPath,
		keyPath:  keyPath,
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	result.cert = &cert
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGHUP)
		for range c {
			log.Printf("Received SIGHUP, reloading TLS certificate and key from %q and %q", certPath, keyPath)
			if err := result.maybeReload(); err != nil {
				log.Printf("Keeping old TLS certificate because the new one could not be loaded: %v", err)
			}
		}
	}()
	return result, nil
}

func (kpr *keypairReloader) maybeReload() error {
	newCert, err := tls.LoadX509KeyPair(kpr.certPath, kpr.keyPath)
	if err != nil {
		return err
	}
	kpr.certMu.Lock()
	defer kpr.certMu.Unlock()
	kpr.cert = &newCert
	return nil
}

func (kpr *keypairReloader) GetCertificateFunc() func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		kpr.certMu.RLock()
		defer kpr.certMu.RUnlock()
		return kpr.cert, nil
	}
}

func (kpr *keypairReloader) GetClientCertificateFunc() func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
	return func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
		kpr.certMu.RLock()
		defer kpr.certMu.RUnlock()
		return kpr.cert, nil
	}
}

func GetTLSConfig(tlsConfig *TLSConfig, log logger.Logger) (TLSConfig *tls.Config, err error) {
	if tlsConfig == nil {
		return nil, nil
	}
	if tlsConfig.CertificatePath != "" && tlsConfig.CertificateKeyPath != "" {
		kpr, err := NewKeypairReloader(tlsConfig.CertificatePath, tlsConfig.CertificateKeyPath, log)
		if err != nil {
			return nil, errors.Wrap(err, "loading keypair")
		}
		TLSConfig = &tls.Config{
			InsecureSkipVerify:       tlsConfig.SkipVerify,
			PreferServerCipherSuites: true,
			MinVersion:               tls.VersionTLS12,
			GetCertificate:           kpr.GetCertificateFunc(),
			GetClientCertificate:     kpr.GetClientCertificateFunc(),
		}
		if tlsConfig.CACertPath != "" {
			b, err := ioutil.ReadFile(tlsConfig.CACertPath)
			if err != nil {
				return nil, errors.Wrap(err, "loading tls ca key")
			}
			certPool := x509.NewCertPool()

			ok := certPool.AppendCertsFromPEM(b)
			if !ok {
				return nil, errors.New("error parsing CA certificate")
			}
			TLSConfig.ClientCAs = certPool
			TLSConfig.RootCAs = certPool
		}
		if tlsConfig.EnableClientVerification {
			TLSConfig.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	return TLSConfig, nil
}
