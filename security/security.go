package security

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConf stores all of the parameters the MakeTLSConfig
type TLSConf struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	IsServer   bool
	ServerAddr string
}

// MakeTLSConfig takes in the custom config and creates a *tls.Config instance
func MakeTLSConfig(cfg TLSConf) (*tls.Config, error) {
	tlsConf := &tls.Config{}

	var err error
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		// create a certificate from a public/private key pair
		tlsConf.Certificates = make([]tls.Certificate, 1)
		tlsConf.Certificates[0], err = tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
	}

	if cfg.CAFile != "" {
		// read certificate
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}

		// parse pem-encoded certificates.
		ca := x509.NewCertPool()
		if ok := ca.AppendCertsFromPEM(b); !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %s", cfg.CAFile)
		}

		if cfg.IsServer {
			tlsConf.ClientCAs = ca

			// make sure that at least one valid certificate is given
			// during a handshake.
			tlsConf.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConf.RootCAs = ca
		}

		tlsConf.ServerName = cfg.ServerAddr
	}

	return tlsConf, nil
}
