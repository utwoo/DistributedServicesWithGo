package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	Server        bool
}

// Our tests use a few different *tls.Config configurations, and SetupTLSConfig() allows
// us to get each type of *tls.Config with one function call. These are the different configurations
//• Client *tls.Config is set up to verify the server’s certificate with the client’s by setting the *tls.Config ’s RootCAs .
//• Client *tls.Config is set up to verify the server’s certificate and allow the server to verify the client’s certificate
//  by setting its RootCAs and its Certificates .
//• Server *tls.Config is set up to verify the client’s certificate and allow the
//  client to verify the server’s certificate by setting its ClientCAs , Certificate , and ClientAuth mode set to tls.RequireAndVerifyCert
func SetupTLSConfig(ctg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if ctg.CertFile != "" && ctg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(ctg.CertFile, ctg.KeyFile)
		if err != nil {
			return nil, err
		}
	}
	if ctg.CAFile != "" {
		b, err := ioutil.ReadFile(ctg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificate: %q", ctg.CAFile)
		}
		if ctg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = ctg.ServerAddress
	}
	return tlsConfig, nil
}
