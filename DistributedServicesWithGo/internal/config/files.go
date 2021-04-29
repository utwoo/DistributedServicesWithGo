package config

import (
	"os"
	"path/filepath"
)

// These variables define the paths to the certs we generated and need to look
// up and parse for our tests. I would use constants and the const keyword if Go
// allowed using const with function calls.
var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
	RootCertFile   = configFile("root-client.pem")
	RootKeyFile    = configFile("root-client-key.pem")
	NobodyCertFile = configFile("nobody-client.pem")
	NobodyKeyFile  = configFile("nobody-client-key.pem")
	ACLModelFile   = configFile("model.conf")
	ACLPolicyFile  = configFile("policy.csv")
)

func configFile(filename string) string {
	if dir := os.Getenv("CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	homeDir, err := os.UserHomeDir()
	if err != nil {
		panic(err)
	}
	return filepath.Join(homeDir, ".proglog", filename)
}