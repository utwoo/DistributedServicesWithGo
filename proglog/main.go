package main

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"log"
	"os"
	"os/signal"
	"path"
	"syscall"
	"utwoo.com/proglog/internal/agent"
	"utwoo.com/proglog/internal/config"
)

func main() {
	commandLine := &cli{}

	// Our CLI is about as simple as it gets. In more complex applications, this command would act as the root
	// command tying together your subcommands. Cobra calls the RunE function
	// you set on your command when the command runs. Put or call the command’s
	// primary logic in that function. Cobra enables you to run hook functions to
	// run before and after RunE.
	cmd := &cobra.Command{
		Use:     "proglog",
		PreRunE: commandLine.setupConfig,
		RunE:    commandLine.run,
	}
	if err := setupFlags(cmd); err != nil {
		log.Fatal(err)
	}
	if err := cmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

// setupConfig(cmd *cobra.Command, args []string) reads the configuration and prepares
// the agent’s configuration. Cobra calls setupConfig() before running the command’s RunE function.
func (c *cli) setupConfig(cmd *cobra.Command, args []string) error {
	var err error

	configFile, err := cmd.Flags().GetString("config-file")
	if err != nil {
		return err
	}
	viper.SetConfigName(configFile)

	if err := viper.ReadInConfig(); err != nil {
		// it's ok if config file doesn't exist
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	c.cfg.DataDir = viper.GetString("data-dir")
	c.cfg.NodeName = viper.GetString("node-name")
	c.cfg.BindAddr = viper.GetString("bind-addr")
	c.cfg.RPCPort = viper.GetInt("rpc-port")
	c.cfg.StartJoinAddrs = viper.GetStringSlice("start-join-addrs")
	c.cfg.Bootstrap = viper.GetBool("bootstrap")
	c.cfg.ACLModelFile = viper.GetString("acl-mode-file")
	c.cfg.ACLPolicyFile = viper.GetString("acl-policy-file")
	c.cfg.ServerTLSConfig.CertFile = viper.GetString("server-tls-cert-file")
	c.cfg.ServerTLSConfig.KeyFile = viper.GetString("server-tls-key-file")
	c.cfg.ServerTLSConfig.CAFile = viper.GetString("server-tls-ca-file")
	c.cfg.PeerTLSConfig.CertFile = viper.GetString("peer-tls-cert-file")
	c.cfg.PeerTLSConfig.KeyFile = viper.GetString("peer-tls-key-file")
	c.cfg.PeerTLSConfig.CAFile = viper.GetString("peer-tls-ca-file")
	if c.cfg.ServerTLSConfig.CertFile != "" &&
		c.cfg.ServerTLSConfig.KeyFile != "" {
		c.cfg.ServerTLSConfig.Server = true
		c.cfg.Config.ServerTLSConfig, err = config.SetupTLSConfig(
			c.cfg.ServerTLSConfig,
		)
		if err != nil {
			return err
		}
	}
	if c.cfg.PeerTLSConfig.CertFile != "" &&
		c.cfg.PeerTLSConfig.KeyFile != "" {
		c.cfg.Config.PeerTLSConfig, err = config.SetupTLSConfig(
			c.cfg.PeerTLSConfig,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// run(cmd *cobra.Command, args []string) runs our executable’s logic by:
// • Creating the agent;
// • Handling signals from the operating system; and
// • Shutting down the agent gracefully when the operating system terminates the program.
func (c *cli) run(cmd *cobra.Command, args []string) error {
	var err error
	agent, err := agent.NewAgent(c.cfg.Config)
	if err != nil {
		return err
	}
	signalc := make(chan os.Signal, 1)
	signal.Notify(signalc, syscall.SIGINT, syscall.SIGTERM)
	<-signalc
	return agent.Shutdown()
}

// These flags allow people calling your CLI to configure the agent and learn the default configuration.
// With the pflag.FlagSet.{{type}}Var() methods, we can set our configuration’s values
// directly. However, the problem with setting the configurations directly is that
// not all types have supporting APIs out of the box. Our BindAddr configuration
// is an example, which is a *net.TCPAddr that we need to parse from a string. You
// can define custom flag values when you have enough flags of the same type,
// or just use an intermediate value otherwise.
// https://golang.org/pkg/flag/#Value
func setupFlags(cmd *cobra.Command) error {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	cmd.Flags().String("config-file", "", "Path to config file.")
	dataDir := path.Join(os.TempDir(), "proglog")
	cmd.Flags().String("data-dir",
		dataDir,
		"Directory to store log and Raft data.")
	cmd.Flags().String("node-name", hostname, "Unique server ID.")
	cmd.Flags().String("bind-addr",
		"127.0.0.1:8401",
		"Address to bind Serf on.")
	cmd.Flags().Int("rpc-port",
		8400,
		"Port for RPC clients (and Raft) connections.")
	cmd.Flags().StringSlice("start-join-addrs",
		nil,
		"Serf addresses to join.")
	cmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster.")
	cmd.Flags().String("acl-model-file", "", "Path to ACL model.")
	cmd.Flags().String("acl-policy-file", "", "Path to ACL policy.")
	cmd.Flags().String("server-tls-cert-file", "", "Path to server tls cert.")
	cmd.Flags().String("server-tls-key-file", "", "Path to server tls key.")
	cmd.Flags().String("server-tls-ca-file",
		"",
		"Path to server certificate authority.")
	cmd.Flags().String("peer-tls-cert-file", "", "Path to peer tls cert.")
	cmd.Flags().String("peer-tls-key-file", "", "Path to peer tls key.")
	cmd.Flags().String("peer-tls-ca-file",
		"",
		"Path to peer certificate authority.")
	return viper.BindPFlags(cmd.Flags())
}

// Create a cli struct in which I can put logic and data that’s common
// to all the commands. I created a separate cfg struct from the agent.Config struct
// to handle the field types that we can’t parse without error handling: the
// *net.TCPAddr and the *tls.Config.
type cli struct {
	cfg cfg
}

type cfg struct {
	agent.Config
	ServerTLSConfig config.TLSConfig
	PeerTLSConfig   config.TLSConfig
}
