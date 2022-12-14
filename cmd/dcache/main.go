package main

import (
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/nireo/dcache/security"
	"github.com/nireo/dcache/service"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type config struct {
	service.Config
	serverconf security.TLSConf
	peerconf   security.TLSConf
}

func main() {
	conf := config{}
	cmd := &cobra.Command{
		Use:     "dcache",
		PreRunE: conf.setupConf,
		RunE:    conf.runService,
	}

	if err := parseFlags(cmd); err != nil {
		log.Fatalf("error parsing flags: %s", err)
	}

	if err := cmd.Execute(); err != nil {
		log.Fatalf("error running service: %s", err)
	}
}

func parseFlags(cmd *cobra.Command) error {
	cmd.Flags().String("conf", "", "Path to a configuration file.")
	cmd.Flags().
		Bool("in-memory",
			true,
			"Whether to keep even raft logs in memory. Improves performance but makes system less tolerant to failures.",
		)

	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cmd.Flags().
		String("data-dir", filepath.Join(os.TempDir(), "dcache"), "Where to store raft logs.")
	cmd.Flags().String("id", hostname, "Identifier on the cluster.")
	cmd.Flags().Int("rpc-port", 9200, "Port for gRPC clients and Raft connections.")
	cmd.Flags().
		StringSlice("join", nil, "Existing addresses in the cluster where you want this node to attempt connection")
	cmd.Flags().Bool("bootstrap", false, "Whether this node should bootstrap the cluster.")
	cmd.Flags().String("addr", "127.0.0.1:9000", "Address where serf is binded.")
	cmd.Flags().Bool("http", false, "Enable HTTP server for client communication")
	cmd.Flags().Bool("grpc", false, "Enable gRPC server for client communication")

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

	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return err
	}
	return nil
}

func (c *config) setupConf(cmd *cobra.Command, args []string) error {
	// try overriding flag values with values from a config file. However this config might not
	// exist. We need to check if the config file is valid; if not just use the flag values.
	confFile, err := cmd.Flags().GetString("conf")
	if err != nil {
		return err
	}
	viper.SetConfigFile(confFile)

	if err := viper.ReadInConfig(); err != nil {
		// if the error is viper.ConfigFileNotFoundError then we can ignore it. That just means
		// that the confFile variable is most likely "" (the default value).
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return err
		}
	}

	c.DataDir = viper.GetString("data-dir")
	c.BindAddr = viper.GetString("addr")
	c.RPCPort = viper.GetInt("rpc-port")
	c.Bootstrap = viper.GetBool("bootstrap")
	c.StartJoinAddrs = viper.GetStringSlice("join")
	c.EnableHTTP = viper.GetBool("http")
	c.NodeName = viper.GetString("id")
	c.EnableGRPC = viper.GetBool("grpc")
	c.EnableHTTP = viper.GetBool("http")
	c.serverconf.CertFile = viper.GetString("server-tls-cert-file")
	c.serverconf.KeyFile = viper.GetString("server-tls-key-file")
	c.serverconf.CAFile = viper.GetString("server-tls-ca-file")
	c.peerconf.CertFile = viper.GetString("peer-tls-cert-file")
	c.peerconf.KeyFile = viper.GetString("peer-tls-key-file")
	c.peerconf.CAFile = viper.GetString("peer-tls-ca-file")

	if c.serverconf.CertFile != "" &&
		c.serverconf.KeyFile != "" {
		c.serverconf.IsServer = true
		c.ServerTLS, err = security.MakeTLSConfig(
			c.serverconf,
		)
		if err != nil {
			return err
		}
	}

	if c.peerconf.CertFile != "" &&
		c.peerconf.KeyFile != "" {
		c.PeerTLS, err = security.MakeTLSConfig(
			c.peerconf,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (c *config) runService(cmd *cobra.Command, args []string) error {
	serv, err := service.New(c.Config)
	if err != nil {
		return err
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	return serv.Close()
}
