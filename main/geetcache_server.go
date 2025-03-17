package main

import (
	"flag"
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"

	gt "github.com/nipuntalukdar/geetcache/thrift"
)

func main() {
	fmt.Println("Make sure that IDs for the http listener config and raft config match ")
	configFile := flag.String("config", "config/config.json", "Path to configuration file")
	logfileconfig := flag.String("logfileconfig", "config/logfile_config.json", "logfileconfig")
	fmt.Println(*configFile, *logfileconfig)
	init_logger(*logfileconfig)
	config, err := initConfig(*configFile)
	if err != nil {
		SLOG.Fatal("Configuration", "error", err)
	}
	geetcacheHandler, err := NewGeetCacheHandler()
	if err != nil {
		SLOG.Fatal("Command Handler", "Initilization", err)
	}
	geetcacheServiceProcessor := gt.NewGeetcacheServiceProcessor(geetcacheHandler)
	addr := config.RPCAddr
	ssock, err := thrift.NewTServerSocket(addr)
	SLOG.Info("Thrift server Address", "Address", addr)
	if err != nil {
		SLOG.Fatal("Server socket create prpblem", "Address", addr, "error", err)
	}
	server := thrift.NewTSimpleServer4(geetcacheServiceProcessor, ssock,
		thrift.NewTBufferedTransportFactory(204800), thrift.NewTBinaryProtocolFactoryDefault())

	SLOG.Info("Starting the server....")
	server.Serve()
}
