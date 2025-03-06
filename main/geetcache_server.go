package main

import (
	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	geetcacheHandler, err := NewGeetCacheHandler()
	if err != nil {
		LOG.Fatalf("Couldn't initialize the command handler %v", err)
	}
	geetcacheServiceProcessor := NewGeetcacheServiceProcessor(geetcacheHandler)
	addr, err := getConfig().getRpcAddr()
	if err != nil {
		LOG.Fatalf("Couldn't get RPC address %v", err)
	}
	ssock, err := thrift.NewTServerSocket(addr)
	LOG.Infof("Addr is %s", addr)
	if err != nil {
		LOG.Fatalf("Couldn't create server socket for %s, error:%v", addr, err)
	}
	server := thrift.NewTSimpleServer4(geetcacheServiceProcessor, ssock,
		thrift.NewTBufferedTransportFactory(204800), thrift.NewTBinaryProtocolFactoryDefault())

	LOG.Info("Starting the server....")
	server.Serve()
}
