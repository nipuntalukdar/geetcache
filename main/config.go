package main

import (
	"encoding/json"
	"os"
)

const (
	default_config = `
	{
		"myid" : "id1",
		"logs" : "data/logs",
		"sstore" : "data/db",
		"snapshot" : "data/snapshot",
		"rpcaddr" : "127.0.0.1:8000",
		"httpappdress" : "127.0.0.1:6000",
		"raftpeers" : [
			{   
	            "Suffrage" : 0,
				"Address" :"127.0.0.1:7000",
				"ID" : "id1"
	        },
			{   
	            "Suffrage" : 0,
				"Address" :"127.0.0.1:7001",
				"ID" : "id1"
	        },
			{   
	            "Suffrage" : 0,
				"Address" :"127.0.0.1:7002",
				"ID" : "id1"
	        }
		],
		"numpartitions" : 8192
	}`
)

type RaftPeer struct {
	Suffrage int    `json:"Suffrage"`
	Address  string `json:"Address"`
	ID       string `json:"ID"`
}

type Config struct {
	MyID          string     `json:"myid"`
	Logs          string     `json:"logs"`
	SStore        string     `json:"sstore"`
	Snapshot      string     `json:"snapshot"`
	RPCAddr       string     `json:"rpcaddr"`
	HTTPAddress   string     `json:"httpappdress"`
	RaftPeers     []RaftPeer `json:"raftpeers"`
	NumPartitions int        `json:"numpartitions"`
}

var (
	CONFIG *Config
)

func initConfigS(config_data []byte) (*Config, error) {
	var conf Config
	err := json.Unmarshal(config_data, &conf)
	CONFIG = &conf
	return &conf, err
}

func initConfig(configfile string) (*Config, error) {
	data, err := os.ReadFile(configfile)
	if err != nil {
		return nil, err
	}
	return initConfigS(data)
}


func (config *Config) GetRaftAddress(id string) (address string) {
	for _, rpeer := range config.RaftPeers {
		if rpeer.ID == id {
			address = rpeer.Address
			break
		}
	}
	return
}
