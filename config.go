package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"sync"
)

const (
	default_config = `
	{
		"myid" : 1,
		"myself" : "127.0.0.1:7001",
		"peerdir" : "/tmp/peer1",
		"logs" : "/tmp/logs1/logs",
		"sstore" : "/tmp/sstore1/db",
		"snapshot" : "/tmp/snapshot1",
		"rpcaddr" : "127.0.0.1:9091",
		"raftpeers" : [
			"127.0.0.1:7001",
			"127.0.0.1:7002",
			"127.0.0.1:7003"
		],
		"numpartitions" : 8192,
		"loggingfilepath" : "/tmp/logfile1/geetcache.log",
		"logrolloversize" : 20971520,
		"loglevel" : "info",
		"maxbackup" : 10
	}`
)

var conf *config = nil
var init_once sync.Once

type config struct {
	conf_data map[string]interface{}
}

func initConfigS(config_data []byte) (*config, error) {
	var err error = nil
	init_once.Do(func() {
		var decoded map[string]interface{}
		err = json.Unmarshal(config_data, &decoded)
		if err == nil {
			conf = &config{conf_data: decoded}
		}
	})
	if err != nil {
		fmt.Printf("Error in config %v\n", err)
	}
	return conf, err
}

func initConfig(configfile string) (*config, error) {
	data, err := ioutil.ReadFile(configfile)
	if err != nil {
		return nil, err
	}
	return initConfigS(data)
}

func (conf *config) getIntVal(key string) (int, error) {
	val, ok := conf.conf_data[key]
	if !ok {
		return 0, errors.New(fmt.Sprintf("Could not get value for key:%s", key))
	}
	return int(val.(float64)), nil
}

func (conf *config) getStringVal(key string) (string, error) {
	val, ok := conf.conf_data[key]
	if !ok {
		return "", errors.New(fmt.Sprintf("Could not get value for key:%s", key))
	}
	return val.(string), nil
}

func (conf *config) getMyId() (int, error) {
	return conf.getIntVal("myid")
}

func (conf *config) getStringArray(key string) ([]string, error) {
	val, ok := conf.conf_data[key]
	if !ok {
		return nil, errors.New(fmt.Sprintf("Could not get value for key:%s", key))
	}
	return val.([]string), nil
}

func (conf *config) getMyAddr() (string, error) {
	return conf.getStringVal("myself")
}

func (conf *config) getLogDir() (string, error) {
	return conf.getStringVal("logs")
}

func (conf *config) getPeerListDir() (string, error) {
	return conf.getStringVal("peerdir")
}

func (conf *config) getStableStoreDir() (string, error) {
	return conf.getStringVal("sstore")
}

func (conf *config) getSnapshotDir() (string, error) {
	return conf.getStringVal("snapshot")
}

func (conf *config) getRpcAddr() (string, error) {
	return conf.getStringVal("rpcaddr")
}

func (conf *config) getPeers() ([]string, error) {
	return conf.getStringArray("raftpeers")
}

func (conf *config) getLogFile() (string, error) {
	return conf.getStringVal("loggingfilepath")
}

func (conf *config) getLogRollSize() (int, error) {
	rollsize, ok := conf.conf_data["logrolloversize"]
	if !ok {
		return 0, errors.New("logrolloversize is not provided")
	}
	return int(rollsize.(float64)), nil
}

func (conf *config) getLogLevel() (string, error) {
	return conf.getStringVal("loglevel")
}

func (conf *config) getLogBackup() (int, error) {
	max_backup, err := conf.getIntVal("maxbackup")
	if err != nil {
		return 0, err
	}
	if max_backup <= 0 {
		max_backup = 2
	}
	if max_backup > MAX_LOG_BACKUP {
		max_backup = MAX_LOG_BACKUP
	}
	return max_backup, nil
}

func (conf *config) getNumPartition() (int, error) {
	numpartitions, err := conf.getIntVal("numpartitions")
	if err != nil {
		return 0, err
	}
	if numpartitions < MIN_PARTITIONS {
		numpartitions = MIN_PARTITIONS
	}
	if numpartitions > MAX_PARTITIONS {
		numpartitions = MAX_PARTITIONS
	}
	return numpartitions, nil
}

func getConfig() *config {
	return conf
}
