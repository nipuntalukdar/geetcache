package main

import (
	"flag"
)

func init() {
	configFile := flag.String("config", "", "Config file name")
	flag.Parse()
	if *configFile == "" {
		initConfigS([]byte(default_config))
	} else {
		initConfig(*configFile)
	}

	init_logger()

	if *configFile == "" {
		LOG.Info("Initializing default config")
	} else {
		LOG.Infof("Intializing configs from file: %s", *configFile)
	}
}
